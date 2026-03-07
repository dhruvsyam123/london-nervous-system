"""TfL Station Asset Disruptions ingestor — lift, escalator, and facility outages.

Fetches station-level disruptions from TfL's StopPoint Disruption API, capturing
lift faults, escalator replacements, subway closures, and other facility issues.
This provides a proxy for asset condition and maintenance burden — a high count of
concurrent lift/escalator outages at a station cluster may indicate systemic
infrastructure aging or maintenance strain, rather than isolated random failures.

Complements tfl_disruption_incidents.py (which covers line-level service disruptions)
by providing station-infrastructure-level detail.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.tfl_station_assets")

# StopPoint disruption endpoint — no API key required
TFL_STOPPOINT_DISRUPTION_URL = (
    "https://api.tfl.gov.uk/StopPoint/Mode/"
    "tube,dlr,overground,elizabeth-line/Disruption"
)

# Classify disruption descriptions into asset categories
_ASSET_PATTERNS = [
    (re.compile(r"\blift\b", re.I), "lift"),
    (re.compile(r"\bescalator\b", re.I), "escalator"),
    (re.compile(r"\bstep[- ]?free\b", re.I), "step_free_access"),
    (re.compile(r"\bsubway\b", re.I), "subway_passage"),
    (re.compile(r"\btoilet\b|\bwc\b", re.I), "toilet"),
    (re.compile(r"\bentrance\b|\bexit\b|\bgate\b", re.I), "entrance_exit"),
    (re.compile(r"\bplatform\b", re.I), "platform"),
    (re.compile(r"\btunnel\b|\bpassage\b|\bwalkway\b", re.I), "walkway"),
]

# Map appearance field to severity
_APPEARANCE_SEVERITY = {
    "RealTime": 3,
    "PlannedWork": 2,
    "Information": 1,
}


def _classify_asset(description: str) -> str:
    """Classify disruption into an asset category based on description text."""
    for pattern, category in _ASSET_PATTERNS:
        if pattern.search(description):
            return category
    return "other_facility"


class TflStationAssetIngestor(BaseIngestor):
    source_name = "tfl_station_assets"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_STOPPOINT_DISRUPTION_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected data format: %s", type(data))
            return

        processed = 0
        asset_counts: dict[str, int] = {}

        for item in data:
            station = item.get("commonName", "unknown")
            description = item.get("description", "")
            appearance = item.get("appearance", "")
            mode = item.get("mode", "")
            from_date = item.get("fromDate", "")
            to_date = item.get("toDate", "")
            disruption_type = item.get("type", "")

            asset_category = _classify_asset(description)
            asset_counts[asset_category] = asset_counts.get(asset_category, 0) + 1

            severity = _APPEARANCE_SEVERITY.get(appearance, 2)

            # Try to get station lat/lon from stationAtcoCode or atcoCode
            # The endpoint doesn't include coords, so we leave location as None
            # and let downstream agents match by station name
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=severity,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "station": station,
                    "asset_category": asset_category,
                    "description": description[:500],
                    "appearance": appearance,
                    "disruption_type": disruption_type,
                    "mode": mode,
                    "from_date": from_date,
                    "to_date": to_date,
                    "atco_code": item.get("stationAtcoCode", ""),
                },
            )
            await self.board.store_observation(obs)

            is_realtime = appearance == "RealTime"
            tag = " [FAULT]" if is_realtime else " [PLANNED]"
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Station asset{tag} [{station}] {asset_category} "
                    f"severity={severity}"
                    + (f" — {description[:200]}" if description else "")
                ),
                data={
                    "station": station,
                    "asset_category": asset_category,
                    "appearance": appearance,
                    "severity_score": severity,
                    "mode": mode,
                    "from_date": from_date,
                    "to_date": to_date,
                    "observation_id": obs.id,
                },
                location_id=None,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "TfL station assets: processed=%d breakdown=%s",
            processed, dict(asset_counts),
        )


async def ingest_tfl_station_assets(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL station asset disruption fetch cycle."""
    ingestor = TflStationAssetIngestor(board, graph, scheduler)
    await ingestor.run()
