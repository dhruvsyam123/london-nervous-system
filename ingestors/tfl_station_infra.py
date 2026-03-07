"""TfL Station Infrastructure Disruptions — lifts, escalators, closures.

Fetches per-station disruptions from TfL's StopPoint Disruption API, covering
lift outages, escalator replacements, station closures, and access restrictions.
By accumulating these observations over time, downstream agents can detect
recurring failure patterns at specific stations — a proxy for asset condition
and maintenance needs, since TfL's internal asset management database is not
publicly accessible.

Complements tfl_disruption_incidents.py (line-level disruptions) with
station-level infrastructure detail.

Endpoint: /StopPoint/Mode/{modes}/Disruption  (no API key required)
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

log = logging.getLogger("london.ingestors.tfl_station_infra")

TFL_STOPPOINT_DISRUPTION_URL = (
    "https://api.tfl.gov.uk/StopPoint/Mode/"
    "tube,dlr,overground,elizabeth-line,tram/Disruption"
)

# Classify disruption description into asset categories
_ASSET_PATTERNS = [
    (re.compile(r"lift", re.I), "lift"),
    (re.compile(r"escalator", re.I), "escalator"),
    (re.compile(r"toilet", re.I), "toilet"),
    (re.compile(r"subway|underpass|passage", re.I), "subway_passage"),
    (re.compile(r"(?:step[- ]?free|wheelchair|accessible)", re.I), "step_free_access"),
    (re.compile(r"platform", re.I), "platform"),
    (re.compile(r"entrance|exit", re.I), "entrance_exit"),
    (re.compile(r"(?:will not (?:call|stop)|closed|closure)", re.I), "station_closure"),
]

# Severity based on disruption type/appearance
_APPEARANCE_SEVERITY = {
    "RealTime": 4,
    "PlannedWork": 2,
    "Information": 1,
}


def _classify_asset(description: str) -> str:
    """Classify which infrastructure asset is affected."""
    for pattern, asset_type in _ASSET_PATTERNS:
        if pattern.search(description):
            return asset_type
    return "other"


class TflStationInfraIngestor(BaseIngestor):
    source_name = "tfl_station_infra"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_STOPPOINT_DISRUPTION_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected data format: %s", type(data))
            return

        processed = 0

        for item in data:
            common_name = item.get("commonName", "unknown")
            description = item.get("description", "")
            appearance = item.get("appearance", "")
            disruption_type = item.get("type", "")
            mode = item.get("mode", "")
            from_date = item.get("fromDate", "")
            to_date = item.get("toDate", "")
            atco_code = item.get("stationAtcoCode") or item.get("atcoCode", "")

            asset_type = _classify_asset(description)
            severity = _APPEARANCE_SEVERITY.get(appearance, 2)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=severity,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "station": common_name,
                    "atco_code": atco_code,
                    "asset_type": asset_type,
                    "disruption_type": disruption_type,
                    "appearance": appearance,
                    "mode": mode,
                    "from_date": from_date,
                    "to_date": to_date,
                    "description": description[:500],
                },
            )
            await self.board.store_observation(obs)

            tag = f"[{appearance.upper()}]" if appearance else "[UNKNOWN]"
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Station infra {tag} {common_name}: "
                    f"asset={asset_type} type={disruption_type} "
                    f"severity={severity}"
                    + (f" — {description[:200]}" if description else "")
                ),
                data={
                    "station": common_name,
                    "atco_code": atco_code,
                    "asset_type": asset_type,
                    "disruption_type": disruption_type,
                    "appearance": appearance,
                    "mode": mode,
                    "severity": severity,
                    "observation_id": obs.id,
                },
                location_id=None,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "TfL station infrastructure: processed=%d disruptions", processed
        )


async def ingest_tfl_station_infra(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL station infra fetch cycle."""
    ingestor = TflStationInfraIngestor(board, graph, scheduler)
    await ingestor.run()
