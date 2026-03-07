"""TfL station accessibility ingestor — lift/escalator disruptions and closures.

Polls the TfL StopPoint disruption endpoint for tube stations to detect
lift failures, escalator outages, and step-free access issues. This enables
cross-referencing social media complaints about accessibility with the
official operational status of infrastructure at specific stations.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.tfl_accessibility")

TFL_DISRUPTION_URL = "https://api.tfl.gov.uk/StopPoint/Mode/tube/Disruption"

# Keywords that indicate accessibility-related disruptions
_ACCESSIBILITY_KEYWORDS = {
    "lift", "elevator", "escalator", "step-free", "step free",
    "wheelchair", "accessible", "mobility", "ramp", "stairs",
}


def _is_accessibility_related(description: str) -> bool:
    """Check if a disruption description relates to accessibility."""
    desc_lower = description.lower()
    return any(kw in desc_lower for kw in _ACCESSIBILITY_KEYWORDS)


class TflAccessibilityIngestor(BaseIngestor):
    source_name = "tfl_accessibility"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_DISRUPTION_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected TfL disruption response type: %s", type(data))
            return

        processed = 0
        skipped = 0

        for disruption in data:
            try:
                description = disruption.get("description", "")
                stop_id = disruption.get("stationAtcoCode", "") or disruption.get("atcoCode", "")
                disruption_type = disruption.get("type", "")
                common_name = disruption.get("commonName", stop_id)

                if not description:
                    skipped += 1
                    continue

                is_accessibility = _is_accessibility_related(description)

                # Extract appearance/closure dates
                from_date = disruption.get("fromDate", "")
                to_date = disruption.get("toDate", "")

                # Try to get coordinates from the disruption or associated stop
                lat = None
                lon = None
                appearance = disruption.get("appearance", "")
                closure_text = disruption.get("closureText", "")

                cell_id = None
                if lat and lon:
                    cell_id = self.graph.latlon_to_cell(float(lat), float(lon))

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.EVENT,
                    value=description,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "stop_id": stop_id,
                        "station_name": common_name,
                        "disruption_type": disruption_type,
                        "is_accessibility": is_accessibility,
                        "from_date": from_date,
                        "to_date": to_date,
                        "appearance": appearance,
                        "closure_text": closure_text,
                    },
                )
                await self.board.store_observation(obs)

                tag = "[ACCESSIBILITY]" if is_accessibility else "[DISRUPTION]"
                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"TfL Station {tag} {common_name}: {description[:200]}"
                    ),
                    data={
                        "stop_id": stop_id,
                        "station_name": common_name,
                        "disruption_type": disruption_type,
                        "is_accessibility": is_accessibility,
                        "description": description,
                        "from_date": from_date,
                        "to_date": to_date,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception(
                    "Error processing disruption entry: %s",
                    disruption.get("stationAtcoCode", "unknown"),
                )
                skipped += 1

        accessibility_count = sum(
            1 for d in data
            if isinstance(d, dict) and _is_accessibility_related(d.get("description", ""))
        )
        self.log.info(
            "TfL accessibility: processed=%d (accessibility=%d) skipped=%d",
            processed, accessibility_count, skipped,
        )


async def ingest_tfl_accessibility(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL accessibility fetch cycle."""
    ingestor = TflAccessibilityIngestor(board, graph, scheduler)
    await ingestor.run()
