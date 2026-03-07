"""FixMyStreet ingestor — street-level infrastructure reports for London.

Fetches recent citizen-reported issues (potholes, broken lights, fly-tipping,
road obstructions, flooding, etc.) from FixMyStreet's public /around endpoint
using bounding-box queries. Provides ground-truth data on localised
disruptions for hypothesis verification by the Validator agent.

No API key required — uses the public AJAX pin endpoint.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.fixmystreet")

# FixMyStreet returns pins via the /around AJAX endpoint with bbox queries.
# We tile Greater London into quadrants to stay under the 1000-result limit.
FMS_BASE = "https://www.fixmystreet.com/around"

# Greater London bbox tiles (SW-lon, SW-lat, NE-lon, NE-lat)
LONDON_TILES = [
    (-0.51, 51.28, -0.10, 51.50),   # SW London
    (-0.10, 51.28,  0.33, 51.50),   # SE London
    (-0.51, 51.50, -0.10, 51.70),   # NW London
    (-0.10, 51.50,  0.33, 51.70),   # NE London
]

# Pin colour → status mapping (from FixMyStreet source)
PIN_STATUS = {
    "yellow": "open",
    "green":  "fixed",
    "grey":   "closed",
    "red":    "open",
}

# Categories relevant to transport/infrastructure disruption
_TRANSPORT_CATEGORIES = {
    "pothole", "road", "pavement", "light", "traffic", "sign",
    "drain", "flooding", "bridge", "kerb", "manhole", "barrier",
    "obstruct", "danger", "damage", "broken", "leak", "burst",
    "gas", "water", "electric", "cable", "sewer", "collapse",
    "cycling", "cycle",
}

# High-severity keywords for infrastructure issues
_HIGH_SEVERITY_KEYWORDS = {
    "flooding", "collapse", "danger", "obstruct", "burst", "pothole",
    "traffic light", "traffic signal", "road surface", "sinkhole",
}


def _is_transport_relevant(title: str) -> bool:
    title_lower = title.lower()
    return any(kw in title_lower for kw in _TRANSPORT_CATEGORIES)


def _is_high_severity(title: str) -> bool:
    title_lower = title.lower()
    return any(kw in title_lower for kw in _HIGH_SEVERITY_KEYWORDS)


class FixMyStreetIngestor(BaseIngestor):
    source_name = "fixmystreet"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Fetch pins from all London tiles."""
        all_pins: list[list] = []
        for sw_lon, sw_lat, ne_lon, ne_lat in LONDON_TILES:
            bbox = f"{sw_lon},{sw_lat},{ne_lon},{ne_lat}"
            data = await self.fetch(
                FMS_BASE,
                params={"ajax": "1", "bbox": bbox},
            )
            if data and isinstance(data, dict):
                pins = data.get("pins", [])
                all_pins.extend(pins)
        return all_pins if all_pins else None

    async def process(self, data: Any) -> None:
        pins: list[list] = data
        processed = 0
        seen_ids: set[int] = set()

        for pin in pins:
            # pin format: [lat, lon, colour, report_id, title, category, is_fixed, ...]
            if not isinstance(pin, list) or len(pin) < 5:
                continue

            try:
                lat = float(pin[0])
                lon = float(pin[1])
                colour = str(pin[2])
                report_id = int(pin[3])
                title = str(pin[4])
            except (ValueError, TypeError, IndexError):
                continue

            if report_id in seen_ids:
                continue
            seen_ids.add(report_id)

            status = PIN_STATUS.get(colour, "unknown")
            cell_id = self.graph.latlon_to_cell(lat, lon)
            category = str(pin[5]) if len(pin) > 5 else ""
            transport_relevant = _is_transport_relevant(title)
            high_severity = _is_high_severity(title)
            severity_value = 3.0 if high_severity else (2.0 if transport_relevant else 1.0)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=severity_value if status == "open" else 0.0,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "report_id": report_id,
                    "title": title[:200],
                    "status": status,
                    "category": category,
                    "is_transport_relevant": transport_relevant,
                    "is_high_severity": high_severity,
                },
            )
            await self.board.store_observation(obs)

            relevance_tag = " [transport]" if transport_relevant else ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"FixMyStreet [{status}]{relevance_tag}: {title[:120]}"
                ),
                data={
                    "report_id": report_id,
                    "title": title[:200],
                    "status": status,
                    "category": category,
                    "severity": severity_value,
                    "is_transport_relevant": transport_relevant,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "FixMyStreet: processed=%d unique reports (from %d pins)",
            processed, len(pins),
        )


async def ingest_fixmystreet(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one FixMyStreet fetch cycle."""
    ingestor = FixMyStreetIngestor(board, graph, scheduler)
    await ingestor.run()
