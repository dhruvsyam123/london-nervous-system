"""Metropolitan Police neighbourhood events & stop-and-search ingestor.

Complements police_crimes.py (monthly street-level crime stats) with:
  - Neighbourhood events: planned community engagement activities
  - Stop and search: recent stop-and-search data indicating active policing hotspots

Both from data.police.uk — free, no API key required.

Note: The Met Police does not publish a real-time incident feed for protests,
accidents, or security alerts. This ingestor captures what IS publicly available.
"""

from __future__ import annotations

import logging
import random
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.police_events")

# data.police.uk endpoints
NEIGHBOURHOODS_URL = "https://data.police.uk/api/metropolitan/neighbourhoods"
EVENTS_URL = "https://data.police.uk/api/metropolitan/{neighbourhood_id}/events"
STOPS_URL = "https://data.police.uk/api/stops-street"

# Sample points for stop-and-search queries (spread across London)
SAMPLE_POINTS = [
    (51.5074, -0.1278),  # Westminster
    (51.5155, -0.0922),  # City of London
    (51.5033, -0.1195),  # Whitehall
    (51.4613, -0.1156),  # Brixton
    (51.5430, -0.0553),  # Hackney
    (51.5313, -0.1040),  # Kings Cross
    (51.4975, -0.1357),  # Victoria
    (51.5136, 0.0890),   # Canning Town
    (51.4720, -0.4887),  # Heathrow area
    (51.5556, -0.1084),  # Finsbury Park
]

# Sample a subset per cycle to stay within rate limits
MAX_STOP_POINTS_PER_CYCLE = 3
MAX_NEIGHBOURHOODS_PER_CYCLE = 5


class PoliceEventsIngestor(BaseIngestor):
    source_name = "police_events"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {"events": [], "stops": []}

        # 1. Fetch neighbourhood list, sample a few, get their events
        neighbourhoods = await self.fetch(NEIGHBOURHOODS_URL)
        if isinstance(neighbourhoods, list) and neighbourhoods:
            sampled = random.sample(
                neighbourhoods,
                min(MAX_NEIGHBOURHOODS_PER_CYCLE, len(neighbourhoods)),
            )
            for nb in sampled:
                nb_id = nb.get("id", "")
                nb_name = nb.get("name", "unknown")
                url = EVENTS_URL.format(neighbourhood_id=nb_id)
                events = await self.fetch(url)
                if isinstance(events, list):
                    for ev in events:
                        ev["_neighbourhood_id"] = nb_id
                        ev["_neighbourhood_name"] = nb_name
                    results["events"].extend(events)

        # 2. Fetch stop-and-search for sampled points
        points = random.sample(
            SAMPLE_POINTS, min(MAX_STOP_POINTS_PER_CYCLE, len(SAMPLE_POINTS))
        )
        for lat, lng in points:
            stops = await self.fetch(
                STOPS_URL, params={"lat": str(lat), "lng": str(lng)}
            )
            if isinstance(stops, list):
                results["stops"].extend(stops)

        if not results["events"] and not results["stops"]:
            return None

        return results

    async def process(self, data: Any) -> None:
        events = data.get("events", [])
        stops = data.get("stops", [])
        event_count = 0
        stop_count = 0

        # ── Process neighbourhood events ──
        for ev in events:
            title = ev.get("title", "Untitled event")
            description = ev.get("description", "")
            address = ev.get("address", "")
            ev_type = ev.get("type", "")
            start_date = ev.get("start_date", "")
            nb_name = ev.get("_neighbourhood_name", "unknown")

            # Events don't have lat/lon, post without spatial reference
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=1.0,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "event_type": "neighbourhood_event",
                    "title": title,
                    "description": description[:500],
                    "address": address,
                    "type": ev_type,
                    "start_date": start_date,
                    "neighbourhood": nb_name,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Police event [{nb_name}] {title}"
                    + (f" ({ev_type})" if ev_type else "")
                    + (f" @ {address}" if address else "")
                ),
                data={
                    "title": title,
                    "type": ev_type,
                    "neighbourhood": nb_name,
                    "start_date": start_date,
                    "observation_id": obs.id,
                },
            )
            await self.board.post(msg)
            event_count += 1

        # ── Process stop-and-search ──
        # Aggregate by (object_of_search, location) to avoid flooding
        stop_agg: dict[tuple[str, float, float], dict] = {}

        for stop in stops:
            obj_search = stop.get("object_of_search", "unknown")
            outcome = stop.get("outcome", "")
            location = stop.get("location", {})
            lat_str = location.get("latitude") if location else None
            lon_str = location.get("longitude") if location else None
            street = (
                location.get("street", {}).get("name", "unknown")
                if location
                else "unknown"
            )

            try:
                lat = float(lat_str) if lat_str else None
                lon = float(lon_str) if lon_str else None
            except (ValueError, TypeError):
                continue

            if lat is None or lon is None:
                continue

            key = (obj_search, lat, lon)
            if key not in stop_agg:
                stop_agg[key] = {
                    "count": 0,
                    "street": street,
                    "outcomes": {},
                    "lat": lat,
                    "lon": lon,
                }
            stop_agg[key]["count"] += 1
            stop_agg[key]["outcomes"][outcome] = (
                stop_agg[key]["outcomes"].get(outcome, 0) + 1
            )

        for (obj_search, lat, lon), agg in stop_agg.items():
            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(agg["count"]),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "event_type": "stop_and_search",
                    "object_of_search": obj_search,
                    "street": agg["street"],
                    "count": agg["count"],
                    "outcomes": agg["outcomes"],
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Police stop-search [{agg['street']}] "
                    f"{obj_search}={agg['count']}"
                ),
                data={
                    "object_of_search": obj_search,
                    "street": agg["street"],
                    "lat": lat,
                    "lon": lon,
                    "count": agg["count"],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            stop_count += 1

        self.log.info(
            "Police events: %d neighbourhood events, %d stop-search clusters",
            event_count,
            stop_count,
        )


async def ingest_police_events(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one police events fetch cycle."""
    ingestor = PoliceEventsIngestor(board, graph, scheduler)
    await ingestor.run()
