"""UK Police stop-and-search ingestor — data.police.uk (free, no API key).

Fetches recent stop-and-search data for sampled London locations, providing
geo-located police enforcement activity with incident types (drugs, weapons,
public order, etc.). Complements the police_crimes ingestor by adding
ground-truth operational policing data — the closest public proxy to real-time
CAD (Computer Aided Dispatch) incident types.

Data is monthly (not real-time), but provides spatial awareness of enforcement
hotspots and incident categories the system can use to validate or challenge
anomalies from other sensors.
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

log = logging.getLogger("london.ingestors.police_stops")

STOPS_STREET_URL = "https://data.police.uk/api/stops-street"
STOP_DATES_URL = "https://data.police.uk/api/crimes-street-dates"

# Object-of-search categories most relevant to urban disruption detection
DISRUPTION_RELEVANT = {
    "Offensive weapons",
    "Firearms",
    "Fireworks",
    "Anything to threaten or harm anyone",
    "Stolen goods",
    "Evidence of offences under the Act",
}

# Sample points across Greater London — shared spread with police_crimes
# but we sample independently to avoid correlated gaps
SAMPLE_POINTS = [
    (51.5074, -0.1278),   # Central London / Westminster
    (51.5155, -0.0922),   # City of London
    (51.5313, -0.1040),   # Kings Cross / Euston
    (51.4613, -0.1156),   # Brixton
    (51.5430, -0.0553),   # Hackney / Dalston
    (51.5225, -0.1540),   # Paddington
    (51.4975, -0.1357),   # Victoria
    (51.5556, -0.1084),   # Finsbury Park
    (51.4720, -0.4887),   # Heathrow area
    (51.5136, 0.0890),    # Canning Town / Newham
    (51.4875, -0.0076),   # Greenwich
    (51.5853, -0.0754),   # Tottenham
]

MAX_POINTS_PER_CYCLE = 3


class PoliceStopsIngestor(BaseIngestor):
    source_name = "police_stops"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # Get the latest available date
        dates_data = await self.fetch(STOP_DATES_URL)
        if not dates_data or not isinstance(dates_data, list):
            self.log.warning("Could not fetch available stop-and-search dates")
            return None

        latest_date = dates_data[0].get("date") if dates_data else None
        if not latest_date:
            self.log.warning("No date found in stop dates response")
            return None

        points = random.sample(
            SAMPLE_POINTS, min(MAX_POINTS_PER_CYCLE, len(SAMPLE_POINTS))
        )

        all_stops: list[dict] = []
        for lat, lng in points:
            data = await self.fetch(
                STOPS_STREET_URL,
                params={"lat": str(lat), "lng": str(lng), "date": latest_date},
            )
            if isinstance(data, list):
                all_stops.extend(data)

        return {"date": latest_date, "stops": all_stops}

    async def process(self, data: Any) -> None:
        stops = data.get("stops", [])
        date = data.get("date", "unknown")

        if not stops:
            self.log.info("No stop-and-search data returned for %s", date)
            return

        # Aggregate by (object_of_search, snap_location) to avoid flooding
        location_counts: dict[tuple[str, str, float, float], int] = {}
        outcome_counts: dict[str, int] = {}

        for stop in stops:
            obj_of_search = stop.get("object_of_search") or "Unknown"
            stop_type = stop.get("type", "Person search")
            outcome = stop.get("outcome") or "No further action"
            location = stop.get("location") or {}
            lat_str = location.get("latitude")
            lon_str = location.get("longitude")
            street_name = (location.get("street") or {}).get("name", "unknown")

            try:
                lat = float(lat_str) if lat_str else None
                lon = float(lon_str) if lon_str else None
            except (ValueError, TypeError):
                continue

            if lat is None or lon is None:
                continue

            key = (obj_of_search, street_name, lat, lon)
            location_counts[key] = location_counts.get(key, 0) + 1
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1

        processed = 0
        for (obj_search, street, lat, lon), count in location_counts.items():
            cell_id = self.graph.latlon_to_cell(lat, lon)
            is_disruption = obj_search in DISRUPTION_RELEVANT

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(count),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "object_of_search": obj_search,
                    "street": street,
                    "date": date,
                    "count": count,
                    "disruption_relevant": is_disruption,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Police stop [{date}] {street}: {obj_search}={count}"
                    + (" [DISRUPTION-RELEVANT]" if is_disruption else "")
                ),
                data={
                    "object_of_search": obj_search,
                    "street": street,
                    "lat": lat,
                    "lon": lon,
                    "count": count,
                    "date": date,
                    "disruption_relevant": is_disruption,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        disruption_total = sum(
            count
            for (obj, _, _, _), count in location_counts.items()
            if obj in DISRUPTION_RELEVANT
        )
        total = sum(location_counts.values())

        self.log.info(
            "Police stops [%s]: %d locations, %d total stops (%d disruption-relevant)",
            date,
            processed,
            total,
            disruption_total,
        )


async def ingest_police_stops(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one police stop-and-search cycle."""
    ingestor = PoliceStopsIngestor(board, graph, scheduler)
    await ingestor.run()
