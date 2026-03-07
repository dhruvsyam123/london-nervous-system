"""OpenSky Network ADS-B ingestor — real-time aircraft positions over London.

Uses the free anonymous REST API to track aircraft within the Greater London
bounding box.  Particularly useful for detecting helicopter activity (police,
air ambulance, news) which can corroborate security/incident hypotheses.

API docs: https://openskynetwork.github.io/opensky-api/rest.html
Anonymous limits: 400 credits/day, 10 s state-vector resolution.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LONDON_BBOX
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.opensky")

OPENSKY_URL = "https://opensky-network.org/api/states/all"

# Aircraft category codes from OpenSky (extended=1)
_CATEGORY_LABELS = {
    0: "No info",
    1: "No ADS-B category",
    2: "Light (<15500 lbs)",
    3: "Small (15500-75000 lbs)",
    4: "Large (75000-300000 lbs)",
    5: "High vortex large",
    6: "Heavy (>300000 lbs)",
    7: "High performance",
    8: "Rotorcraft",
    9: "Glider/sailplane",
    10: "Lighter-than-air",
    11: "Parachutist/skydiver",
    12: "Ultralight/hang-glider",
    14: "UAV",
    15: "Space vehicle",
    16: "Surface emergency vehicle",
    17: "Surface service vehicle",
    18: "Point obstacle",
    19: "Cluster obstacle",
    20: "Line obstacle",
}

# Interesting categories for security/incident correlation
_HELICOPTER_CATEGORIES = {8}  # Rotorcraft


class OpenSkyIngestor(BaseIngestor):
    source_name = "opensky_adsb"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        params = {
            "lamin": LONDON_BBOX["min_lat"],
            "lomin": LONDON_BBOX["min_lon"],
            "lamax": LONDON_BBOX["max_lat"],
            "lomax": LONDON_BBOX["max_lon"],
            "extended": 1,
        }
        return await self.fetch(OPENSKY_URL, params=params)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected OpenSky response type: %s", type(data))
            return

        states = data.get("states")
        if not states:
            self.log.info("OpenSky: no aircraft in London bbox")
            return

        processed = 0
        helicopters = 0

        for sv in states:
            # State vector indices per OpenSky docs:
            #  0=icao24, 1=callsign, 2=origin_country, 5=longitude, 6=latitude,
            #  7=baro_altitude, 9=velocity, 10=true_track, 11=vertical_rate,
            #  13=geo_altitude, 16=category (when extended=1)
            if len(sv) < 17:
                continue

            icao24 = sv[0] or ""
            callsign = (sv[1] or "").strip()
            origin = sv[2] or ""
            lon = sv[5]
            lat = sv[6]
            baro_alt = sv[7]  # metres
            velocity = sv[9]  # m/s
            vertical_rate = sv[11]  # m/s
            on_ground = sv[8]
            geo_alt = sv[13]
            category = sv[16] if sv[16] is not None else 0

            if lat is None or lon is None:
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)
            is_helicopter = category in _HELICOPTER_CATEGORIES
            cat_label = _CATEGORY_LABELS.get(category, f"Unknown ({category})")

            if is_helicopter:
                helicopters += 1

            alt_ft = round(baro_alt * 3.281) if baro_alt is not None else None
            speed_kts = round(velocity * 1.944) if velocity is not None else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=baro_alt if baro_alt is not None else 0,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "icao24": icao24,
                    "callsign": callsign,
                    "origin_country": origin,
                    "category": category,
                    "category_label": cat_label,
                    "is_helicopter": is_helicopter,
                    "altitude_m": baro_alt,
                    "altitude_ft": alt_ft,
                    "velocity_ms": velocity,
                    "speed_kts": speed_kts,
                    "vertical_rate_ms": vertical_rate,
                    "on_ground": on_ground,
                    "geo_altitude_m": geo_alt,
                },
            )
            await self.board.store_observation(obs)

            # Build a concise message
            alt_str = f"{alt_ft}ft" if alt_ft is not None else "alt?"
            spd_str = f"{speed_kts}kts" if speed_kts is not None else ""
            heli_tag = " [HELICOPTER]" if is_helicopter else ""
            content = (
                f"Aircraft {callsign or icao24} ({cat_label}) "
                f"over ({lat:.3f},{lon:.3f}) {alt_str} {spd_str}{heli_tag}"
            )

            msg = AgentMessage(
                from_agent=f"ingestor:{self.source_name}",
                channel="#raw",
                content=content,
                data={
                    "icao24": icao24,
                    "callsign": callsign,
                    "category": category,
                    "category_label": cat_label,
                    "is_helicopter": is_helicopter,
                    "lat": lat,
                    "lon": lon,
                    "altitude_ft": alt_ft,
                    "speed_kts": speed_kts,
                    "on_ground": on_ground,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "OpenSky ADS-B: %d aircraft, %d helicopters over London",
            processed, helicopters,
        )


async def ingest_opensky(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one OpenSky fetch cycle."""
    ingestor = OpenSkyIngestor(board, graph, scheduler)
    await ingestor.run()
