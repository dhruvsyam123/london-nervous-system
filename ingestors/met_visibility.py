"""Met Office visibility ingestor — atmospheric visibility & fog detection.

Uses Open-Meteo's free API for visibility, cloud cover, and humidity data
across multiple London locations. Provides quantitative fog/mist detection
to correlate with transport disruptions.

Visibility categories (Met Office standard):
  - Dense fog:    <40m
  - Thick fog:    40–200m
  - Fog:          200–1000m
  - Mist:         1000–2000m
  - Haze:         2000–4000m
  - Poor:         4000–10000m
  - Moderate:     10000–20000m
  - Good:         20000–40000m
  - Excellent:    >40000m
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.met_visibility")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Sample points across Greater London for spatial coverage
LONDON_SITES = [
    {"name": "Central London", "lat": 51.507, "lon": -0.128},
    {"name": "Heathrow", "lat": 51.470, "lon": -0.454},
    {"name": "City Airport", "lat": 51.505, "lon": 0.055},
    {"name": "North London", "lat": 51.588, "lon": -0.103},
    {"name": "South London", "lat": 51.421, "lon": -0.072},
]


def _classify_visibility(vis_m: float) -> str:
    """Classify visibility in metres into Met Office categories."""
    if vis_m < 40:
        return "dense_fog"
    elif vis_m < 200:
        return "thick_fog"
    elif vis_m < 1000:
        return "fog"
    elif vis_m < 2000:
        return "mist"
    elif vis_m < 4000:
        return "haze"
    elif vis_m < 10000:
        return "poor"
    elif vis_m < 20000:
        return "moderate"
    elif vis_m < 40000:
        return "good"
    else:
        return "excellent"


def _is_fog_risk(vis_m: float, humidity: float | None, cloud_low: float | None) -> bool:
    """Heuristic: flag fog risk when visibility is low or conditions favour fog."""
    if vis_m < 2000:
        return True
    if humidity is not None and humidity > 95 and vis_m < 5000:
        return True
    if cloud_low is not None and cloud_low > 80 and humidity is not None and humidity > 90:
        return True
    return False


class MetVisibilityIngestor(BaseIngestor):
    source_name = "met_visibility"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results = []
        for site in LONDON_SITES:
            params = {
                "latitude": site["lat"],
                "longitude": site["lon"],
                "current": "visibility,relative_humidity_2m,cloud_cover_low,cloud_cover",
                "timezone": "Europe/London",
            }
            data = await self.fetch(OPEN_METEO_URL, params=params)
            if data and isinstance(data, dict):
                results.append({"site": site, "data": data})
        return results if results else None

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected data type: %s", type(data))
            return

        processed = 0
        fog_sites = []

        for entry in data:
            site = entry["site"]
            current = entry["data"].get("current", {})
            if not current:
                continue

            vis_raw = current.get("visibility")
            humidity = current.get("relative_humidity_2m")
            cloud_low = current.get("cloud_cover_low")
            cloud_total = current.get("cloud_cover")

            if vis_raw is None:
                continue

            try:
                vis_m = float(vis_raw)
            except (ValueError, TypeError):
                continue

            humidity_f = float(humidity) if humidity is not None else None
            cloud_low_f = float(cloud_low) if cloud_low is not None else None
            cloud_total_f = float(cloud_total) if cloud_total is not None else None

            category = _classify_visibility(vis_m)
            fog_risk = _is_fog_risk(vis_m, humidity_f, cloud_low_f)

            cell_id = self.graph.latlon_to_cell(site["lat"], site["lon"])

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=vis_m,
                location_id=cell_id,
                lat=site["lat"],
                lon=site["lon"],
                metadata={
                    "site": site["name"],
                    "visibility_m": vis_m,
                    "visibility_category": category,
                    "humidity_pct": humidity_f,
                    "cloud_cover_low_pct": cloud_low_f,
                    "cloud_cover_total_pct": cloud_total_f,
                    "fog_risk": fog_risk,
                    "unit": "m",
                },
            )
            await self.board.store_observation(obs)
            processed += 1

            if fog_risk:
                fog_sites.append(site["name"])

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Visibility [{site['name']}] {vis_m:.0f}m ({category})"
                    + (f", humidity={humidity_f:.0f}%" if humidity_f is not None else "")
                    + (" — FOG RISK" if fog_risk else "")
                ),
                data={
                    "site": site["name"],
                    "visibility_m": vis_m,
                    "category": category,
                    "humidity_pct": humidity_f,
                    "cloud_cover_low_pct": cloud_low_f,
                    "fog_risk": fog_risk,
                    "lat": site["lat"],
                    "lon": site["lon"],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)

        if fog_sites:
            self.log.warning(
                "FOG RISK detected at %d site(s): %s",
                len(fog_sites), ", ".join(fog_sites),
            )

        self.log.info(
            "Met visibility: processed=%d sites, fog_risk_sites=%d",
            processed, len(fog_sites),
        )


async def ingest_met_visibility(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one visibility fetch cycle."""
    ingestor = MetVisibilityIngestor(board, graph, scheduler)
    await ingestor.run()
