"""HERE Traffic Flow ingestor — real-time traffic speed from vehicle probe data.

Uses the HERE Traffic Flow API v7 to fetch real-time traffic flow data for
key London corridors. HERE aggregates anonymous GPS traces from connected
vehicles and fleet partners, providing traffic intelligence independent of
TfL infrastructure.

Free tier: 250,000 transactions/month at https://platform.here.com/
Set HERE_API_KEY in .env.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.here_traffic")

# HERE Traffic Flow API v7
_FLOW_URL = "https://data.traffic.hereapi.com/v7/flow"

# Probe points on major London corridors (lat, lon, label)
# Chosen to complement TomTom probe points with different locations
_PROBE_POINTS = [
    (51.5308, -0.1238, "A501 Euston Rd"),
    (51.4945, -0.1760, "A4 Cromwell Rd/Earls Court"),
    (51.5430, -0.0990, "A1 Holloway Rd"),
    (51.4630, -0.1680, "A3 Clapham Common"),
    (51.5100, -0.0550, "A13 Commercial Rd"),
    (51.4510, -0.0530, "A2 New Cross Gate"),
    (51.5680, -0.1370, "A400 Kentish Town Rd"),
    (51.4870, -0.1600, "A3216 Chelsea Embankment"),
    (51.5200, -0.0780, "A1211 Tower Hill"),
    (51.4750, -0.0300, "A200 Lower Rd Rotherhithe"),
    (51.5480, -0.0550, "A107 Dalston Lane"),
    (51.4400, -0.1550, "A24 Tooting High St"),
]


class HereTrafficIngestor(BaseIngestor):
    source_name = "here_traffic"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: LondonGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self.api_key = os.environ.get("HERE_API_KEY", "")

    async def fetch_data(self) -> Any:
        if not self.api_key:
            self.log.debug("HERE_API_KEY not set, skipping")
            return None

        results: list[dict] = []

        for lat, lon, label in _PROBE_POINTS:
            data = await self.fetch(
                _FLOW_URL,
                params={
                    "apiKey": self.api_key,
                    "in": f"circle:{lat},{lon};r=500",
                    "locationReferencing": "none",
                },
            )
            if data and isinstance(data, dict):
                for result in data.get("results", []):
                    result["_probe_label"] = label
                    result["_probe_lat"] = lat
                    result["_probe_lon"] = lon
                    results.append(result)

        return results if results else None

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected HERE data format: %s", type(data))
            return

        processed = 0

        for result in data:
            label = result.get("_probe_label", "Unknown")
            lat = result.get("_probe_lat")
            lon = result.get("_probe_lon")
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            current_flow = result.get("currentFlow", {})
            speed = current_flow.get("speed")  # m/s
            free_flow = current_flow.get("freeFlow")  # m/s
            jam_factor = current_flow.get("jamFactor", 0)  # 0-10
            confidence = current_flow.get("confidence", 0)
            traversability = current_flow.get("traversability", "")

            if speed is None or free_flow is None:
                continue

            # Convert m/s to km/h
            speed_kmh = round(speed * 3.6, 1)
            free_flow_kmh = round(free_flow * 3.6, 1)

            # Congestion ratio: 0 = free flow, 1 = standstill
            congestion = 0.0
            if free_flow > 0:
                congestion = round(1.0 - (speed / free_flow), 3)
                congestion = max(0.0, min(1.0, congestion))

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=congestion,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "label": label,
                    "speed_kmh": speed_kmh,
                    "free_flow_kmh": free_flow_kmh,
                    "jam_factor": jam_factor,
                    "confidence": confidence,
                    "congestion_ratio": congestion,
                    "traversability": traversability,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"HERE flow [{label}] speed={speed_kmh}km/h "
                    f"freeflow={free_flow_kmh}km/h jam={jam_factor}/10 "
                    f"congestion={congestion:.1%}"
                ),
                data={
                    "label": label,
                    "speed_kmh": speed_kmh,
                    "free_flow_kmh": free_flow_kmh,
                    "jam_factor": jam_factor,
                    "congestion_ratio": congestion,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("HERE traffic flow: processed=%d segments", processed)


async def ingest_here_traffic(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one HERE Traffic fetch cycle."""
    ingestor = HereTrafficIngestor(board, graph, scheduler)
    await ingestor.run()
