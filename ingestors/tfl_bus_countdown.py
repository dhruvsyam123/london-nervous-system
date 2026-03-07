"""TfL Bus Countdown ingestor — live arrival predictions at bus stops.

Uses the TfL Unified API to fetch real-time bus arrival predictions for
key London bus routes. Unlike bus_avl.py (BODS GPS positions), this provides
*predicted arrival times* at stops — enabling direct verification of
bus-related predictions (e.g. Route 73 delays).

Computes per-stop headways and wait times so the Validator can check
predictions like "Route 73 delays expected" against actual headway spikes.

API docs: https://api.tfl.gov.uk/swagger/ui/index.html
No API key required; TFL_APP_KEY improves rate limits.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.tfl_bus_countdown")

TFL_API_BASE = "https://api.tfl.gov.uk"

# Sample of high-frequency routes across London for broad coverage.
# Route 73: Victoria–Stoke Newington (flagged by Brain for prediction gaps)
# Route 38: Victoria–Clapton (high-frequency central)
# Route 25: Oxford Circus–Ilford (east-west cross-city)
# Route 148: Camberwell–White City (south-north)
# Route 343: New Cross–City (south-east)
MONITORED_LINES = ["73", "38", "25", "148", "343"]

# Expected peak headways (minutes) per route — used to flag degraded service.
EXPECTED_PEAK_HEADWAY: dict[str, float] = {
    "73": 5.0,
    "38": 5.0,
    "25": 6.0,
    "148": 8.0,
    "343": 8.0,
}

# Cached stop coordinates, keyed by naptanId → (lat, lon).
# Persisted in module-level dict so it survives across cycles (but not restarts).
_stop_coords: dict[str, tuple[float, float]] = {}


class TflBusCountdownIngestor(BaseIngestor):
    source_name = "tfl_bus_countdown"
    rate_limit_name = "tfl"

    async def _ensure_stop_coords(self, line_id: str) -> None:
        """Fetch and cache stop coordinates for a line if not already cached."""
        # Check if we already have stops for this line
        if any(k.startswith(f"_line_{line_id}_") for k in _stop_coords):
            return
        url = f"{TFL_API_BASE}/Line/{line_id}/StopPoints"
        params: dict[str, str] = {}
        app_key = os.environ.get("TFL_APP_KEY")
        if app_key:
            params["app_key"] = app_key
        data = await self.fetch(url, params=params)
        if not data or not isinstance(data, list):
            return
        for stop in data:
            naptan = stop.get("naptanId")
            lat = stop.get("lat")
            lon = stop.get("lon")
            if naptan and lat is not None and lon is not None:
                _stop_coords[naptan] = (float(lat), float(lon))
                # Mark that we've loaded this line's stops
                _stop_coords[f"_line_{line_id}_loaded"] = (0.0, 0.0)

    async def fetch_data(self) -> Any:
        params: dict[str, str] = {}
        app_key = os.environ.get("TFL_APP_KEY")
        if app_key:
            params["app_key"] = app_key

        all_arrivals: list[dict] = []
        for line_id in MONITORED_LINES:
            await self._ensure_stop_coords(line_id)
            url = f"{TFL_API_BASE}/Line/{line_id}/Arrivals"
            data = await self.fetch(url, params=params)
            if isinstance(data, list):
                all_arrivals.extend(data)
            elif data is not None:
                self.log.warning("Unexpected response for line %s: %s", line_id, type(data))

        return all_arrivals if all_arrivals else None

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            return

        # Group arrivals by (line, naptanId) to compute per-stop headways
        by_stop: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for arrival in data:
            line_id = arrival.get("lineId", "")
            naptan = arrival.get("naptanId", "")
            if line_id and naptan and arrival.get("timeToStation") is not None:
                by_stop[(line_id, naptan)].append(arrival)

        processed = 0

        for (line_id, naptan), preds in by_stop.items():
            preds.sort(key=lambda p: p["timeToStation"])
            times = [p["timeToStation"] for p in preds]
            n_vehicles = len(times)

            if n_vehicles == 0:
                continue

            # Resolve coordinates
            coords = _stop_coords.get(naptan)
            if coords:
                lat, lon = coords
            else:
                lat = lon = None
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            avg_wait_s = sum(times) / n_vehicles
            min_wait_s = times[0]

            # Compute headway (gap between consecutive buses) in minutes
            headway_min = None
            if n_vehicles >= 2:
                gaps = [
                    (times[i + 1] - times[i]) / 60.0
                    for i in range(n_vehicles - 1)
                ]
                headway_min = round(sum(gaps) / len(gaps), 1)

            # Bunching: vehicles arriving within 60s of each other
            bunched = sum(
                1 for i in range(1, len(times)) if times[i] - times[i - 1] < 60
            )

            # Headway ratio vs expected (for anomaly detection)
            expected = EXPECTED_PEAK_HEADWAY.get(line_id, 7.0)
            headway_ratio = (headway_min / expected) if headway_min and expected > 0 else None

            stop_name = preds[0].get("stationName", naptan)
            direction = preds[0].get("direction", "")
            towards = preds[0].get("towards", "")

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=round(avg_wait_s, 1),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "stop_name": stop_name,
                    "stop_naptan": naptan,
                    "line": line_id,
                    "direction": direction,
                    "towards": towards,
                    "n_vehicles": n_vehicles,
                    "avg_wait_s": round(avg_wait_s, 1),
                    "min_wait_s": min_wait_s,
                    "headway_min": headway_min,
                    "headway_ratio": round(headway_ratio, 2) if headway_ratio else None,
                    "bunched_pairs": bunched,
                    "metric": "bus_countdown_wait",
                },
            )
            await self.board.store_observation(obs)

            headway_str = f" headway={headway_min}min" if headway_min else ""
            ratio_str = f" ratio={headway_ratio:.1f}x" if headway_ratio else ""
            bunching_str = f" bunched={bunched}" if bunched else ""

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Bus countdown [{line_id}] {stop_name}: "
                    f"vehicles={n_vehicles} avg_wait={avg_wait_s:.0f}s"
                    f"{headway_str}{ratio_str}{bunching_str}"
                ),
                data={
                    "stop_name": stop_name,
                    "stop_naptan": naptan,
                    "line": line_id,
                    "direction": direction,
                    "n_vehicles": n_vehicles,
                    "avg_wait_s": round(avg_wait_s, 1),
                    "min_wait_s": min_wait_s,
                    "headway_min": headway_min,
                    "headway_ratio": round(headway_ratio, 2) if headway_ratio else None,
                    "bunched_pairs": bunched,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "TfL bus countdown: stops=%d (lines=%s)",
            processed, ",".join(MONITORED_LINES),
        )


async def ingest_tfl_bus_countdown(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL bus countdown fetch cycle."""
    ingestor = TflBusCountdownIngestor(board, graph, scheduler)
    await ingestor.run()
