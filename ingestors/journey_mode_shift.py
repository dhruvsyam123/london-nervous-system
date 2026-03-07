"""Journey mode-shift detector — tracks when TfL Journey Planner routes away from Tube.

The Brain suggested ingesting aggregate journey planner query data to detect
'tube-avoiding' travel patterns. That data isn't publicly available (would
require commercial partnerships with Citymapper/TfL Go). This ingestor
implements a feasible proxy: it queries TfL Journey Planner for key corridors
with all modes enabled, then analyses whether the planner is recommending
tube or routing passengers onto alternatives (bus, overground, walking).

A sudden drop in tube-recommending routes on a corridor that normally uses
the Tube is a direct, real-time signal of mode shift — the same leading
indicator the Brain wanted from aggregate query data.

Complements commuter_displacement.py (which tracks travel *times*) by
focusing on mode *composition* of recommended journeys.

No API key required (TfL open data).
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.journey_mode_shift")

TFL_JOURNEY_URL = "https://api.tfl.gov.uk/Journey/JourneyResults/{origin}/to/{destination}"

# Corridors that normally rely on the Tube. If the planner stops
# recommending tube here, something significant is happening.
# (name, origin_coord, dest_coord, expected_primary_mode)
TUBE_CORRIDORS = [
    ("Victoria → Oxford Circus", "51.4952,-0.1439", "51.5154,-0.1420", "tube"),
    ("Kings Cross → Bank", "51.5308,-0.1238", "51.5133,-0.0886", "tube"),
    ("Stratford → Liverpool St", "51.5430,-0.0035", "51.5178,-0.0823", "tube"),
    ("Brixton → Green Park", "51.4613,-0.1146", "51.5067,-0.1428", "tube"),
    ("Hammersmith → Piccadilly Circus", "51.4927,-0.2246", "51.5100,-0.1347", "tube"),
    ("Finsbury Park → Angel", "51.5642,-0.1065", "51.5327,-0.1059", "tube"),
    ("Walthamstow → Oxford Circus", "51.5830,-0.0200", "51.5154,-0.1420", "tube"),
    ("Canning Town → Canary Wharf", "51.5147,0.0082", "51.5054,-0.0235", "tube"),
]

# Modes we track — the tube-vs-alternative split
TUBE_MODES = {"tube", "dlr", "elizabeth-line"}
ALT_MODES = {"bus", "overground", "national-rail", "walking", "cycle"}


def _classify_journey(journey: dict) -> dict:
    """Extract mode composition from a single journey result."""
    mode_minutes: Counter[str] = Counter()
    total_duration = journey.get("duration", 0)

    for leg in journey.get("legs", []):
        leg_dur = leg.get("duration", 0)
        mode_info = leg.get("mode", {})
        mode_name = mode_info.get("id", "") if isinstance(mode_info, dict) else str(mode_info)
        mode_minutes[mode_name] += leg_dur

    uses_tube = bool(TUBE_MODES & set(mode_minutes.keys()))
    tube_minutes = sum(mode_minutes.get(m, 0) for m in TUBE_MODES)
    alt_minutes = sum(v for k, v in mode_minutes.items() if k not in TUBE_MODES and k != "walking")

    return {
        "duration": total_duration,
        "uses_tube": uses_tube,
        "tube_minutes": tube_minutes,
        "alt_minutes": alt_minutes,
        "modes": dict(mode_minutes),
    }


class JourneyModeShiftIngestor(BaseIngestor):
    source_name = "journey_mode_shift"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        results = []
        now = datetime.now(timezone.utc)
        time_str = now.strftime("%H%M")

        for name, origin, destination, expected_mode in TUBE_CORRIDORS:
            url = TFL_JOURNEY_URL.format(origin=origin, destination=destination)
            params = {
                "mode": "tube,dlr,overground,elizabeth-line,bus,national-rail,walking",
                "time": time_str,
                "timeIs": "Departing",
                "journeyPreference": "LeastTime",
            }
            data = await self.fetch(url, params=params, retries=2)
            if data and isinstance(data, dict):
                results.append((name, origin, destination, expected_mode, data))

        return results if results else None

    async def process(self, data: Any) -> None:
        processed = 0

        for name, origin, destination, expected_mode, response in data:
            journeys = response.get("journeys", [])
            if not journeys:
                continue

            classified = [_classify_journey(j) for j in journeys]
            total_routes = len(classified)
            tube_routes = sum(1 for c in classified if c["uses_tube"])
            alt_only_routes = total_routes - tube_routes

            # Key metric: what fraction of recommended routes use the Tube?
            tube_fraction = tube_routes / total_routes if total_routes > 0 else 0.0

            # Average tube minutes across tube-using routes
            avg_tube_min = 0.0
            if tube_routes > 0:
                avg_tube_min = sum(c["tube_minutes"] for c in classified if c["uses_tube"]) / tube_routes

            # Fastest tube route vs fastest alt route
            tube_durations = [c["duration"] for c in classified if c["uses_tube"]]
            alt_durations = [c["duration"] for c in classified if not c["uses_tube"]]
            fastest_tube = min(tube_durations) if tube_durations else None
            fastest_alt = min(alt_durations) if alt_durations else None

            # Aggregate modes across all journeys
            all_modes: Counter[str] = Counter()
            for c in classified:
                for mode, mins in c["modes"].items():
                    all_modes[mode] += mins

            # Midpoint for spatial indexing
            try:
                olat, olon = [float(x) for x in origin.split(",")]
                dlat, dlon = [float(x) for x in destination.split(",")]
                mid_lat = (olat + dlat) / 2
                mid_lon = (olon + dlon) / 2
            except (ValueError, IndexError):
                mid_lat = mid_lon = None

            cell_id = self.graph.latlon_to_cell(mid_lat, mid_lon) if mid_lat else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=round(tube_fraction * 100, 1),  # percentage of routes using tube
                location_id=cell_id,
                lat=mid_lat,
                lon=mid_lon,
                metadata={
                    "corridor": name,
                    "expected_mode": expected_mode,
                    "tube_fraction_pct": round(tube_fraction * 100, 1),
                    "tube_routes": tube_routes,
                    "alt_only_routes": alt_only_routes,
                    "total_routes": total_routes,
                    "fastest_tube_min": fastest_tube,
                    "fastest_alt_min": fastest_alt,
                    "avg_tube_min": round(avg_tube_min, 1),
                    "mode_minutes": dict(all_modes),
                },
            )
            await self.board.store_observation(obs)

            # Determine shift status
            if tube_fraction == 0 and total_routes > 0:
                status = "FULL_SHIFT"  # planner completely avoiding tube
            elif tube_fraction < 0.5:
                status = "PARTIAL_SHIFT"  # majority of routes avoid tube
            else:
                status = "NORMAL"  # tube still primary recommendation

            extra = ""
            if fastest_tube is not None and fastest_alt is not None:
                extra = f" tube={fastest_tube}min alt={fastest_alt}min"

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Mode shift [{name}] tube_use={tube_fraction:.0%} "
                    f"({tube_routes}/{total_routes} routes) ({status}){extra}"
                ),
                data={
                    "corridor": name,
                    "tube_fraction_pct": round(tube_fraction * 100, 1),
                    "tube_routes": tube_routes,
                    "total_routes": total_routes,
                    "status": status,
                    "fastest_tube_min": fastest_tube,
                    "fastest_alt_min": fastest_alt,
                    "lat": mid_lat,
                    "lon": mid_lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Journey mode shift: processed=%d corridors", processed)


async def ingest_journey_mode_shift(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one journey mode-shift fetch cycle."""
    ingestor = JourneyModeShiftIngestor(board, graph, scheduler)
    await ingestor.run()
