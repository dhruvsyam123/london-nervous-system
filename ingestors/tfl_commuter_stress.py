"""TfL Commuter Stress Index — composite network-wide stress from line disruptions.

Computes a 0-100 Commuter Stress Index by fetching real-time severity levels
(0-10) for every tube, DLR, Overground, and Elizabeth line, weighting each by
approximate daily passenger volume, and normalising into a single score.

The Brain suggested tap-in/tap-out data, but TfL does not expose Oyster/
contactless tap data via any public API.  This ingestor achieves the same goal
— quantifying unexpected journey-time stress — by combining per-line disruption
severity with passenger-volume weights:
  stress = Σ(severity_i × weight_i) / Σ(max_severity × weight_i) × 100

Severity mapping (TfL statusSeverity values):
  10 = Good Service, 0 = Special Service, 5 = Part Closure, 6 = Severe Delays,
  7 = Reduced Service, 9 = Minor Delays, 20 = Service Closed, etc.
We invert these so higher = worse stress.

No API key required (TfL open data).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.tfl_commuter_stress")

# TfL Line Status endpoint — returns all lines with current status
TFL_LINE_STATUS_URL = (
    "https://api.tfl.gov.uk/Line/Mode/"
    "tube,dlr,overground,elizabeth-line/Status"
)

# TfL statusSeverity → stress score (0 = no stress, 10 = maximum stress).
# TfL uses higher numbers for better service, so we invert.
# Reference: https://api.tfl.gov.uk/Line/Meta/Severity
_SEVERITY_TO_STRESS: dict[int, float] = {
    10: 0.0,   # Good Service
    9: 2.0,    # Minor Delays
    8: 3.0,    # Reduced service (planned)
    7: 5.0,    # Reduced Service
    6: 8.0,    # Severe Delays
    5: 7.0,    # Part Closure
    4: 6.0,    # Planned Closure
    3: 4.0,    # Part Suspended
    2: 9.0,    # Suspended
    1: 6.0,    # Part Closure (variant)
    0: 5.0,    # Special Service
    20: 10.0,  # Service Closed
}

# Approximate daily passengers (thousands) per line — from TfL annual reports.
# Used to weight the index so Victoria line disruptions matter more than
# Waterloo & City. Numbers are rough 2023 estimates.
_LINE_WEIGHT: dict[str, float] = {
    "bakerloo": 280,
    "central": 700,
    "circle": 260,
    "district": 500,
    "hammersmith-city": 250,
    "jubilee": 600,
    "metropolitan": 370,
    "northern": 800,
    "piccadilly": 550,
    "victoria": 700,
    "waterloo-city": 50,
    "dlr": 350,
    "london-overground": 500,
    "elizabeth": 600,
    "tram": 80,
}

# Representative station coordinates per line for spatial indexing.
# Chosen as a central station on each line.
_LINE_LOCATION: dict[str, tuple[float, float]] = {
    "bakerloo": (51.5226, -0.1571),   # Baker Street
    "central": (51.5152, -0.1415),    # Oxford Circus
    "circle": (51.5113, -0.0904),     # Monument
    "district": (51.4913, -0.2228),   # Hammersmith
    "hammersmith-city": (51.5178, -0.0823),  # Liverpool Street
    "jubilee": (51.5035, -0.0187),    # Canary Wharf
    "metropolitan": (51.5733, -0.3536),  # Harrow-on-the-Hill
    "northern": (51.5113, -0.0904),   # Bank
    "piccadilly": (51.5067, -0.1428), # Green Park
    "victoria": (51.5308, -0.1238),   # King's Cross
    "waterloo-city": (51.5133, -0.0886),  # Bank
    "dlr": (51.5035, -0.0187),        # Canary Wharf
    "london-overground": (51.5416, -0.0042),  # Stratford
    "elizabeth": (51.5154, -0.1755),  # Paddington
    "tram": (51.3763, -0.0986),       # East Croydon area
}

_DEFAULT_WEIGHT = 200.0


class TflCommuterStressIngestor(BaseIngestor):
    source_name = "tfl_commuter_stress"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_LINE_STATUS_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected Line Status response type: %s", type(data))
            return

        weighted_stress_sum = 0.0
        weight_sum = 0.0
        per_line: list[dict[str, Any]] = []
        disrupted_lines: list[str] = []
        processed = 0

        for line in data:
            line_id = line.get("id", "")
            line_name = line.get("name", line_id)
            statuses = line.get("lineStatuses", [])

            if not statuses:
                continue

            # Take the worst (highest stress) status if multiple
            worst_stress = 0.0
            worst_reason = ""
            worst_severity_raw = 10

            for status in statuses:
                sev = status.get("statusSeverity", 10)
                stress = _SEVERITY_TO_STRESS.get(sev, 5.0)
                if stress > worst_stress:
                    worst_stress = stress
                    worst_severity_raw = sev
                    worst_reason = status.get("reason", "")

            weight = _LINE_WEIGHT.get(line_id, _DEFAULT_WEIGHT)
            weighted_stress_sum += worst_stress * weight
            weight_sum += 10.0 * weight  # max possible stress per line

            per_line.append({
                "line_id": line_id,
                "line_name": line_name,
                "stress": worst_stress,
                "severity_raw": worst_severity_raw,
                "reason": worst_reason[:300],
                "weight": weight,
            })

            if worst_stress >= 2.0:
                disrupted_lines.append(line_name)

            # Post per-line observation for spatial correlation
            lat, lon = _LINE_LOCATION.get(line_id, (None, None))
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=worst_stress,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "line_id": line_id,
                    "line_name": line_name,
                    "severity_raw": worst_severity_raw,
                    "metric": "line_stress",
                    "reason": worst_reason[:300],
                },
            )
            await self.board.store_observation(obs)
            processed += 1

        # Compute composite index (0-100)
        if weight_sum > 0:
            composite_index = (weighted_stress_sum / weight_sum) * 100.0
        else:
            composite_index = 0.0

        # Post network-wide composite observation
        # Use central London as location (Oxford Circus area)
        central_lat, central_lon = 51.5152, -0.1415
        cell_id = self.graph.latlon_to_cell(central_lat, central_lon)

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=round(composite_index, 1),
            location_id=cell_id,
            lat=central_lat,
            lon=central_lon,
            metadata={
                "metric": "commuter_stress_index",
                "composite_index": round(composite_index, 1),
                "lines_monitored": len(per_line),
                "lines_disrupted": len(disrupted_lines),
                "disrupted_lines": disrupted_lines,
            },
        )
        await self.board.store_observation(obs)

        # Summary message
        if disrupted_lines:
            disrupted_str = ", ".join(disrupted_lines[:5])
            if len(disrupted_lines) > 5:
                disrupted_str += f" +{len(disrupted_lines) - 5} more"
        else:
            disrupted_str = "none"

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Commuter Stress Index: {composite_index:.1f}/100 "
                f"({len(per_line)} lines monitored, "
                f"{len(disrupted_lines)} disrupted: {disrupted_str})"
            ),
            data={
                "composite_index": round(composite_index, 1),
                "lines_monitored": len(per_line),
                "lines_disrupted": len(disrupted_lines),
                "disrupted_lines": disrupted_lines,
                "per_line": per_line,
                "lat": central_lat,
                "lon": central_lon,
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info(
            "Commuter Stress Index: %.1f/100 (%d lines, %d disrupted)",
            composite_index, len(per_line), len(disrupted_lines),
        )


async def ingest_tfl_commuter_stress(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Commuter Stress Index cycle."""
    ingestor = TflCommuterStressIngestor(board, graph, scheduler)
    await ingestor.run()
