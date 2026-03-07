"""National Grid system frequency ingestor — real-time 50Hz grid frequency.

Fetches rolling system frequency from the Elexon BMRS API (datasets/FREQ).
The UK grid nominally runs at 50Hz; deviations indicate supply/demand imbalance.
Frequency drops below 49.8Hz or rises above 50.2Hz signal grid stress.

This is a low-bandwidth, high-reliability signal of national infrastructure health.
The Brain can correlate frequency excursions with other energy/transport anomalies.

Free API, no key required. Data updates every 15 seconds.
We sample every 5 minutes, taking the most recent reading plus min/max over the window.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.grid_frequency")

# Elexon BMRS Insights API — no key required
FREQ_URL = "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ"

LONDON_LAT = 51.5
LONDON_LON = -0.12

# Nominal frequency and alert thresholds
NOMINAL_HZ = 50.0
WARN_LOW = 49.8
WARN_HIGH = 50.2
STATUTORY_LOW = 49.5
STATUTORY_HIGH = 50.5


class GridFrequencyIngestor(BaseIngestor):
    source_name = "grid_frequency"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(FREQ_URL, params={"format": "json"})

    async def process(self, data: Any) -> None:
        # Response is {"data": [...]}, unwrap it
        if isinstance(data, dict):
            entries = data.get("data", [])
        elif isinstance(data, list):
            entries = data
        else:
            self.log.warning("Unexpected frequency data format: %s", type(data))
            return

        if not entries:
            self.log.warning("No frequency entries in response")
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        # Extract frequency values
        readings = []
        for entry in entries:
            freq = entry.get("frequency")
            if freq is not None:
                try:
                    readings.append(float(freq))
                except (ValueError, TypeError):
                    continue

        if not readings:
            self.log.warning("No valid frequency readings in response")
            return

        latest = readings[0]  # most recent
        freq_min = min(readings)
        freq_max = max(readings)
        freq_mean = sum(readings) / len(readings)

        # Determine status
        status = "normal"
        if latest <= STATUTORY_LOW or latest >= STATUTORY_HIGH:
            status = "statutory_breach"
        elif latest <= WARN_LOW or latest >= WARN_HIGH:
            status = "warning"

        deviation = latest - NOMINAL_HZ

        # Store the latest frequency as a numeric observation
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=latest,
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "system_frequency_hz",
                "deviation_hz": round(deviation, 4),
                "min_hz": round(freq_min, 4),
                "max_hz": round(freq_max, 4),
                "mean_hz": round(freq_mean, 4),
                "sample_count": len(readings),
                "status": status,
            },
        )
        await self.board.store_observation(obs)

        # Post message to #raw
        parts = [f"freq={latest:.3f}Hz"]
        parts.append(f"dev={deviation:+.3f}Hz")
        parts.append(f"range=[{freq_min:.3f}-{freq_max:.3f}]")
        parts.append(f"status={status}")

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=f"Grid frequency: {', '.join(parts)}",
            data={
                "frequency_hz": latest,
                "deviation_hz": round(deviation, 4),
                "min_hz": round(freq_min, 4),
                "max_hz": round(freq_max, 4),
                "mean_hz": round(freq_mean, 4),
                "sample_count": len(readings),
                "status": status,
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info(
            "Grid frequency: %.3f Hz (dev=%+.3f, status=%s, samples=%d)",
            latest, deviation, status, len(readings),
        )


async def ingest_grid_frequency(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one grid frequency fetch cycle."""
    ingestor = GridFrequencyIngestor(board, graph, scheduler)
    await ingestor.run()
