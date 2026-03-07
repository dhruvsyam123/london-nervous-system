"""Carbon Intensity ingestor — GB grid forecast & generation mix via carbonintensity.org.uk.

Ingests the National Grid ESO / NESO Carbon Intensity API to provide:
- 48-hour carbon intensity forecasts for the London region
- Current generation mix (wind, solar, gas, etc.)
- Actual vs forecast comparison for model calibration

The API is free, requires no key, and updates every 30 minutes.
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

log = logging.getLogger("london.ingestors.carbon_intensity")

BASE_URL = "https://api.carbonintensity.org.uk"

# London postcodes for regional data — central London
LONDON_POSTCODE = "SW1"

# Approximate centre of London for spatial tagging
LONDON_LAT = 51.5074
LONDON_LON = -0.1278


class CarbonIntensityIngestor(BaseIngestor):
    source_name = "carbon_intensity"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Fetch current intensity, 48h forecast, and generation mix."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")

        # Fetch all three endpoints in sequence (rate-limited)
        current = await self.fetch(
            f"{BASE_URL}/regional/postcode/{LONDON_POSTCODE}",
            headers={"Accept": "application/json"},
        )
        forecast = await self.fetch(
            f"{BASE_URL}/regional/intensity/{now}/fw48h/postcode/{LONDON_POSTCODE}",
            headers={"Accept": "application/json"},
        )
        generation = await self.fetch(
            f"{BASE_URL}/generation",
            headers={"Accept": "application/json"},
        )

        if current is None and forecast is None and generation is None:
            return None

        return {
            "current": current,
            "forecast": forecast,
            "generation": generation,
        }

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        processed = 0

        # ── Current regional intensity ──────────────────────────────────
        current = data.get("current")
        if current:
            try:
                region_data = current["data"][0]["data"][0]
                intensity = region_data["intensity"]
                forecast_val = intensity.get("forecast")
                index_label = intensity.get("index", "")

                if forecast_val is not None:
                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=float(forecast_val),
                        location_id=cell_id,
                        lat=LONDON_LAT,
                        lon=LONDON_LON,
                        metadata={
                            "metric": "carbon_intensity_gCO2_per_kWh",
                            "index": index_label,
                            "type": "current",
                        },
                    )
                    await self.board.store_observation(obs)

                    # Extract generation mix from regional response
                    gen_mix = region_data.get("generationmix", [])
                    mix_summary = {
                        g["fuel"]: g["perc"]
                        for g in gen_mix
                        if g.get("perc") is not None
                    }

                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=(
                            f"Carbon intensity [London]: {forecast_val} gCO2/kWh ({index_label})"
                        ),
                        data={
                            "intensity_forecast": forecast_val,
                            "index": index_label,
                            "generation_mix": mix_summary,
                            "lat": LONDON_LAT,
                            "lon": LONDON_LON,
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)
                    processed += 1
            except (KeyError, IndexError, TypeError) as exc:
                self.log.warning("Failed to parse current intensity: %s", exc)

        # ── 48h forecast ────────────────────────────────────────────────
        forecast = data.get("forecast")
        if forecast:
            try:
                periods = forecast["data"]["data"]
                forecast_points = []
                for period in periods:
                    intensity = period.get("intensity", {})
                    f_val = intensity.get("forecast")
                    idx = intensity.get("index", "")
                    from_time = period.get("from", "")
                    if f_val is not None:
                        forecast_points.append({
                            "from": from_time,
                            "forecast": f_val,
                            "index": idx,
                        })

                if forecast_points:
                    # Store summary observation with forecast curve
                    peak = max(forecast_points, key=lambda p: p["forecast"])
                    trough = min(forecast_points, key=lambda p: p["forecast"])

                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=float(peak["forecast"]),
                        location_id=cell_id,
                        lat=LONDON_LAT,
                        lon=LONDON_LON,
                        metadata={
                            "metric": "carbon_intensity_48h_peak_gCO2_per_kWh",
                            "type": "forecast_48h",
                            "peak_time": peak["from"],
                            "peak_index": peak["index"],
                            "trough_value": trough["forecast"],
                            "trough_time": trough["from"],
                            "trough_index": trough["index"],
                            "num_periods": len(forecast_points),
                        },
                    )
                    await self.board.store_observation(obs)

                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=(
                            f"Carbon intensity 48h forecast [London]: "
                            f"peak {peak['forecast']} gCO2/kWh ({peak['index']}) at {peak['from']}, "
                            f"trough {trough['forecast']} gCO2/kWh ({trough['index']}) at {trough['from']}"
                        ),
                        data={
                            "forecast_points": forecast_points,
                            "peak": peak,
                            "trough": trough,
                            "num_periods": len(forecast_points),
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)
                    processed += 1
            except (KeyError, IndexError, TypeError) as exc:
                self.log.warning("Failed to parse 48h forecast: %s", exc)

        # ── National generation mix ─────────────────────────────────────
        generation = data.get("generation")
        if generation:
            try:
                gen_data = generation["data"]
                mix = gen_data.get("generationmix", [])
                mix_dict = {
                    g["fuel"]: g["perc"]
                    for g in mix
                    if g.get("perc") is not None
                }
                from_time = gen_data.get("from", "")
                to_time = gen_data.get("to", "")

                # Calculate renewable percentage
                renewable_fuels = {"wind", "solar", "hydro", "biomass"}
                renewable_pct = sum(
                    mix_dict.get(f, 0) for f in renewable_fuels
                )

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=renewable_pct,
                    location_id=cell_id,
                    lat=LONDON_LAT,
                    lon=LONDON_LON,
                    metadata={
                        "metric": "renewable_generation_pct",
                        "type": "generation_mix",
                        "mix": mix_dict,
                        "from": from_time,
                        "to": to_time,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"GB generation mix: {renewable_pct:.1f}% renewable "
                        f"(wind={mix_dict.get('wind', 0)}%, "
                        f"solar={mix_dict.get('solar', 0)}%, "
                        f"gas={mix_dict.get('gas', 0)}%)"
                    ),
                    data={
                        "generation_mix": mix_dict,
                        "renewable_pct": renewable_pct,
                        "from": from_time,
                        "to": to_time,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1
            except (KeyError, IndexError, TypeError) as exc:
                self.log.warning("Failed to parse generation mix: %s", exc)

        self.log.info("Carbon intensity: processed=%d data streams", processed)


async def ingest_carbon_intensity(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one carbon intensity fetch cycle."""
    ingestor = CarbonIntensityIngestor(board, graph, scheduler)
    await ingestor.run()
