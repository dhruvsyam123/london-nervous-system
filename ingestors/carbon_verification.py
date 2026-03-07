"""Carbon intensity forecast verification — regional London data + forecast-vs-actual.

Addresses the 'prediction verification blind spot' identified by the Brain agent.
The existing energy.py ingestor fetches national-level current intensity, but:
1. It doesn't get London-region-specific intensity (South England region)
2. It doesn't compare past forecasts against actual outcomes

This ingestor fetches:
- Regional carbon intensity for London (postcode EC1) with forecast AND actual
- 24h forward regional forecast for prediction tracking
- Recent past intensity data to compute forecast error (forecast vs actual)

Free API (carbonintensity.org.uk), no key required.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.carbon_verification")

# Carbon Intensity API — regional endpoints
BASE_URL = "https://api.carbonintensity.org.uk"
# EC1 = City of London / central London → resolves to South England region
REGIONAL_CURRENT_URL = f"{BASE_URL}/regional/postcode/EC1"
# 24h forward forecast for all regions
REGIONAL_FORECAST_URL = f"{BASE_URL}/regional/intensity/{{}}/fw24h"
# Past 24h national intensity (has both forecast and actual for verification)
INTENSITY_PAST_24H_URL = f"{BASE_URL}/intensity/date"

LONDON_LAT = 51.5
LONDON_LON = -0.12


class CarbonVerificationIngestor(BaseIngestor):
    source_name = "carbon_verification"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # 1. Current regional intensity for London
        regional = await self.fetch(REGIONAL_CURRENT_URL)

        # 2. Past 24h national data (contains forecast + actual for verification)
        past = await self.fetch(INTENSITY_PAST_24H_URL)

        return {"regional": regional, "past": past}

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        regional_ok = await self._process_regional(data.get("regional"), cell_id)
        verification = await self._process_verification(data.get("past"), cell_id)

        self.log.info(
            "Carbon verification: regional=%s, verification_periods=%d",
            "ok" if regional_ok else "no_data",
            verification,
        )

    async def _process_regional(self, data: Any, cell_id: str | None) -> bool:
        """Process regional carbon intensity for London's region."""
        if not isinstance(data, dict):
            self.log.warning("Unexpected regional response: %s", type(data))
            return False

        # Response: {"data": [{"regionid": N, "shortname": "South England", ...}]}
        entries = data.get("data", [])
        if not entries:
            return False

        entry = entries[0] if isinstance(entries, list) else entries
        region_name = entry.get("shortname", "unknown")
        region_id = entry.get("regionid")

        intensity_data = entry.get("data", [])
        if isinstance(intensity_data, list) and intensity_data:
            period = intensity_data[0]
        elif isinstance(intensity_data, dict):
            period = intensity_data
        else:
            return False

        intensity = period.get("intensity", {})
        forecast = _float(intensity.get("forecast"))
        actual = _float(intensity.get("actual"))
        index_label = intensity.get("index", "")

        gen_mix = period.get("generationmix", [])

        # Use actual if available, otherwise forecast
        value = actual if actual is not None else forecast
        if value is None:
            return False

        # Compute forecast error if both available
        forecast_error = None
        forecast_error_pct = None
        if actual is not None and forecast is not None and forecast > 0:
            forecast_error = actual - forecast
            forecast_error_pct = round((forecast_error / forecast) * 100, 1)

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=value,
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "regional_carbon_intensity_gco2_kwh",
                "region": region_name,
                "region_id": region_id,
                "index": index_label,
                "actual": actual,
                "forecast": forecast,
                "forecast_error": forecast_error,
                "forecast_error_pct": forecast_error_pct,
            },
        )
        await self.board.store_observation(obs)

        # Extract regional generation mix highlights
        renewables_pct = 0.0
        fossil_pct = 0.0
        for fuel in gen_mix:
            fuel_name = fuel.get("fuel", "")
            perc = _float(fuel.get("perc"))
            if perc is None:
                continue
            if fuel_name in ("wind", "solar", "hydro", "biomass"):
                renewables_pct += perc
            elif fuel_name in ("gas", "coal", "oil"):
                fossil_pct += perc

        content_parts = [
            f"Regional carbon [{region_name}]: {value:.0f} gCO2/kWh [{index_label}]",
        ]
        if forecast_error is not None:
            content_parts.append(
                f"forecast_error={forecast_error:+.0f} ({forecast_error_pct:+.1f}%)"
            )
        if renewables_pct > 0:
            content_parts.append(f"renewables={renewables_pct:.1f}%")

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=" | ".join(content_parts),
            data={
                "region": region_name,
                "region_id": region_id,
                "intensity_gco2_kwh": value,
                "actual": actual,
                "forecast": forecast,
                "forecast_error": forecast_error,
                "forecast_error_pct": forecast_error_pct,
                "index": index_label,
                "renewables_pct": round(renewables_pct, 1),
                "fossil_pct": round(fossil_pct, 1),
                "generation_mix": gen_mix,
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)
        return True

    async def _process_verification(self, data: Any, cell_id: str | None) -> int:
        """Compute forecast-vs-actual accuracy over past 24h periods."""
        if not isinstance(data, dict):
            return 0

        entries = data.get("data", [])
        if not isinstance(entries, list):
            return 0

        errors: list[float] = []
        abs_errors: list[float] = []

        for entry in entries:
            intensity = entry.get("intensity", {})
            actual = _float(intensity.get("actual"))
            forecast = _float(intensity.get("forecast"))

            if actual is None or forecast is None or forecast == 0:
                continue

            error = actual - forecast
            errors.append(error)
            abs_errors.append(abs(error))

        if not errors:
            return 0

        # Summary statistics
        mean_error = sum(errors) / len(errors)
        mean_abs_error = sum(abs_errors) / len(abs_errors)
        max_abs_error = max(abs_errors)

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=mean_abs_error,
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "carbon_forecast_mae_gco2_kwh",
                "mean_error": round(mean_error, 1),
                "mean_abs_error": round(mean_abs_error, 1),
                "max_abs_error": round(max_abs_error, 1),
                "periods_verified": len(errors),
            },
        )
        await self.board.store_observation(obs)

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Carbon forecast verification (24h): "
                f"MAE={mean_abs_error:.1f} gCO2/kWh, "
                f"bias={mean_error:+.1f}, "
                f"max_error={max_abs_error:.1f}, "
                f"periods={len(errors)}"
            ),
            data={
                "verification_type": "carbon_intensity_forecast",
                "mean_error": round(mean_error, 1),
                "mean_abs_error": round(mean_abs_error, 1),
                "max_abs_error": round(max_abs_error, 1),
                "periods_verified": len(errors),
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        return len(errors)


def _float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


async def ingest_carbon_verification(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one carbon verification fetch cycle."""
    ingestor = CarbonVerificationIngestor(board, graph, scheduler)
    await ingestor.run()
