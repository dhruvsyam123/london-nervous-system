"""Elexon BMRS balancing mechanism ingestor — system prices, imbalance volumes, and grid frequency.

Fetches two complementary datasets from the Elexon BMRS API:

1. **System Prices** (settlement/system-prices): half-hourly System Sell Price (SSP)
   and System Buy Price (SBP), net imbalance volume, total accepted offer/bid volumes.
   Price spikes reveal grid stress; the spread between SSP and SBP indicates balancing
   cost and market tension.

2. **System Frequency** (system/frequency): 15-second resolution measurements of the
   GB grid frequency (target 50.000 Hz). Deviations indicate real-time supply/demand
   imbalance — frequency drops when demand exceeds supply, rises when supply exceeds
   demand. Sustained deviations beyond ±0.2 Hz are operationally significant.

Together these provide the granular, real-time balancing data that turns speculative
grid-state predictions into verifiable ones, complementing the existing grid_generation
(FUELINST), grid_demand (INDO), and grid_status (NESO SOP/OPMR) ingestors.

Free API, no key required.
"""

from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.bmrs_balancing")

# Elexon BMRS API v1 endpoints
SYSTEM_PRICES_URL = "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices"
SYSTEM_FREQ_URL = "https://data.elexon.co.uk/bmrs/api/v1/system/frequency"

# Grid-level data — not location-specific, pin to central London
LONDON_LAT = 51.5
LONDON_LON = -0.12

# Frequency thresholds (Hz)
FREQ_TARGET = 50.0
FREQ_WARN_DEVIATION = 0.1   # notable
FREQ_ALERT_DEVIATION = 0.2  # operationally significant


class BmrsBalancingIngestor(BaseIngestor):
    source_name = "bmrs_balancing"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")

        # Fetch today's system prices (half-hourly settlement periods)
        prices = await self.fetch(
            f"{SYSTEM_PRICES_URL}/{today}",
            params={"format": "json"},
        )

        # Fetch last 30 min of frequency data (120 records at 15s intervals)
        freq_from = (now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        freq_to = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        freq = await self.fetch(
            SYSTEM_FREQ_URL,
            params={"from": freq_from, "to": freq_to, "format": "json"},
        )

        return {"prices": prices, "frequency": freq}

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        price_count = await self._process_prices(data.get("prices"), cell_id)
        freq_count = await self._process_frequency(data.get("frequency"), cell_id)
        self.log.info(
            "BMRS balancing: %d price records, %d freq records processed",
            price_count, freq_count,
        )

    async def _process_prices(self, raw: Any, cell_id: str | None) -> int:
        """Process system prices — store latest settlement period."""
        if not isinstance(raw, dict):
            self.log.warning("Unexpected system prices response: %s", type(raw))
            return 0

        records = raw.get("data", [])
        if not records:
            self.log.info("No system price records returned")
            return 0

        # Use most recent settlement period
        latest = records[-1]
        ssp = _float(latest.get("systemSellPrice"))
        sbp = _float(latest.get("systemBuyPrice"))
        net_imbalance = _float(latest.get("netImbalanceVolume"))
        offer_vol = _float(latest.get("totalAcceptedOfferVolume"))
        bid_vol = _float(latest.get("totalAcceptedBidVolume"))
        period = latest.get("settlementPeriod")
        start_time = latest.get("startTime", "")

        stored = 0
        metrics = {
            "system_sell_price_gbp_mwh": ssp,
            "system_buy_price_gbp_mwh": sbp,
            "net_imbalance_volume_mwh": net_imbalance,
            "accepted_offer_volume_mwh": offer_vol,
            "accepted_bid_volume_mwh": bid_vol,
        }

        for metric_name, value in metrics.items():
            if value is None:
                continue
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": metric_name,
                    "settlement_period": period,
                    "start_time": start_time,
                },
            )
            await self.board.store_observation(obs)
            stored += 1

        # Compute price spread (SSP - SBP) as a stress indicator
        spread = None
        if ssp is not None and sbp is not None:
            spread = abs(ssp - sbp)

        # Determine price stress level
        price_stress = "normal"
        if ssp is not None:
            if ssp > 300:
                price_stress = "very_high"
            elif ssp > 150:
                price_stress = "elevated"
            elif ssp < 0:
                price_stress = "negative"

        # Build recent price trend from last few periods
        recent_prices = []
        for r in records[-6:]:
            p = _float(r.get("systemBuyPrice"))
            if p is not None:
                recent_prices.append(p)

        trend = ""
        if len(recent_prices) >= 2:
            delta = recent_prices[-1] - recent_prices[0]
            direction = "rising" if delta > 10 else "falling" if delta < -10 else "stable"
            trend = f" trend={direction} ({delta:+.1f} GBP/MWh)"

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Grid balancing prices [period {period}]: "
                f"SBP={sbp:.1f} GBP/MWh, SSP={ssp:.1f} GBP/MWh"
                f"{f', spread={spread:.1f}' if spread else ''}"
                f", imbalance={net_imbalance:+.0f} MWh"
                f", offers={offer_vol:.0f} MWh, bids={bid_vol:.0f} MWh"
                f" stress={price_stress}{trend}"
                if ssp is not None and sbp is not None
                else f"Grid balancing prices [period {period}]: data incomplete"
            ),
            data={
                "dataset": "system_prices",
                "settlement_period": period,
                "start_time": start_time,
                "system_sell_price": ssp,
                "system_buy_price": sbp,
                "price_spread": spread,
                "net_imbalance_volume": net_imbalance,
                "accepted_offer_volume": offer_vol,
                "accepted_bid_volume": bid_vol,
                "price_stress": price_stress,
                "recent_prices": recent_prices,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)
        return stored

    async def _process_frequency(self, raw: Any, cell_id: str | None) -> int:
        """Process system frequency — store summary statistics."""
        if not isinstance(raw, dict):
            self.log.warning("Unexpected frequency response: %s", type(raw))
            return 0

        records = raw.get("data", [])
        if not records:
            self.log.info("No frequency records returned")
            return 0

        freqs = []
        for r in records:
            f = _float(r.get("frequency"))
            if f is not None:
                freqs.append(f)

        if not freqs:
            return 0

        freq_min = min(freqs)
        freq_max = max(freqs)
        freq_mean = statistics.mean(freqs)
        freq_stdev = statistics.stdev(freqs) if len(freqs) > 1 else 0.0
        max_deviation = max(abs(freq_min - FREQ_TARGET), abs(freq_max - FREQ_TARGET))
        latest_freq = freqs[-1]
        latest_time = records[-1].get("measurementTime", "")

        # Store key frequency metrics
        stored = 0
        freq_metrics = {
            "grid_frequency_hz": latest_freq,
            "grid_frequency_min_hz": freq_min,
            "grid_frequency_max_hz": freq_max,
            "grid_frequency_stdev_hz": freq_stdev,
            "grid_frequency_max_deviation_hz": max_deviation,
        }

        for metric_name, value in freq_metrics.items():
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": metric_name,
                    "window_minutes": 30,
                    "sample_count": len(freqs),
                    "latest_time": latest_time,
                },
            )
            await self.board.store_observation(obs)
            stored += 1

        # Classify frequency stability
        if max_deviation >= FREQ_ALERT_DEVIATION:
            stability = "alert"
        elif max_deviation >= FREQ_WARN_DEVIATION:
            stability = "notable"
        else:
            stability = "stable"

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Grid frequency (30min window): "
                f"current={latest_freq:.3f}Hz, "
                f"range={freq_min:.3f}-{freq_max:.3f}Hz, "
                f"stdev={freq_stdev:.4f}Hz, "
                f"max_deviation={max_deviation:.3f}Hz, "
                f"stability={stability}"
            ),
            data={
                "dataset": "frequency",
                "latest_frequency": latest_freq,
                "latest_time": latest_time,
                "freq_min": freq_min,
                "freq_max": freq_max,
                "freq_mean": round(freq_mean, 4),
                "freq_stdev": round(freq_stdev, 4),
                "max_deviation": round(max_deviation, 4),
                "stability": stability,
                "sample_count": len(freqs),
            },
            location_id=cell_id,
        )
        await self.board.post(msg)
        return stored


def _float(val: Any) -> float | None:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


async def ingest_bmrs_balancing(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one BMRS balancing fetch cycle."""
    ingestor = BmrsBalancingIngestor(board, graph, scheduler)
    await ingestor.run()
