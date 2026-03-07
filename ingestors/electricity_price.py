"""Wholesale electricity price ingestor — Elexon BMRS market index prices.

Fetches half-hourly GB wholesale electricity spot prices (£/MWh) from the
Elexon Insights Solution API (free, no API key required).

This enables the system to disentangle whether demand responses correlate with
low carbon intensity (environmental signal) or low wholesale price (economic
signal) — the key question raised by the Brain's 6-hour lag hypothesis.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.electricity_price")

# Elexon BMRS Insights Solution — Market Index Prices (no key needed)
MARKET_INDEX_URL = (
    "https://data.elexon.co.uk/bmrs/api/v1/balancing/pricing/market-index"
)

LONDON_LAT = 51.5
LONDON_LON = -0.12


class ElectricityPriceIngestor(BaseIngestor):
    source_name = "electricity_price"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        now = datetime.now(timezone.utc)
        from_time = now - timedelta(hours=2)
        params = {
            "from": from_time.strftime("%Y-%m-%dT%H:%MZ"),
            "to": now.strftime("%Y-%m-%dT%H:%MZ"),
        }
        return await self.fetch(MARKET_INDEX_URL, params=params)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected response type: %s", type(data))
            return

        entries = data.get("data", [])
        if not isinstance(entries, list):
            self.log.warning("No data array in response")
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        processed = 0
        latest_price = None
        latest_period = None

        for entry in entries:
            provider = entry.get("dataProvider", "")
            # Use APXMIDP — the provider with actual non-zero prices
            if "APX" not in provider:
                continue

            try:
                price = float(entry.get("price", 0))
            except (ValueError, TypeError):
                continue

            if price <= 0:
                continue

            settlement_period = entry.get("settlementPeriod")
            settlement_date = entry.get("settlementDate", "")
            start_time = entry.get("startTime", "")

            try:
                volume = float(entry.get("volume", 0))
            except (ValueError, TypeError):
                volume = 0.0

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=price,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "wholesale_price_gbp_mwh",
                    "provider": provider,
                    "settlement_period": settlement_period,
                    "settlement_date": settlement_date,
                    "start_time": start_time,
                    "volume_mwh": volume,
                },
            )
            await self.board.store_observation(obs)
            processed += 1

            if latest_period is None or (
                settlement_period is not None and settlement_period > latest_period
            ):
                latest_period = settlement_period
                latest_price = price

        if latest_price is not None:
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Wholesale electricity price: £{latest_price:.2f}/MWh "
                    f"(settlement period {latest_period})"
                ),
                data={
                    "price_gbp_mwh": latest_price,
                    "settlement_period": latest_period,
                    "total_periods": processed,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)

        self.log.info(
            "Electricity price: processed=%d periods, latest=£%.2f/MWh",
            processed,
            latest_price or 0,
        )


async def ingest_electricity_price(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one electricity price fetch cycle."""
    ingestor = ElectricityPriceIngestor(board, graph, scheduler)
    await ingestor.run()
