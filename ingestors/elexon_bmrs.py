"""Elexon BMRS ingestor — wholesale prices, generation outturn, and imbalance forecasts.

Uses the free Elexon Insights API (no API key required).
Endpoints:
  - /datasets/MID         — Market Index Data (wholesale electricity prices)
  - /generation/outturn/summary — Actual generation by fuel type (MW)
  - /datasets/IMBALNGC    — Day-ahead indicated imbalance (MWh)

This data complements the existing carbon_intensity ingestor by providing
the economic/operational layer: what the grid is actually generating,
at what price, and whether supply-demand is balanced.
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

log = logging.getLogger("london.ingestors.elexon_bmrs")

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"

# Central London coordinates for grid-cell lookup
LONDON_LAT = 51.5
LONDON_LON = -0.12


class ElexonBmrsIngestor(BaseIngestor):
    source_name = "elexon_bmrs"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        now = datetime.now(timezone.utc)
        one_hour_ago = now.replace(minute=0, second=0, microsecond=0)
        from_str = one_hour_ago.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        mid_data = await self.fetch(
            f"{BASE_URL}/datasets/MID",
            params={"from": from_str, "to": to_str, "format": "json"},
        )
        gen_data = await self.fetch(
            f"{BASE_URL}/generation/outturn/summary",
            params={"from": from_str, "to": to_str, "format": "json"},
        )
        imbalance_data = await self.fetch(
            f"{BASE_URL}/datasets/IMBALNGC",
            params={"from": from_str, "to": to_str, "format": "json"},
        )
        if mid_data is None and gen_data is None and imbalance_data is None:
            return None
        return {
            "mid": mid_data,
            "generation": gen_data,
            "imbalance": imbalance_data,
        }

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        counts = {"prices": 0, "generation": 0, "imbalance": 0}

        # ── Market Index Data (wholesale prices) ───────────────────────
        mid = data.get("mid")
        if isinstance(mid, dict):
            entries = mid.get("data", [])
            latest_price = None
            latest_volume = None
            latest_provider = None
            for entry in entries:
                price = entry.get("price")
                volume = entry.get("volume")
                provider = entry.get("dataProvider", "")
                if price and float(price) > 0:
                    latest_price = float(price)
                    latest_volume = float(volume) if volume else None
                    latest_provider = provider

            if latest_price is not None:
                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=latest_price,
                    location_id=cell_id,
                    lat=LONDON_LAT,
                    lon=LONDON_LON,
                    metadata={
                        "metric": "wholesale_electricity_price_gbp_mwh",
                        "volume_mwh": latest_volume,
                        "provider": latest_provider,
                    },
                )
                await self.board.store_observation(obs)
                counts["prices"] += 1

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"BMRS wholesale price: £{latest_price:.2f}/MWh"
                        f" (vol={latest_volume:.0f} MWh)"
                        if latest_volume
                        else f"BMRS wholesale price: £{latest_price:.2f}/MWh"
                    ),
                    data={
                        "price_gbp_mwh": latest_price,
                        "volume_mwh": latest_volume,
                        "provider": latest_provider,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)

        # ── Generation outturn by fuel type ────────────────────────────
        gen = data.get("generation")
        if isinstance(gen, (dict, list)):
            gen_entries = gen if isinstance(gen, list) else gen.get("data", [])
            # Take the most recent settlement period
            if gen_entries:
                latest_period = gen_entries[-1] if isinstance(gen_entries[-1], dict) else {}
                fuel_data = latest_period.get("data", [])
                total_mw = 0
                fuel_summary = {}
                for fuel_entry in fuel_data:
                    fuel_type = fuel_entry.get("fuelType", "")
                    generation_mw = fuel_entry.get("generation")
                    if fuel_type and generation_mw is not None:
                        gen_val = float(generation_mw)
                        fuel_summary[fuel_type] = gen_val
                        total_mw += gen_val

                        obs = Observation(
                            source=self.source_name,
                            obs_type=ObservationType.NUMERIC,
                            value=gen_val,
                            location_id=cell_id,
                            lat=LONDON_LAT,
                            lon=LONDON_LON,
                            metadata={
                                "metric": "generation_outturn_mw",
                                "fuel_type": fuel_type,
                            },
                        )
                        await self.board.store_observation(obs)
                        counts["generation"] += 1

                if fuel_summary:
                    # Summarise top fuels for the board message
                    top_fuels = sorted(
                        fuel_summary.items(), key=lambda x: abs(x[1]), reverse=True
                    )[:5]
                    summary_str = ", ".join(
                        f"{f}={v:.0f}MW" for f, v in top_fuels
                    )
                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=f"BMRS generation outturn: total={total_mw:.0f}MW — {summary_str}",
                        data={
                            "total_mw": total_mw,
                            "fuel_mix": fuel_summary,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)

        # ── Imbalance forecast ─────────────────────────────────────────
        imb = data.get("imbalance")
        if isinstance(imb, dict):
            imb_entries = imb.get("data", [])
            if imb_entries:
                # Aggregate the latest imbalance across boundaries
                latest_sp = max(
                    (e for e in imb_entries if e.get("imbalance") is not None),
                    key=lambda e: e.get("settlementPeriod", 0),
                    default=None,
                )
                if latest_sp:
                    imb_val = float(latest_sp["imbalance"])
                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=imb_val,
                        location_id=cell_id,
                        lat=LONDON_LAT,
                        lon=LONDON_LON,
                        metadata={
                            "metric": "indicated_imbalance_mwh",
                            "settlement_period": latest_sp.get("settlementPeriod"),
                            "boundary": latest_sp.get("boundary", ""),
                        },
                    )
                    await self.board.store_observation(obs)
                    counts["imbalance"] += 1

                    direction = "surplus" if imb_val > 0 else "deficit"
                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=(
                            f"BMRS indicated imbalance: {imb_val:+.0f} MWh ({direction})"
                            f" SP{latest_sp.get('settlementPeriod', '?')}"
                        ),
                        data={
                            "imbalance_mwh": imb_val,
                            "settlement_period": latest_sp.get("settlementPeriod"),
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)

        self.log.info(
            "Elexon BMRS: prices=%d generation=%d imbalance=%d",
            counts["prices"],
            counts["generation"],
            counts["imbalance"],
        )


async def ingest_elexon_bmrs(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Elexon BMRS fetch cycle."""
    ingestor = ElexonBmrsIngestor(board, graph, scheduler)
    await ingestor.run()
