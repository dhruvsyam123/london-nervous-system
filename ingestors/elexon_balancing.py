"""Elexon BMRS Balancing Mechanism ingestor — real-time dispatch acceptances.

Fetches BOALF (Bid-Offer Acceptance Level Flagged) data: the System Operator's
real-time instructions to power plants to increase or decrease output. This is
the actual "balancing mechanism" — when the grid needs more gas generation or
less wind, these records show which specific units are dispatched and by how much.

Enriches each acceptance with fuel type from the BM Units reference API (cached).

No API key required. Free public API.

Key value: enables causal analysis between grid balancing actions (e.g. gas plant
ramp-up) and downstream environmental effects (air quality changes near plants).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.elexon_balancing")

# Elexon BMRS v1 — free, no API key
BOALF_URL = "https://data.elexon.co.uk/bmrs/api/v1/balancing/acceptances/all/latest"
BMUNITS_URL = "https://data.elexon.co.uk/bmrs/api/v1/reference/bmunits/all"

# Cache BM unit → fuel type mapping (refreshed every 24h)
_bmunit_cache: dict[str, str] = {}
_bmunit_cache_time: float = 0.0
_CACHE_TTL = 86400  # 24 hours

# Fuel type categories for summary stats
FOSSIL_FUELS = {"CCGT", "COAL", "OCGT", "OIL"}
RENEWABLE_FUELS = {"WIND", "BIOMASS", "NPSHYD"}
STORAGE_FUELS = {"PS"}  # pumped storage

# London coords (grid-level data, not location-specific)
LONDON_LAT = 51.5
LONDON_LON = -0.12


class ElexonBalancingIngestor(BaseIngestor):
    source_name = "elexon_balancing"
    rate_limit_name = "default"

    async def _refresh_bmunit_cache(self) -> None:
        global _bmunit_cache, _bmunit_cache_time
        if _bmunit_cache and (time.time() - _bmunit_cache_time) < _CACHE_TTL:
            return
        data = await self.fetch(BMUNITS_URL, params={"format": "json"})
        if not isinstance(data, list):
            self.log.warning("Unexpected BM units response type: %s", type(data))
            return
        new_cache: dict[str, str] = {}
        for unit in data:
            ng_id = unit.get("nationalGridBmUnit")
            fuel = unit.get("fuelType")
            if ng_id and fuel:
                new_cache[ng_id] = fuel
        if new_cache:
            _bmunit_cache.clear()
            _bmunit_cache.update(new_cache)
            _bmunit_cache_time = time.time()
            self.log.info("BM unit cache refreshed: %d units with fuel types", len(new_cache))

    async def fetch_data(self) -> Any:
        await self._refresh_bmunit_cache()
        return await self.fetch(BOALF_URL, params={"format": "json"})

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected BOALF response type: %s", type(data))
            return

        records = data.get("data", [])
        if not records:
            self.log.info("No BOALF records returned (quiet grid period)")
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        processed = 0
        fossil_ramp_up_mw = 0.0
        fossil_ramp_down_mw = 0.0
        renewable_ramp_mw = 0.0
        total_actions = 0
        fuel_actions: dict[str, int] = {}

        # Deduplicate by (bmUnit, acceptanceNumber) — API may return
        # multiple level steps per acceptance
        seen_acceptances: set[tuple[str, int]] = set()

        for record in records:
            bm_unit = record.get("nationalGridBmUnit") or record.get("bmUnit", "")
            acc_num = record.get("acceptanceNumber", 0)
            level_from = record.get("levelFrom")
            level_to = record.get("levelTo")
            acc_time = record.get("acceptanceTime", "")

            if level_from is None or level_to is None:
                continue

            try:
                mw_from = float(level_from)
                mw_to = float(level_to)
            except (ValueError, TypeError):
                continue

            delta_mw = mw_to - mw_from
            if abs(delta_mw) < 1.0:
                continue  # skip trivial changes

            # Get fuel type from cache
            fuel_type = _bmunit_cache.get(bm_unit, "UNKNOWN")

            # Track unique acceptances for summary
            acc_key = (bm_unit, acc_num)
            if acc_key not in seen_acceptances:
                seen_acceptances.add(acc_key)
                total_actions += 1
                fuel_actions[fuel_type] = fuel_actions.get(fuel_type, 0) + 1

                # Categorise ramp direction
                if fuel_type in FOSSIL_FUELS:
                    if delta_mw > 0:
                        fossil_ramp_up_mw += delta_mw
                    else:
                        fossil_ramp_down_mw += abs(delta_mw)
                elif fuel_type in RENEWABLE_FUELS:
                    renewable_ramp_mw += delta_mw

            # Store per-acceptance observation
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=delta_mw,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "balancing_dispatch_mw",
                    "bm_unit": bm_unit,
                    "fuel_type": fuel_type,
                    "level_from_mw": mw_from,
                    "level_to_mw": mw_to,
                    "acceptance_number": acc_num,
                    "acceptance_time": acc_time,
                    "direction": "up" if delta_mw > 0 else "down",
                },
            )
            await self.board.store_observation(obs)
            processed += 1

        # Summary observation for fossil ramp-up (key environmental signal)
        if fossil_ramp_up_mw > 0:
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=fossil_ramp_up_mw,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={"metric": "fossil_ramp_up_total_mw"},
            )
            await self.board.store_observation(obs)

        # Build summary
        fuel_summary = ", ".join(
            f"{ft}={n}" for ft, n in sorted(fuel_actions.items(), key=lambda x: -x[1])
            if n > 0
        )

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Grid balancing: {total_actions} dispatch actions. "
                f"Fossil ramp-up={fossil_ramp_up_mw:.0f}MW, "
                f"ramp-down={fossil_ramp_down_mw:.0f}MW. "
                f"Actions by fuel: {fuel_summary}"
            ),
            data={
                "total_actions": total_actions,
                "fossil_ramp_up_mw": fossil_ramp_up_mw,
                "fossil_ramp_down_mw": fossil_ramp_down_mw,
                "renewable_ramp_mw": renewable_ramp_mw,
                "actions_by_fuel": fuel_actions,
                "acceptance_count": len(seen_acceptances),
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info(
            "Elexon balancing: %d actions, fossil ramp-up=%.0fMW, ramp-down=%.0fMW, %d records",
            total_actions, fossil_ramp_up_mw, fossil_ramp_down_mw, processed,
        )


async def ingest_elexon_balancing(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Elexon balancing fetch cycle."""
    ingestor = ElexonBalancingIngestor(board, graph, scheduler)
    await ingestor.run()
