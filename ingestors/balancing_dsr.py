"""Elexon Balancing Mechanism — DSR & battery storage visibility.

Closes the visibility gap on 'UNKNOWN' / 'OTHER' participants in Elexon
balancing data by tracking:

1. The BMU registry for battery storage (BESS) and DSR aggregator units —
   fleet composition, new registrations, lead parties.
2. The 'OTHER' and 'PS' (pumped storage) fuel-type generation from the
   outturn summary — quantifying how much hidden flexibility is active on
   the grid at any given moment.

Together these let the system correlate battery/DSR dispatch with grid stress,
renewable intermittency, and price spikes.

Free Elexon BMRS API — no API key required.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.balancing_dsr")

# Elexon BMRS v1 endpoints (free, no key)
BMU_REGISTRY_URL = "https://data.elexon.co.uk/bmrs/api/v1/reference/bmunits/all"
OUTTURN_SUMMARY_URL = "https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/summary"

# Keywords to identify battery/storage BMUs
_BATTERY_KEYWORDS = {"battery", "bess", "storage", "energy store"}
# Keywords to identify DSR / aggregator BMUs
_DSR_KEYWORDS = {"dsr", "demand side", "aggregat", "demand response", "flexibil"}
# BMU type codes: V = virtual/aggregated, S = secondary/supplier
_FLEX_BMU_TYPES = {"V", "S"}

# National-level data, pinned to London for board posting
LONDON_LAT = 51.5
LONDON_LON = -0.12


def _classify_bmu(bmu: dict) -> str | None:
    """Classify a BMU as 'battery', 'dsr', or None."""
    searchable = " ".join([
        bmu.get("bmUnitName", ""),
        bmu.get("leadPartyName", ""),
        bmu.get("nationalGridBmUnit", ""),
    ]).lower()
    if any(kw in searchable for kw in _BATTERY_KEYWORDS):
        return "battery"
    if any(kw in searchable for kw in _DSR_KEYWORDS):
        return "dsr"
    # V-type BMUs with no clear name are likely aggregated flex assets
    if bmu.get("bmUnitType") == "V":
        return "battery"  # conservative: most V-type are BESS
    return None


class BalancingDsrIngestor(BaseIngestor):
    source_name = "balancing_dsr"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        bmu_data = await self.fetch(BMU_REGISTRY_URL, params={"format": "json"})
        outturn_data = await self.fetch(OUTTURN_SUMMARY_URL, params={"format": "json"})
        return {"bmu": bmu_data, "outturn": outturn_data}

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        bmu_data = data.get("bmu")
        outturn_data = data.get("outturn")

        # --- 1. BMU fleet analysis ---
        battery_count = 0
        dsr_count = 0
        battery_parties: dict[str, int] = {}
        dsr_parties: dict[str, int] = {}

        if bmu_data:
            bmu_list = bmu_data if isinstance(bmu_data, list) else bmu_data.get("data", [])
            for bmu in bmu_list:
                classification = _classify_bmu(bmu)
                party = bmu.get("leadPartyName", "unknown")
                if classification == "battery":
                    battery_count += 1
                    battery_parties[party] = battery_parties.get(party, 0) + 1
                elif classification == "dsr":
                    dsr_count += 1
                    dsr_parties[party] = dsr_parties.get(party, 0) + 1

            # Store fleet composition observations
            obs_battery = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(battery_count),
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "battery_bmu_count",
                    "top_parties": dict(sorted(battery_parties.items(), key=lambda x: -x[1])[:5]),
                },
            )
            await self.board.store_observation(obs_battery)

            obs_dsr = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(dsr_count),
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "dsr_bmu_count",
                    "top_parties": dict(sorted(dsr_parties.items(), key=lambda x: -x[1])[:5]),
                },
            )
            await self.board.store_observation(obs_dsr)

        # --- 2. Flex generation outturn (OTHER + PS) ---
        other_mw = 0.0
        ps_mw = 0.0
        total_mw = 0.0
        snapshot_time = ""

        if outturn_data and isinstance(outturn_data, list) and outturn_data:
            # Take the latest settlement period
            latest = outturn_data[-1]
            snapshot_time = latest.get("startTime", "")
            for fuel in latest.get("data", []):
                ft = fuel.get("fuelType", "")
                gen = fuel.get("generation", 0) or 0
                total_mw += gen
                if ft == "OTHER":
                    other_mw = float(gen)
                elif ft == "PS":
                    ps_mw = float(gen)

            flex_mw = other_mw + ps_mw

            obs_flex = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=flex_mw,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "flex_generation_mw",
                    "other_mw": other_mw,
                    "ps_mw": ps_mw,
                    "snapshot_time": snapshot_time,
                },
            )
            await self.board.store_observation(obs_flex)

            # OTHER as percentage of total — useful for spotting flex dispatch events
            flex_pct = (flex_mw / total_mw * 100) if total_mw > 0 else 0

            obs_pct = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=round(flex_pct, 2),
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "flex_generation_pct",
                    "snapshot_time": snapshot_time,
                },
            )
            await self.board.store_observation(obs_pct)

        # --- Summary message ---
        top_battery = sorted(battery_parties.items(), key=lambda x: -x[1])[:3]
        top_battery_str = ", ".join(f"{p}({n})" for p, n in top_battery) if top_battery else "none"

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Balancing DSR/Battery [{snapshot_time}]: "
                f"fleet={battery_count} battery + {dsr_count} DSR BMUs. "
                f"Flex generation: OTHER={other_mw:.0f}MW + PS={ps_mw:.0f}MW = "
                f"{other_mw + ps_mw:.0f}MW ({(other_mw + ps_mw) / total_mw * 100:.1f}% of total). "
                f"Top battery operators: {top_battery_str}"
            ) if total_mw > 0 else (
                f"Balancing DSR/Battery: fleet={battery_count} battery + {dsr_count} DSR BMUs. "
                f"Top battery operators: {top_battery_str}"
            ),
            data={
                "battery_bmu_count": battery_count,
                "dsr_bmu_count": dsr_count,
                "flex_mw": other_mw + ps_mw,
                "other_mw": other_mw,
                "ps_mw": ps_mw,
                "total_generation_mw": total_mw,
                "snapshot_time": snapshot_time,
                "top_battery_parties": dict(top_battery),
                "top_dsr_parties": dict(sorted(dsr_parties.items(), key=lambda x: -x[1])[:3]),
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info(
            "Balancing DSR: %d battery + %d DSR BMUs, flex=%0.fMW (OTHER=%.0f + PS=%.0f)",
            battery_count, dsr_count, other_mw + ps_mw, other_mw, ps_mw,
        )


async def ingest_balancing_dsr(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one balancing DSR fetch cycle."""
    ingestor = BalancingDsrIngestor(board, graph, scheduler)
    await ingestor.run()
