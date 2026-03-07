"""IXP traffic ingestor — aggregate throughput from London internet exchange points.

Fetches public traffic statistics from LONAP (London Network Access Point) via
the IXP Manager Grapher API. Provides real-time throughput data (bits/sec in+out)
that can help diagnose whether data feed issues stem from network infrastructure
problems vs. sensor/software faults.

No API key required — LONAP publishes aggregate stats publicly.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.ixp_traffic")

# LONAP IXP Manager Grapher API — public, no auth required.
# Returns JSON summary with current/avg/max in/out traffic.
LONAP_STATS_URL = "https://portal.lonap.net/grapher/ixp"

# Central London coordinates (approximate LONAP PoP location — Telehouse North, Docklands)
LONAP_LAT = 51.508
LONAP_LON = -0.004


class IxpTrafficIngestor(BaseIngestor):
    source_name = "ixp_traffic"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(
            LONAP_STATS_URL,
            params={"type": "json", "period": "day", "category": "bits"},
        )

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected IXP data type: %s", type(data))
            return

        stats = data.get("statistics") or {}
        if not stats:
            self.log.warning("No statistics in IXP response")
            return

        cell_id = self.graph.latlon_to_cell(LONAP_LAT, LONAP_LON)

        # Extract key metrics
        cur_in = stats.get("cur_in", 0)
        cur_out = stats.get("cur_out", 0)
        avg_in = stats.get("avg_in", 0)
        avg_out = stats.get("avg_out", 0)
        max_in = stats.get("max_in", 0)
        max_out = stats.get("max_out", 0)

        total_cur = cur_in + cur_out
        total_avg = avg_in + avg_out

        # Store aggregate throughput as primary observation
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=total_cur,
            location_id=cell_id,
            lat=LONAP_LAT,
            lon=LONAP_LON,
            metadata={
                "exchange": "LONAP",
                "metric": "throughput_bps",
                "cur_in_bps": cur_in,
                "cur_out_bps": cur_out,
                "avg_in_bps": avg_in,
                "avg_out_bps": avg_out,
                "max_in_bps": max_in,
                "max_out_bps": max_out,
            },
        )
        await self.board.store_observation(obs)

        # Human-readable summary
        cur_gbps = total_cur / 1e9
        avg_gbps = total_avg / 1e9
        max_gbps = (max_in + max_out) / 1e9

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"IXP traffic [LONAP] current={cur_gbps:.1f} Gbps "
                f"avg={avg_gbps:.1f} Gbps max={max_gbps:.1f} Gbps"
            ),
            data={
                "exchange": "LONAP",
                "current_gbps": round(cur_gbps, 2),
                "average_gbps": round(avg_gbps, 2),
                "max_gbps": round(max_gbps, 2),
                "cur_in_bps": cur_in,
                "cur_out_bps": cur_out,
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info(
            "LONAP IXP traffic: current=%.1f Gbps avg=%.1f Gbps max=%.1f Gbps",
            cur_gbps, avg_gbps, max_gbps,
        )


async def ingest_ixp_traffic(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one IXP traffic fetch cycle."""
    ingestor = IxpTrafficIngestor(board, graph, scheduler)
    await ingestor.run()
