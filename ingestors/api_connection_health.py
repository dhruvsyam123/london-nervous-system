"""API connection health ingestor — comprehensive outbound connection monitoring.

Implements the Brain's suggestion to make our external API connection health
observable. Since this system has no API gateway/proxy, we read the circuit
breaker registry (which tracks every outbound connection's failure state) and
emit per-source + aggregate health observations as time-series data.

Existing coverage gap: system_health.py only reports OPEN circuits,
provider_status.py only probes 4 of 60+ APIs. This ingestor gives the
statistical connector and brain a complete, continuous view of ALL sources'
connection health — enabling correlation of data gaps with connection failures.

Data sources (all local, no external API):
- Circuit breaker registry: per-source failures, trips, cooldown state
- Aggregate metrics: % healthy, failure distribution, recently tripped
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor, get_circuit_states

log = logging.getLogger("london.ingestors.api_connection_health")


class APIConnectionHealthIngestor(BaseIngestor):
    source_name = "api_connection_health"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Snapshot all circuit breaker states."""
        circuits = get_circuit_states()
        now = time.monotonic()
        sources = {}
        for name, cs in circuits.items():
            remaining_cooldown = 0.0
            if cs.is_open:
                elapsed = now - cs.open_since
                remaining_cooldown = max(0.0, cs.cooldown - elapsed)
            sources[name] = {
                "failures": cs.failures,
                "is_open": cs.is_open,
                "total_trips": cs.total_trips,
                "cooldown_s": cs.cooldown,
                "remaining_cooldown_s": round(remaining_cooldown, 1),
            }
        return sources

    async def process(self, data: Any) -> None:
        sources: dict[str, dict] = data
        if not sources:
            self.log.info("No circuit breaker data yet — skipping")
            return

        total = len(sources)
        open_count = sum(1 for s in sources.values() if s["is_open"])
        healthy_count = total - open_count
        degraded_count = sum(
            1 for s in sources.values()
            if not s["is_open"] and s["failures"] > 0
        )
        total_trips = sum(s["total_trips"] for s in sources.values())
        total_failures = sum(s["failures"] for s in sources.values())
        health_pct = healthy_count / total if total else 0.0

        # ── Aggregate connection health ────────────────────────────────────
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=health_pct,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "api_health_pct",
                "total_sources": total,
                "healthy": healthy_count,
                "degraded": degraded_count,
                "open": open_count,
                "total_active_failures": total_failures,
                "total_lifetime_trips": total_trips,
            },
        )
        await self.board.store_observation(obs)

        open_names = [n for n, s in sources.items() if s["is_open"]]
        degraded_names = [
            n for n, s in sources.items()
            if not s["is_open"] and s["failures"] > 0
        ]

        summary = (
            f"API connection health: {healthy_count}/{total} healthy"
            f" ({open_count} open circuits, {degraded_count} degraded)"
        )
        if open_names:
            summary += f" | DOWN: {', '.join(sorted(open_names))}"

        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=summary,
            data={
                "metric": "api_health_pct",
                "health_pct": round(health_pct, 4),
                "healthy": healthy_count,
                "degraded": degraded_count,
                "open": open_count,
                "open_sources": sorted(open_names),
                "degraded_sources": sorted(degraded_names),
                "observation_id": obs.id,
            },
        ))

        # ── Per-source observations (only for non-pristine sources) ────────
        reported = 0
        for name, state in sorted(sources.items()):
            if state["failures"] == 0 and state["total_trips"] == 0:
                continue  # skip perfectly healthy sources to reduce noise

            health_val = 0.0 if state["is_open"] else (
                0.5 if state["failures"] > 0 else 1.0
            )
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=health_val,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "source_connection_health",
                    "monitored_source": name,
                    "failures": state["failures"],
                    "is_open": state["is_open"],
                    "total_trips": state["total_trips"],
                    "remaining_cooldown_s": state["remaining_cooldown_s"],
                },
            )
            await self.board.store_observation(obs)

            status = "OPEN" if state["is_open"] else (
                f"degraded ({state['failures']} failures)" if state["failures"] > 0
                else "healthy"
            )
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"API connection [{name}]: {status}"
                    f" (lifetime trips: {state['total_trips']})"
                ),
                data={
                    "metric": "source_connection_health",
                    "monitored_source": name,
                    "health": health_val,
                    "failures": state["failures"],
                    "is_open": state["is_open"],
                    "total_trips": state["total_trips"],
                    "observation_id": obs.id,
                },
            ))
            reported += 1

        self.log.info(
            "API connection health: %d/%d healthy, %d open, %d degraded "
            "(%d detailed reports)",
            healthy_count, total, open_count, degraded_count, reported,
        )


async def ingest_api_connection_health(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one API connection health cycle."""
    ingestor = APIConnectionHealthIngestor(board, graph, scheduler)
    await ingestor.run()
