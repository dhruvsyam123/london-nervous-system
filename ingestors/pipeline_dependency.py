"""Pipeline dependency graph ingestor — maps source→agent dependencies and
predicts downstream impact of source failures.

Addresses Brain suggestion: ingest a real-time dependency graph so the system
can proactively predict which agents/outputs degrade when a source fails.

Complements:
- data_lineage.py (per-source freshness — whether a source is delivering)
- system_health.py (log errors, circuit breakers — whether a source is erroring)

This ingestor adds the *structural* layer: which agents depend on which sources,
and what is the cascade impact when sources go down.

Data sources (all local, no external API):
- Coordinator task registry (registered tasks + source mappings)
- Circuit breaker states (from base.py)
- Data lineage staleness (from recent observations)
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

log = logging.getLogger("london.ingestors.pipeline_dependency")

# Static dependency map: agent → list of source names it consumes.
# Derived from ARCHITECTURE.md data flow and agent implementations.
_AGENT_DEPENDENCIES: dict[str, list[str]] = {
    "vision_interpreter": [
        "tfl_jamcam",
    ],
    "numeric_interpreter": [
        "laqn", "tfl_traffic", "tfl_crowding", "carbon_intensity",
        "environment_agency", "open_meteo", "grid_demand", "grid_generation",
        "river_flow", "cycle_hire", "tfl_bus_congestion", "bus_crowding",
        "tfl_passenger_counts", "waze_traffic", "tomtom_traffic",
        "sensor_health", "data_lineage", "system_health",
    ],
    "text_interpreter": [
        "gdelt", "social_sentiment", "bluesky_transport",
    ],
    "financial_interpreter": [
        "financial_stocks", "financial_crypto", "financial_polymarket",
        "retail_spending",
    ],
    "spatial_connector": [
        # Consumes anomalies from ALL interpreters (indirect dependency)
        "tfl_jamcam", "laqn", "tfl_traffic", "gdelt", "financial_stocks",
        "open_meteo", "environment_agency",
    ],
    "narrative_connector": [
        # Consumes spatial connector output (indirect)
    ],
    "brain": [
        # Consumes from all channels — depends on everything
    ],
}

# Agents ordered by pipeline depth (for cascade computation)
_PIPELINE_DEPTH: dict[str, int] = {
    "vision_interpreter": 1,
    "numeric_interpreter": 1,
    "text_interpreter": 1,
    "financial_interpreter": 1,
    "spatial_connector": 2,
    "narrative_connector": 3,
    "statistical_connector": 2,
    "causal_chain_connector": 2,
    "brain": 4,
}


class PipelineDependencyIngestor(BaseIngestor):
    source_name = "pipeline_dependency"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Gather current state of all pipeline components."""
        # Circuit breaker states
        circuits = get_circuit_states()

        # Source staleness from recent observations
        stale_sources = set()
        try:
            import datetime as dt
            cutoff = dt.datetime.fromtimestamp(
                time.time() - 3600, tz=dt.timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%S")
            _cur = await self.board._db.execute(
                "SELECT source, MAX(timestamp) as last_ts "
                "FROM observations GROUP BY source"
            )
            rows = await _cur.fetchall()
            now = time.time()
            for source, last_ts in rows:
                age = _age_seconds(last_ts, now)
                if age is not None and age > 3600:
                    stale_sources.add(source)
        except Exception as exc:
            self.log.warning("Failed to query source staleness: %s", exc)

        # Coordinator task health
        task_errors: dict[str, int] = {}
        try:
            # Access coordinator through scheduler if available
            if hasattr(self.scheduler, '_tasks'):
                for name, entry in self.scheduler._tasks.items():
                    if entry.consecutive_errors > 0:
                        task_errors[name] = entry.consecutive_errors
        except Exception:
            pass

        return {
            "circuits": circuits,
            "stale_sources": stale_sources,
            "task_errors": task_errors,
        }

    async def process(self, data: Any) -> None:
        circuits = data["circuits"]
        stale_sources: set[str] = data["stale_sources"]
        task_errors: dict[str, int] = data["task_errors"]

        # Identify all impaired sources (circuit open OR stale)
        impaired_sources: dict[str, str] = {}
        for name, cs in circuits.items():
            if cs.is_open:
                impaired_sources[name] = "circuit_open"
        for name in stale_sources:
            if name not in impaired_sources:
                impaired_sources[name] = "stale"

        # Compute per-agent impact scores
        agent_impacts: dict[str, dict] = {}
        for agent, deps in _AGENT_DEPENDENCIES.items():
            if not deps:
                continue
            impaired_deps = [d for d in deps if d in impaired_sources]
            impact_ratio = len(impaired_deps) / len(deps) if deps else 0.0
            agent_impacts[agent] = {
                "total_deps": len(deps),
                "impaired_deps": impaired_deps,
                "impaired_reasons": {
                    d: impaired_sources[d] for d in impaired_deps
                },
                "impact_ratio": round(impact_ratio, 3),
                "depth": _PIPELINE_DEPTH.get(agent, 0),
            }

        # Cascade analysis: which downstream agents are at risk
        at_risk_agents = [
            a for a, info in agent_impacts.items()
            if info["impact_ratio"] > 0.3
        ]

        # Brain impact: fraction of all unique sources that are impaired
        all_sources = set()
        for deps in _AGENT_DEPENDENCIES.values():
            all_sources.update(deps)
        brain_impact = (
            len(impaired_sources.keys() & all_sources) / len(all_sources)
            if all_sources else 0.0
        )

        # Store overall pipeline resilience score (1.0 = fully healthy)
        resilience = 1.0 - brain_impact
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=resilience,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "pipeline_resilience",
                "total_sources_tracked": len(all_sources),
                "impaired_count": len(impaired_sources),
                "impaired_sources": dict(
                    list(impaired_sources.items())[:20]
                ),
                "at_risk_agents": at_risk_agents,
                "brain_impact_ratio": round(brain_impact, 3),
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Pipeline dependency: resilience={resilience:.0%}, "
                f"{len(impaired_sources)} impaired sources, "
                f"{len(at_risk_agents)} agents at risk"
            ),
            data={
                "metric": "pipeline_resilience",
                "resilience": round(resilience, 3),
                "impaired_sources": dict(
                    list(impaired_sources.items())[:20]
                ),
                "at_risk_agents": at_risk_agents,
                "observation_id": obs.id,
            },
        ))

        # Per-agent impact observations (only for impacted agents)
        for agent, info in sorted(
            agent_impacts.items(),
            key=lambda x: -x[1]["impact_ratio"],
        ):
            if info["impact_ratio"] == 0:
                continue
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=info["impact_ratio"],
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "agent_dependency_impact",
                    "agent": agent,
                    "total_deps": info["total_deps"],
                    "impaired_deps": info["impaired_deps"],
                    "impaired_reasons": info["impaired_reasons"],
                    "depth": info["depth"],
                },
            )
            await self.board.store_observation(obs)
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Pipeline impact: {agent} degraded {info['impact_ratio']:.0%} "
                    f"({len(info['impaired_deps'])}/{info['total_deps']} sources impaired: "
                    f"{', '.join(info['impaired_deps'][:5])})"
                ),
                data={
                    "metric": "agent_dependency_impact",
                    "agent": agent,
                    "impact_ratio": info["impact_ratio"],
                    "impaired_deps": info["impaired_deps"],
                    "observation_id": obs.id,
                },
            ))

        self.log.info(
            "Pipeline dependency: resilience=%.1f%% impaired=%d at_risk=%d",
            resilience * 100,
            len(impaired_sources),
            len(at_risk_agents),
        )


def _age_seconds(iso_ts: str, now: float) -> float | None:
    import datetime as dt
    try:
        parsed = dt.datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return now - parsed.timestamp()
    except (ValueError, AttributeError):
        return None


async def ingest_pipeline_dependency(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one pipeline dependency check."""
    ingestor = PipelineDependencyIngestor(board, graph, scheduler)
    await ingestor.run()
