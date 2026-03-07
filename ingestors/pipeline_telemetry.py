"""Pipeline telemetry ingestor — monitors the anomaly promotion pipeline.

Addresses the Brain's suggestion: ingest our own processing logs to diagnose
why valid raw detections are being discarded before analysis.

Tracks the full funnel:
  #raw → #anomalies → #hypotheses → #discoveries

Metrics produced:
- promotion_rate: fraction of raw messages that become anomalies
- suppression_rate: fraction of anomalies filtered by Brain grounding
- per_source_promotion: per-source raw→anomaly conversion rates
- funnel_throughput: message counts at each pipeline stage

Data sources: board SQLite DB (local, no external API).
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.pipeline_telemetry")

# Analysis window (seconds)
_WINDOW_SECS = 900  # 15 min

# Regex to extract suppression counts from Brain log messages
# e.g. "[Brain] Filtered out 12/30 anomalies as routine ..."
_FILTER_RE = re.compile(
    r"\[Brain\] Filtered out (\d+)/(\d+) anomalies as routine"
)
# e.g. "[Brain] Suppressed cluster: ... (mundane=..., base_rate=0.45)"
_SUPPRESS_RE = re.compile(
    r"\[Brain\] Suppressed cluster: (.+?) \(mundane=(.+?), base_rate=([\d.]+)\)"
)


class PipelineTelemetryIngestor(BaseIngestor):
    source_name = "pipeline_telemetry"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Query board DB for pipeline stage counts and Brain filtering logs."""
        import datetime as dt

        cutoff = dt.datetime.fromtimestamp(
            time.time() - _WINDOW_SECS, tz=dt.timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S")

        try:
            db = self.board._db

            # Count messages per channel in window
            cur = await db.execute(
                "SELECT channel, COUNT(*) FROM messages "
                "WHERE timestamp > ? GROUP BY channel",
                (cutoff,),
            )
            channel_counts = {r[0]: r[1] for r in await cur.fetchall()}

            # Per-source raw message counts (from_agent field)
            cur = await db.execute(
                "SELECT from_agent, COUNT(*) FROM messages "
                "WHERE channel = '#raw' AND timestamp > ? "
                "GROUP BY from_agent",
                (cutoff,),
            )
            raw_by_source = {r[0]: r[1] for r in await cur.fetchall()}

            # Per-source anomaly counts
            cur = await db.execute(
                "SELECT source, COUNT(*) FROM anomalies "
                "WHERE timestamp > ? GROUP BY source",
                (cutoff,),
            )
            anomalies_by_source = {r[0]: r[1] for r in await cur.fetchall()}

            # Brain messages mentioning filtering/suppression
            cur = await db.execute(
                "SELECT content FROM messages "
                "WHERE channel IN ('#meta', '#discoveries') "
                "AND from_agent = 'brain' AND timestamp > ?",
                (cutoff,),
            )
            brain_msgs = [r[0] for r in await cur.fetchall()]

        except Exception as exc:
            self.log.warning("Failed to query pipeline telemetry: %s", exc)
            return None

        return {
            "channel_counts": channel_counts,
            "raw_by_source": raw_by_source,
            "anomalies_by_source": anomalies_by_source,
            "brain_msgs": brain_msgs,
        }

    async def process(self, data: Any) -> None:
        if data is None:
            return

        cc = data["channel_counts"]
        raw_by_source = data["raw_by_source"]
        anomalies_by_source = data["anomalies_by_source"]
        brain_msgs = data["brain_msgs"]

        obs_count = 0

        # ── Funnel throughput ──────────────────────────────────────────────
        raw_count = cc.get("#raw", 0)
        anomaly_count = cc.get("#anomalies", 0)
        hypothesis_count = cc.get("#hypotheses", 0)
        discovery_count = cc.get("#discoveries", 0)

        promotion_rate = anomaly_count / raw_count if raw_count > 0 else 0.0
        hypothesis_rate = (
            hypothesis_count / anomaly_count if anomaly_count > 0 else 0.0
        )

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=promotion_rate,
            metadata={
                "metric": "promotion_rate",
                "raw_count": raw_count,
                "anomaly_count": anomaly_count,
                "hypothesis_count": hypothesis_count,
                "discovery_count": discovery_count,
                "hypothesis_rate": round(hypothesis_rate, 4),
                "window_secs": _WINDOW_SECS,
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Pipeline funnel: {raw_count} raw → {anomaly_count} anomalies "
                f"({promotion_rate:.1%}) → {hypothesis_count} hypotheses → "
                f"{discovery_count} discoveries"
            ),
            data={
                "metric": "promotion_rate",
                "promotion_rate": round(promotion_rate, 4),
                "raw_count": raw_count,
                "anomaly_count": anomaly_count,
                "hypothesis_count": hypothesis_count,
                "discovery_count": discovery_count,
                "observation_id": obs.id,
            },
        ))
        obs_count += 1

        # ── Brain suppression metrics ──────────────────────────────────────
        total_filtered = 0
        total_presented = 0
        suppressed_clusters: list[dict] = []

        for msg in brain_msgs:
            m = _FILTER_RE.search(msg)
            if m:
                total_filtered += int(m.group(1))
                total_presented += int(m.group(2))
            m = _SUPPRESS_RE.search(msg)
            if m:
                suppressed_clusters.append({
                    "desc": m.group(1)[:80],
                    "mundane": m.group(2),
                    "base_rate": float(m.group(3)),
                })

        suppression_rate = (
            total_filtered / total_presented if total_presented > 0 else 0.0
        )

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=suppression_rate,
            metadata={
                "metric": "suppression_rate",
                "filtered": total_filtered,
                "presented": total_presented,
                "suppressed_clusters_count": len(suppressed_clusters),
                "top_suppressed": suppressed_clusters[:5],
                "window_secs": _WINDOW_SECS,
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Pipeline suppression: {total_filtered}/{total_presented} "
                f"anomalies filtered by Brain ({suppression_rate:.0%}), "
                f"{len(suppressed_clusters)} clusters suppressed"
            ),
            data={
                "metric": "suppression_rate",
                "suppression_rate": round(suppression_rate, 4),
                "filtered": total_filtered,
                "presented": total_presented,
                "suppressed_clusters": len(suppressed_clusters),
                "observation_id": obs.id,
            },
        ))
        obs_count += 1

        # ── Per-source promotion rates ─────────────────────────────────────
        # Compare raw message counts per source against anomaly counts
        all_sources = set(raw_by_source.keys()) | set(anomalies_by_source.keys())
        source_promotions: list[dict] = []
        silent_sources: list[str] = []  # sources with raw data but zero anomalies

        for src in sorted(all_sources):
            raw_n = raw_by_source.get(src, 0)
            anom_n = anomalies_by_source.get(src, 0)
            rate = anom_n / raw_n if raw_n > 0 else 0.0
            source_promotions.append({
                "source": src,
                "raw": raw_n,
                "anomalies": anom_n,
                "rate": round(rate, 4),
            })
            if raw_n >= 3 and anom_n == 0:
                silent_sources.append(src)

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=float(len(silent_sources)),
            metadata={
                "metric": "silent_sources",
                "silent_sources": silent_sources,
                "source_promotions": source_promotions[:20],
                "total_sources_active": len(all_sources),
                "window_secs": _WINDOW_SECS,
            },
        )
        await self.board.store_observation(obs)

        if silent_sources:
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Pipeline silent sources: {len(silent_sources)} sources "
                    f"produced raw data but zero anomalies: "
                    + ", ".join(silent_sources[:10])
                ),
                data={
                    "metric": "silent_sources",
                    "count": len(silent_sources),
                    "sources": silent_sources,
                    "observation_id": obs.id,
                },
            ))
        obs_count += 1

        self.log.info(
            "Pipeline telemetry: %d obs (raw=%d anom=%d promo=%.1f%% "
            "suppressed=%d/%d silent_sources=%d)",
            obs_count, raw_count, anomaly_count,
            promotion_rate * 100, total_filtered, total_presented,
            len(silent_sources),
        )


async def ingest_pipeline_telemetry(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one pipeline telemetry cycle."""
    ingestor = PipelineTelemetryIngestor(board, graph, scheduler)
    await ingestor.run()
