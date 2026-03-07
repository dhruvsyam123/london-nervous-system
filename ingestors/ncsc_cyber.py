"""NCSC cyber threat advisory ingestor — UK National Cyber Security Centre RSS feed.

Ingests threat reports, alerts, and advisories from the NCSC public RSS feed.
This enables cross-referencing infrastructure anomalies with known cyber attack
patterns, vulnerabilities, and sector-specific alerts.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.ncsc_cyber")

NCSC_RSS_URL = "https://www.ncsc.gov.uk/api/1/services/v1/all-rss-feed.xml"

# Keywords that indicate higher severity / infrastructure relevance
_INFRA_KEYWORDS = frozenset({
    "critical", "exploit", "vulnerability", "ransomware", "attack",
    "infrastructure", "energy", "transport", "water", "nhs", "health",
    "telecoms", "financial", "government", "supply chain", "zero-day",
    "actively exploited", "emergency", "incident",
})


def _score_relevance(title: str, description: str) -> float:
    """Score 0-1 how relevant an advisory is to London infrastructure."""
    text = f"{title} {description}".lower()
    hits = sum(1 for kw in _INFRA_KEYWORDS if kw in text)
    return min(hits / 3.0, 1.0)


class NcscCyberIngestor(BaseIngestor):
    source_name = "ncsc_cyber"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(NCSC_RSS_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, str):
            self.log.warning("Expected XML string, got %s", type(data))
            return

        try:
            root = ET.fromstring(data)
        except ET.ParseError as exc:
            self.log.error("Failed to parse NCSC RSS XML: %s", exc)
            return

        channel = root.find("channel")
        if channel is None:
            self.log.warning("No <channel> element in NCSC RSS")
            return

        items = channel.findall("item")
        processed = 0

        for item in items:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = (item.findtext("description") or "").strip()
            pub_date_str = (item.findtext("pubDate") or "").strip()
            guid = (item.findtext("guid") or link).strip()

            if not title:
                continue

            # Parse publication date
            pub_date = None
            if pub_date_str:
                try:
                    pub_date = parsedate_to_datetime(pub_date_str)
                except (ValueError, TypeError):
                    pass

            relevance = _score_relevance(title, description)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=title,
                metadata={
                    "link": link,
                    "description": description[:500],
                    "guid": guid,
                    "pub_date": pub_date.isoformat() if pub_date else None,
                    "relevance_score": round(relevance, 2),
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"NCSC advisory: {title}"
                    + (f" [relevance={relevance:.1f}]" if relevance > 0 else "")
                ),
                data={
                    "title": title,
                    "link": link,
                    "description": description[:500],
                    "pub_date": pub_date.isoformat() if pub_date else None,
                    "relevance_score": round(relevance, 2),
                    "observation_id": obs.id,
                },
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("NCSC cyber advisories: processed=%d items", processed)


async def ingest_ncsc_cyber(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one NCSC RSS fetch cycle."""
    ingestor = NcscCyberIngestor(board, graph, scheduler)
    await ingestor.run()
