"""Transport union industrial action ingestor — RMT and ASLEF RSS feeds.

Scrapes press releases from RMT and ASLEF union RSS feeds to detect
upcoming strikes and industrial action announcements. Provides lead
indicators for staff-related service disruptions on London transport.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.union_industrial_action")

RSS_FEEDS = {
    "rmt": "https://www.rmt.org.uk/news/feed.xml",
    "aslef": "https://aslef.org.uk/rss.xml",
}

# Keywords that signal transport-relevant industrial action
_ACTION_KEYWORDS = re.compile(
    r"strike|industrial action|walkout|overtime ban|work.to.rule|ballot|dispute|picket",
    re.IGNORECASE,
)

# London transport keywords for relevance filtering
_LONDON_TRANSPORT = re.compile(
    r"london|tfl|tube|underground|overground|elizabeth line|dlr|crossrail|"
    r"south.?western|south.?eastern|thameslink|great.?northern|"
    r"c2c|greater.?anglia|chiltern|avanti|lumo|national rail|"
    r"bus|transport for london|windrush|metropolitan|district|"
    r"central line|northern line|jubilee|piccadilly|victoria line|"
    r"bakerloo|hammersmith|circle line",
    re.IGNORECASE,
)


class UnionIndustrialActionIngestor(BaseIngestor):
    source_name = "union_industrial_action"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Fetch RSS feeds from RMT and ASLEF."""
        results = {}
        for union, url in RSS_FEEDS.items():
            xml_text = await self.fetch(url)
            if xml_text and isinstance(xml_text, str):
                results[union] = xml_text
            else:
                self.log.warning("No data from %s RSS feed", union)
        return results if results else None

    async def process(self, data: Any) -> None:
        processed = 0
        skipped = 0

        for union, xml_text in data.items():
            try:
                items = self._parse_rss(xml_text)
            except ET.ParseError:
                self.log.warning("Failed to parse %s RSS XML", union)
                continue

            for item in items:
                title = item.get("title", "")
                description = item.get("description", "")
                link = item.get("link", "")
                pub_date = item.get("pub_date", "")
                combined_text = f"{title} {description}"

                # Filter: must mention industrial action OR London transport
                has_action = bool(_ACTION_KEYWORDS.search(combined_text))
                has_london = bool(_LONDON_TRANSPORT.search(combined_text))

                if not (has_action or has_london):
                    skipped += 1
                    continue

                # Tag relevance level
                if has_action and has_london:
                    relevance = "high"
                elif has_action:
                    relevance = "medium"
                else:
                    relevance = "low"

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=title,
                    location_id=None,
                    lat=None,
                    lon=None,
                    metadata={
                        "union": union.upper(),
                        "url": link,
                        "pub_date": pub_date,
                        "description": description[:500],
                        "has_action_keywords": has_action,
                        "has_london_keywords": has_london,
                        "relevance": relevance,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Union [{union.upper()}] [{relevance}] {title[:120]}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "union": union.upper(),
                        "title": title,
                        "url": link,
                        "pub_date": pub_date,
                        "relevance": relevance,
                        "has_action_keywords": has_action,
                        "observation_id": obs.id,
                    },
                    location_id=None,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "Union industrial action: processed=%d skipped=%d", processed, skipped
        )

    @staticmethod
    def _parse_rss(xml_text: str) -> list[dict[str, str]]:
        """Parse RSS 2.0 XML into a list of item dicts."""
        root = ET.fromstring(xml_text)
        items = []
        # Handle RSS 2.0 and Atom namespaces
        for item_el in root.iter("item"):
            item: dict[str, str] = {}
            for child in item_el:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag in ("title", "description", "link", "pubDate"):
                    key = "pub_date" if tag == "pubDate" else tag
                    item[key] = (child.text or "").strip()
            items.append(item)
        return items


async def ingest_union_industrial_action(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one union RSS fetch cycle."""
    ingestor = UnionIndustrialActionIngestor(board, graph, scheduler)
    await ingestor.run()
