"""Rail forum ingestor — pre-narrative disruption signals from niche communities.

Monitors RailForums.co.uk RSS feeds and r/uktrains for early disruption
signals that surface in enthusiast communities before mainstream news.
Complements social_sentiment (which covers r/london, r/tfl, r/LondonUnderground)
by targeting rail-specific niche sources.

Brain suggestion origin: Integrate data streams from public but niche
transport-related forums (e.g., RailUK Forums) and subreddits for
pre-narrative signals on potential disruptions.
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

log = logging.getLogger("london.ingestors.rail_forums")

# ── Sources ──────────────────────────────────────────────────────────────────

# RailForums.co.uk RSS (public, no auth)
RAILFORUMS_RSS_URLS = [
    "https://www.railforums.co.uk/forums/current-railway-issues-news.44/index.rss",
    "https://www.railforums.co.uk/forums/commuting.72/index.rss",
]

# Reddit (public JSON, no auth) — niche rail subreddits NOT covered by social_sentiment
REDDIT_SUBREDDITS = ["uktrains"]
REDDIT_NEW_URL = "https://www.reddit.com/r/{subreddit}/new.json"

_HEADERS_REDDIT = {"User-Agent": "LondonNervousSystem/1.0 (transport research)"}
_HEADERS_RSS = {"User-Agent": "LondonNervousSystem/1.0 (transport research)"}

# ── Disruption keywords (weighted: high-signal vs general) ───────────────────

_HIGH_SIGNAL_KEYWORDS = {
    "signal failure", "points failure", "track defect", "broken rail",
    "trespass", "fatality", "person on the track", "derailment",
    "overhead wire", "power failure", "emergency services",
    "speed restriction", "bridge strike", "level crossing failure",
    "flooding", "landslip", "rail replacement", "short formed",
    "cancelled", "terminating short",
}

_GENERAL_KEYWORDS = {
    "delay", "disruption", "suspended", "part closure", "blocked",
    "reduced service", "engineering works", "late", "overcrowded",
    "no service", "bus replacement", "diversion",
}

# London-relevant terms to filter for London relevance
_LONDON_TERMS = {
    "london", "paddington", "euston", "king's cross", "kings cross",
    "st pancras", "liverpool street", "fenchurch street", "victoria",
    "waterloo", "charing cross", "london bridge", "blackfriars",
    "marylebone", "moorgate", "cannon street", "thameslink",
    "southeastern", "south western", "greater anglia", "c2c",
    "chiltern", "great northern", "great western", "elizabeth line",
    "overground", "crossrail", "tfl rail", "govia",
    "southern", "gatwick express",
}

# Station coordinates for geo-tagging
_STATION_COORDS: dict[str, tuple[float, float]] = {
    "paddington": (51.5154, -0.1755),
    "euston": (51.5282, -0.1337),
    "king's cross": (51.5308, -0.1238),
    "kings cross": (51.5308, -0.1238),
    "st pancras": (51.5305, -0.1260),
    "liverpool street": (51.5178, -0.0823),
    "fenchurch street": (51.5119, -0.0789),
    "victoria": (51.4952, -0.1441),
    "waterloo": (51.5014, -0.1131),
    "charing cross": (51.5074, -0.1278),
    "london bridge": (51.5055, -0.0860),
    "blackfriars": (51.5117, -0.1037),
    "marylebone": (51.5225, -0.1631),
    "moorgate": (51.5186, -0.0886),
    "cannon street": (51.5113, -0.0904),
    "stratford": (51.5416, -0.0033),
    "clapham junction": (51.4643, -0.1704),
}


def _extract_location(text: str) -> tuple[float, float] | None:
    text_lower = text.lower()
    for station, coords in _STATION_COORDS.items():
        if station in text_lower:
            return coords
    return None


def _is_london_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(term in text_lower for term in _LONDON_TERMS)


def _score_disruption_signal(text: str) -> float:
    """Score how strong the disruption signal is: 0.0 (none) to 1.0 (high)."""
    text_lower = text.lower()
    high = sum(1 for kw in _HIGH_SIGNAL_KEYWORDS if kw in text_lower)
    general = sum(1 for kw in _GENERAL_KEYWORDS if kw in text_lower)
    if high + general == 0:
        return 0.0
    # High-signal keywords count double
    return min((high * 2 + general) / 5.0, 1.0)


def _parse_rss(xml_text: str) -> list[dict[str, str]]:
    """Parse RSS XML into list of items with title, description, link."""
    items = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items
    # Handle both RSS 2.0 and Atom-style feeds
    for item in root.iter("item"):
        title = item.findtext("title", "")
        desc = item.findtext("description", "")
        link = item.findtext("link", "")
        pub_date = item.findtext("pubDate", "")
        # Strip HTML tags from description
        desc_clean = re.sub(r"<[^>]+>", " ", desc).strip()
        items.append({
            "title": title,
            "description": desc_clean[:500],
            "link": link,
            "pub_date": pub_date,
            "source_type": "railforums",
        })
    return items


class RailForumIngestor(BaseIngestor):
    source_name = "rail_forums"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        import asyncio as _asyncio

        results: list[dict] = []
        fetch_failures = 0
        total_sources = len(RAILFORUMS_RSS_URLS) + len(REDDIT_SUBREDDITS)

        # 1. Fetch RailForums RSS feeds
        for rss_url in RAILFORUMS_RSS_URLS:
            data = await self.fetch(rss_url, headers=_HEADERS_RSS)
            if data and isinstance(data, str):
                items = _parse_rss(data)
                for item in items:
                    if _is_london_relevant(
                        f"{item['title']} {item['description']}"
                    ):
                        results.append(item)
            else:
                fetch_failures += 1
            await _asyncio.sleep(2)

        # 2. Fetch Reddit niche rail subreddits
        for sub in REDDIT_SUBREDDITS:
            url = REDDIT_NEW_URL.format(subreddit=sub)
            params = {"limit": "25", "sort": "new", "t": "day"}
            try:
                data = await self.fetch(
                    url, params=params, headers=_HEADERS_REDDIT
                )
            except Exception as exc:
                if "429" in str(exc):
                    self.log.warning(
                        "Reddit 429 on r/%s — backing off", sub
                    )
                    await _asyncio.sleep(10)
                    fetch_failures += 1
                    continue
                raise
            if data and isinstance(data, dict):
                children = data.get("data", {}).get("children", [])
                for child in children:
                    post = child.get("data", {})
                    full_text = (
                        f"{post.get('title', '')} {post.get('selftext', '')}"
                    )
                    if _is_london_relevant(full_text):
                        results.append({
                            "title": post.get("title", ""),
                            "description": post.get("selftext", "")[:500],
                            "link": post.get("permalink", ""),
                            "reddit_score": post.get("score", 0),
                            "num_comments": post.get("num_comments", 0),
                            "subreddit": post.get("subreddit", sub),
                            "source_type": "reddit",
                        })
            else:
                fetch_failures += 1
            await _asyncio.sleep(2)

        if fetch_failures == total_sources:
            return None
        return results

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("No rail forum posts found")
            return

        processed = 0
        for post in data:
            full_text = f"{post['title']} {post.get('description', '')}"
            signal_score = _score_disruption_signal(full_text)
            coords = _extract_location(full_text)
            lat, lon = coords if coords else (None, None)
            cell_id = (
                self.graph.latlon_to_cell(lat, lon) if lat and lon else None
            )

            source_type = post.get("source_type", "unknown")
            metadata: dict[str, Any] = {
                "title": post["title"][:200],
                "source_type": source_type,
                "signal_score": signal_score,
                "has_location": coords is not None,
            }
            if source_type == "reddit":
                metadata["subreddit"] = post.get("subreddit", "")
                metadata["reddit_score"] = post.get("reddit_score", 0)
                metadata["num_comments"] = post.get("num_comments", 0)
            elif source_type == "railforums":
                metadata["description"] = post.get("description", "")[:200]

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=signal_score,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata=metadata,
            )
            await self.board.store_observation(obs)

            # Format message
            signal_label = (
                "HIGH-SIGNAL"
                if signal_score > 0.6
                else "moderate"
                if signal_score > 0.3
                else "low"
            )
            location_str = ""
            if coords:
                for station in _STATION_COORDS:
                    if station in full_text.lower():
                        location_str = f" near {station.title()}"
                        break

            src_tag = (
                f"r/{post.get('subreddit', '?')}"
                if source_type == "reddit"
                else "RailForums"
            )

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Rail forum [{src_tag}] {signal_label} "
                    f"(signal={signal_score:.1f}): "
                    f"{post['title'][:140]}{location_str}"
                ),
                data={
                    "source": self.source_name,
                    "source_type": source_type,
                    "title": post["title"][:200],
                    "signal_score": signal_score,
                    "signal_label": signal_label,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Rail forums: processed=%d posts", processed)


async def ingest_rail_forums(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one rail forum fetch cycle."""
    ingestor = RailForumIngestor(board, graph, scheduler)
    await ingestor.run()
