"""Bluesky transport sentiment ingestor — real-time London commuter posts.

Uses Bluesky's public search API (no auth required) to find posts mentioning
TfL lines, stations, and disruption keywords. Complements the Reddit-based
social_sentiment ingestor with more real-time, tweet-like observations.

Brain suggestion origin: geo-fenced social media monitoring API filtered for
transport-related keywords to act as a proxy for on-the-ground commuter
sentiment and crowding, especially when primary sensors fail.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.bluesky_transport")

# Bluesky public search (no auth, generous rate limits)
BSKY_SEARCH_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"

# Separate searches to stay focused and reduce noise
SEARCH_QUERIES = [
    "TfL delay london",
    "tube strike london",
    "bus crowded london",
    "signal failure tube",
    "london underground suspended",
    "northern line OR central line OR jubilee line delay",
    "elizabeth line OR piccadilly line OR victoria line delay",
]

# Station/line geo-tagging (shared with social_sentiment but kept local to avoid coupling)
_STATION_COORDS: dict[str, tuple[float, float]] = {
    "king's cross": (51.5308, -0.1238),
    "kings cross": (51.5308, -0.1238),
    "victoria": (51.4952, -0.1441),
    "waterloo": (51.5014, -0.1131),
    "liverpool street": (51.5178, -0.0823),
    "paddington": (51.5154, -0.1755),
    "euston": (51.5282, -0.1337),
    "oxford circus": (51.5152, -0.1418),
    "bank": (51.5133, -0.0886),
    "london bridge": (51.5055, -0.0860),
    "stratford": (51.5416, -0.0033),
    "canary wharf": (51.5054, -0.0235),
    "clapham junction": (51.4643, -0.1704),
    "finsbury park": (51.5642, -0.1065),
    "brixton": (51.4627, -0.1145),
    "camden town": (51.5392, -0.1426),
}

# Line names for tagging
_LINE_NAMES = {
    "northern line", "central line", "jubilee line", "victoria line",
    "piccadilly line", "district line", "circle line", "hammersmith",
    "metropolitan line", "bakerloo line", "elizabeth line", "overground",
    "dlr", "tram",
}

_NEGATIVE_WORDS = {
    "stuck", "stranded", "nightmare", "chaos", "awful", "terrible", "worst",
    "overcrowded", "packed", "rammed", "dangerous", "unsafe", "angry",
    "furious", "unacceptable", "disgusting", "shambles", "delayed",
    "cancelled", "suspended", "evacuated", "no service",
}
_POSITIVE_WORDS = {
    "running well", "smooth", "on time", "no issues", "quiet",
    "good service", "working fine", "back to normal", "resumed",
}


def _extract_location(text: str) -> tuple[float, float] | None:
    text_lower = text.lower()
    for station, coords in _STATION_COORDS.items():
        if station in text_lower:
            return coords
    return None


def _extract_lines(text: str) -> list[str]:
    text_lower = text.lower()
    return [line for line in _LINE_NAMES if line in text_lower]


def _score_sentiment(text: str) -> float:
    text_lower = text.lower()
    neg = sum(1 for w in _NEGATIVE_WORDS if w in text_lower)
    pos = sum(1 for w in _POSITIVE_WORDS if w in text_lower)
    total = neg + pos
    if total == 0:
        return 0.0
    return (pos - neg) / total


class BlueskyTransportIngestor(BaseIngestor):
    source_name = "bluesky_transport"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        seen_uris: set[str] = set()
        results = []
        fetch_failures = 0
        for query in SEARCH_QUERIES:
            params = {"q": query, "limit": "15", "sort": "latest"}
            data = await self.fetch(BSKY_SEARCH_URL, params=params)
            if not data or not isinstance(data, dict):
                fetch_failures += 1
                continue
            for post in data.get("posts", []):
                uri = post.get("uri", "")
                if uri in seen_uris:
                    continue
                seen_uris.add(uri)
                record = post.get("record", {})
                text = record.get("text", "")
                # Basic London relevance filter
                text_lower = text.lower()
                if not any(kw in text_lower for kw in (
                    "london", "tfl", "tube", "underground", "overground",
                    "dlr", "elizabeth line", "thameslink",
                )):
                    continue
                results.append({
                    "uri": uri,
                    "text": text,
                    "author": post.get("author", {}).get("handle", "unknown"),
                    "like_count": post.get("likeCount", 0),
                    "reply_count": post.get("replyCount", 0),
                    "repost_count": post.get("repostCount", 0),
                    "created_at": record.get("createdAt", ""),
                })
        # If ALL queries failed (API down/blocked), signal failure to circuit breaker
        if fetch_failures == len(SEARCH_QUERIES):
            return None
        return results

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("No Bluesky transport posts found")
            return

        processed = 0
        for post in data:
            text = post["text"]
            sentiment = _score_sentiment(text)
            coords = _extract_location(text)
            lines = _extract_lines(text)
            lat, lon = coords if coords else (None, None)
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None
            engagement = post["like_count"] + post["reply_count"] + post["repost_count"]

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=sentiment,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "text": text[:300],
                    "author": post["author"],
                    "engagement": engagement,
                    "like_count": post["like_count"],
                    "reply_count": post["reply_count"],
                    "sentiment": sentiment,
                    "lines_mentioned": lines,
                    "has_location": coords is not None,
                },
            )
            await self.board.store_observation(obs)

            sentiment_label = (
                "negative" if sentiment < -0.3
                else "positive" if sentiment > 0.3
                else "neutral"
            )
            location_str = ""
            if coords:
                for station in _STATION_COORDS:
                    if station in text.lower():
                        location_str = f" near {station.title()}"
                        break
            lines_str = f" [{', '.join(lines)}]" if lines else ""

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Bluesky transport {sentiment_label} "
                    f"(engagement={engagement}){lines_str}: "
                    f"{text[:140]}{location_str}"
                ),
                data={
                    "text": text[:300],
                    "author": post["author"],
                    "sentiment": sentiment,
                    "sentiment_label": sentiment_label,
                    "engagement": engagement,
                    "lines_mentioned": lines,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Bluesky transport: processed=%d posts", processed)


async def ingest_bluesky_transport(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Bluesky transport fetch cycle."""
    ingestor = BlueskyTransportIngestor(board, graph, scheduler)
    await ingestor.run()
