"""Mastodon social ingestor — broad London event/reaction monitoring.

Uses Mastodon's public search API (no auth required) to find posts about
London beyond just transport — covering events, incidents, weather, safety,
culture, and general public reaction. Complements the transport-focused
Reddit and Bluesky ingestors by providing broader social signal coverage.

Brain suggestion origin: real-time, location-specific social media data for
verifying predictions about public reaction to localized events.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.mastodon_social")

# Mastodon public search (no auth, reasonable rate limits)
MASTODON_SEARCH_URL = "https://mastodon.social/api/v2/search"

# Broad London queries covering non-transport events and public reaction
SEARCH_QUERIES = [
    "london flood",
    "london fire",
    "london protest",
    "london air quality",
    "london power outage",
    "london event cancelled",
    "london police incident",
    "london weather",
    "london closure",
    "london emergency",
]

# London location geo-tagging (broader than just stations)
_LOCATION_COORDS: dict[str, tuple[float, float]] = {
    # Areas / boroughs
    "westminster": (51.4975, -0.1357),
    "shoreditch": (51.5264, -0.0780),
    "soho": (51.5137, -0.1337),
    "hackney": (51.5450, -0.0553),
    "brixton": (51.4613, -0.1156),
    "camden": (51.5390, -0.1426),
    "greenwich": (51.4769, -0.0005),
    "islington": (51.5362, -0.1033),
    "tower hamlets": (51.5150, -0.0379),
    "southwark": (51.5035, -0.0804),
    "lambeth": (51.4571, -0.1231),
    "lewisham": (51.4415, -0.0117),
    "croydon": (51.3762, -0.0982),
    "ealing": (51.5130, -0.3089),
    "hammersmith": (51.4927, -0.2248),
    "kensington": (51.4990, -0.1936),
    "chelsea": (51.4875, -0.1687),
    "city of london": (51.5155, -0.0922),
    "canary wharf": (51.5054, -0.0235),
    "stratford": (51.5416, -0.0033),
    # Landmarks
    "hyde park": (51.5073, -0.1657),
    "trafalgar square": (51.5080, -0.1281),
    "buckingham palace": (51.5014, -0.1419),
    "tower bridge": (51.5055, -0.0754),
    "big ben": (51.5007, -0.1246),
    "london eye": (51.5033, -0.1196),
    "wembley": (51.5560, -0.2796),
    "the o2": (51.5030, 0.0032),
    "olympic park": (51.5385, -0.0166),
    "excel": (51.5081, 0.0295),
    "thames": (51.5074, -0.1078),
}

_NEGATIVE_WORDS = {
    "flooding", "flooded", "fire", "smoke", "protest", "chaos", "blocked",
    "dangerous", "emergency", "evacuated", "closed", "cancelled", "unsafe",
    "crash", "accident", "stabbing", "shooting", "riot", "blackout",
    "outage", "sewage", "pollution", "gridlock",
}
_POSITIVE_WORDS = {
    "beautiful", "lovely", "great event", "fantastic", "sunny", "peaceful",
    "reopened", "restored", "cleared", "resolved", "safe", "celebrating",
}


def _extract_location(text: str) -> tuple[str, float, float] | None:
    """Extract location name and coords from text."""
    text_lower = text.lower()
    for loc, coords in _LOCATION_COORDS.items():
        if loc in text_lower:
            return (loc, coords[0], coords[1])
    return None


def _score_sentiment(text: str) -> float:
    """Keyword sentiment score: -1.0 (very negative) to 1.0 (very positive)."""
    text_lower = text.lower()
    neg = sum(1 for w in _NEGATIVE_WORDS if w in text_lower)
    pos = sum(1 for w in _POSITIVE_WORDS if w in text_lower)
    total = neg + pos
    if total == 0:
        return 0.0
    return (pos - neg) / total


def _categorize_event(text: str) -> str:
    """Categorize the type of event mentioned."""
    text_lower = text.lower()
    categories = {
        "weather": ["flood", "rain", "storm", "snow", "heat", "fog", "wind", "weather"],
        "safety": ["fire", "police", "stabbing", "shooting", "crash", "accident", "emergency"],
        "protest": ["protest", "march", "demonstration", "rally", "strike"],
        "environment": ["air quality", "pollution", "smog", "sewage", "thames"],
        "infrastructure": ["power outage", "blackout", "closure", "roadworks", "burst pipe"],
        "culture": ["event", "festival", "concert", "exhibition", "cancelled"],
    }
    for cat, keywords in categories.items():
        if any(kw in text_lower for kw in keywords):
            return cat
    return "general"


class MastodonSocialIngestor(BaseIngestor):
    source_name = "mastodon_social"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        import asyncio as _asyncio

        seen_ids: set[str] = set()
        results = []
        fetch_failures = 0

        for i, query in enumerate(SEARCH_QUERIES):
            if i > 0:
                await _asyncio.sleep(1.5)  # Rate-limit between queries
            params = {
                "q": query,
                "type": "statuses",
                "limit": "10",
            }
            try:
                data = await self.fetch(
                    MASTODON_SEARCH_URL, params=params,
                    headers={"Accept": "application/json"},
                )
            except Exception as exc:
                if "429" in str(exc):
                    self.log.warning("Mastodon 429 rate limit — backing off 15s")
                    await _asyncio.sleep(15)
                    fetch_failures += 1
                    continue
                self.log.debug("Mastodon search error for %r: %s", query, exc)
                fetch_failures += 1
                continue

            if not data or not isinstance(data, dict):
                fetch_failures += 1
                continue

            for status in data.get("statuses", []):
                status_id = status.get("id", "")
                if status_id in seen_ids:
                    continue
                seen_ids.add(status_id)

                # Extract plain text from HTML content
                content = status.get("content", "")
                import re
                plain_text = re.sub(r"<[^>]+>", " ", content)
                plain_text = re.sub(r"\s+", " ", plain_text).strip()

                # Must mention London for relevance
                if "london" not in plain_text.lower():
                    continue

                results.append({
                    "id": status_id,
                    "text": plain_text,
                    "author": status.get("account", {}).get("acct", "unknown"),
                    "favourites": status.get("favourites_count", 0),
                    "reblogs": status.get("reblogs_count", 0),
                    "replies": status.get("replies_count", 0),
                    "created_at": status.get("created_at", ""),
                    "language": status.get("language", ""),
                    "url": status.get("url", ""),
                })

        if fetch_failures == len(SEARCH_QUERIES):
            return None
        return results

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("No Mastodon London posts found")
            return

        processed = 0
        for post in data:
            text = post["text"]
            sentiment = _score_sentiment(text)
            event_type = _categorize_event(text)
            loc_info = _extract_location(text)

            lat, lon, loc_name = None, None, None
            if loc_info:
                loc_name, lat, lon = loc_info
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            engagement = post["favourites"] + post["reblogs"] + post["replies"]

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
                    "event_type": event_type,
                    "engagement": engagement,
                    "favourites": post["favourites"],
                    "reblogs": post["reblogs"],
                    "sentiment": sentiment,
                    "has_location": loc_info is not None,
                    "location_name": loc_name,
                },
            )
            await self.board.store_observation(obs)

            sentiment_label = (
                "negative" if sentiment < -0.3
                else "positive" if sentiment > 0.3
                else "neutral"
            )
            location_str = f" near {loc_name.title()}" if loc_name else ""

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Mastodon [{event_type}] {sentiment_label} "
                    f"(engagement={engagement}): "
                    f"{text[:140]}{location_str}"
                ),
                data={
                    "text": text[:300],
                    "author": post["author"],
                    "event_type": event_type,
                    "sentiment": sentiment,
                    "sentiment_label": sentiment_label,
                    "engagement": engagement,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Mastodon social: processed=%d posts", processed)


async def ingest_mastodon_social(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Mastodon social fetch cycle."""
    ingestor = MastodonSocialIngestor(board, graph, scheduler)
    await ingestor.run()
