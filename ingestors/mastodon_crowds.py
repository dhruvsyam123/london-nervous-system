"""Mastodon crowd & event ingestor — London crowd formation and social event signals.

Uses Mastodon's public search API (no auth required) to find posts about
crowds, gatherings, protests, festivals, and large events in London.
Complements Reddit (transport sentiment) and Bluesky (transport disruptions)
by focusing specifically on crowd-formation and social event predictions.

Brain suggestion origin: real-time geolocated social media for crowd-formation
and social event predictions.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.mastodon_crowds")

# Mastodon public search — no auth needed, generous rate limits
MASTODON_SEARCH_URL = "https://mastodon.social/api/v2/search"

# Crowd / event focused queries (distinct from transport focus of other ingestors)
SEARCH_QUERIES = [
    "london crowd protest",
    "london festival concert",
    "london march rally",
    "london queue huge",
    "london packed event",
    "london gathering thousands",
    "hyde park event london",
    "trafalgar square london",
    "wembley crowd london",
    "london marathon",
]

# Landmark / venue geo-tagging for crowd-relevant locations
_VENUE_COORDS: dict[str, tuple[float, float]] = {
    "hyde park": (51.5073, -0.1657),
    "trafalgar square": (51.5080, -0.1281),
    "wembley": (51.5560, -0.2795),
    "the mall": (51.5025, -0.1340),
    "parliament square": (51.5005, -0.1246),
    "buckingham palace": (51.5014, -0.1419),
    "tower bridge": (51.5055, -0.0754),
    "olympic park": (51.5385, -0.0166),
    "greenwich park": (51.4769, -0.0005),
    "regent's park": (51.5313, -0.1570),
    "south bank": (51.5055, -0.1160),
    "brick lane": (51.5220, -0.0718),
    "camden market": (51.5413, -0.1470),
    "covent garden": (51.5117, -0.1240),
    "borough market": (51.5055, -0.0910),
    "o2 arena": (51.5030, 0.0032),
    "tottenham hotspur stadium": (51.6043, -0.0663),
    "stamford bridge": (51.4817, -0.1910),
    "emirates stadium": (51.5549, -0.1084),
    "twickenham": (51.4559, -0.3415),
    "alexandra palace": (51.5940, -0.1300),
    "crystal palace park": (51.4218, -0.0753),
    "excel london": (51.5085, 0.0294),
    "earls court": (51.4912, -0.1953),
    "notting hill": (51.5090, -0.1960),
    "brixton academy": (51.4656, -0.1148),
    "victoria park": (51.5365, -0.0382),
    "clapham common": (51.4588, -0.1519),
    "finsbury park": (51.5660, -0.1070),
    "whitehall": (51.5040, -0.1260),
    "downing street": (51.5033, -0.1276),
}

# Crowd-related keywords for relevance scoring
_CROWD_KEYWORDS = {
    "crowd", "crowds", "crowded", "packed", "heaving", "thousands",
    "protest", "march", "rally", "demonstration", "gathering",
    "festival", "concert", "gig", "match", "game",
    "queue", "queuing", "queueing", "line",
    "marathon", "parade", "carnival", "fireworks",
    "evacuated", "evacuation", "stampede", "crush",
}

_URGENCY_WORDS = {
    "evacuated", "evacuation", "stampede", "crush", "emergency",
    "police", "ambulance", "blocked", "closed", "dangerous",
}


def _extract_location(text: str) -> tuple[str, float, float] | None:
    """Try to extract venue name and lat/lon from text."""
    text_lower = text.lower()
    for venue, coords in _VENUE_COORDS.items():
        if venue in text_lower:
            return venue, coords[0], coords[1]
    return None


def _crowd_relevance_score(text: str) -> float:
    """Score 0-1 for how crowd/event relevant a post is."""
    text_lower = text.lower()
    hits = sum(1 for kw in _CROWD_KEYWORDS if kw in text_lower)
    return min(hits / 3.0, 1.0)


def _urgency_score(text: str) -> float:
    """Score 0-1 for urgency of crowd situation."""
    text_lower = text.lower()
    hits = sum(1 for kw in _URGENCY_WORDS if kw in text_lower)
    return min(hits / 2.0, 1.0)


class MastodonCrowdsIngestor(BaseIngestor):
    source_name = "mastodon_crowds"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        import asyncio as _asyncio

        seen_ids: set[str] = set()
        results = []
        fetch_failures = 0

        for i, query in enumerate(SEARCH_QUERIES):
            if i > 0:
                await _asyncio.sleep(1.5)  # Respect rate limits
            params = {
                "q": query,
                "type": "statuses",
                "limit": "10",
            }
            try:
                data = await self.fetch(MASTODON_SEARCH_URL, params=params)
            except Exception:
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

                # Extract text (strip basic HTML tags)
                content = status.get("content", "")
                import re
                text = re.sub(r"<[^>]+>", " ", content).strip()

                # Filter: must mention London and have crowd relevance
                text_lower = text.lower()
                if "london" not in text_lower:
                    continue
                if _crowd_relevance_score(text) < 0.33:
                    continue

                results.append({
                    "id": status_id,
                    "text": text,
                    "account": status.get("account", {}).get("acct", "unknown"),
                    "favourites": status.get("favourites_count", 0),
                    "reblogs": status.get("reblogs_count", 0),
                    "replies": status.get("replies_count", 0),
                    "created_at": status.get("created_at", ""),
                    "url": status.get("url", ""),
                })

        if fetch_failures == len(SEARCH_QUERIES):
            return None
        return results

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("No Mastodon crowd/event posts found")
            return

        processed = 0
        for post in data:
            text = post["text"]
            relevance = _crowd_relevance_score(text)
            urgency = _urgency_score(text)
            location = _extract_location(text)

            lat, lon, venue_name = None, None, None
            if location:
                venue_name, lat, lon = location

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None
            engagement = post["favourites"] + post["reblogs"] + post["replies"]

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=relevance,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "text": text[:300],
                    "account": post["account"],
                    "engagement": engagement,
                    "crowd_relevance": relevance,
                    "urgency": urgency,
                    "venue": venue_name,
                    "has_location": location is not None,
                },
            )
            await self.board.store_observation(obs)

            location_str = f" at {venue_name.title()}" if venue_name else ""
            urgency_str = " [URGENT]" if urgency >= 0.5 else ""

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Mastodon crowd signal{urgency_str} "
                    f"(relevance={relevance:.1f}, engagement={engagement}): "
                    f"{text[:140]}{location_str}"
                ),
                data={
                    "text": text[:300],
                    "account": post["account"],
                    "crowd_relevance": relevance,
                    "urgency": urgency,
                    "engagement": engagement,
                    "venue": venue_name,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Mastodon crowds: processed=%d posts", processed)


async def ingest_mastodon_crowds(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Mastodon crowd fetch cycle."""
    ingestor = MastodonCrowdsIngestor(board, graph, scheduler)
    await ingestor.run()
