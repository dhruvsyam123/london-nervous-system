"""Mastodon social density ingestor — post density & keyword analysis per London borough.

Uses Mastodon's public hashtag timeline API (no auth required) to measure
social media activity density across London boroughs and extract trending
keywords. Unlike social_sentiment (Reddit transport) and bluesky_transport
(Bluesky transport), this ingestor covers *all* topics to provide a
geographically-resolved social activity heatmap.

Brain suggestion origin: anonymized, geofenced social media post density and
keyword analysis at the cell or borough level to validate localized social
trend predictions.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.mastodon_social_density")

# Mastodon public hashtag timeline — no auth required, ~300 req/5 min
MASTODON_INSTANCE = "https://mastodon.social"
HASHTAG_URL = MASTODON_INSTANCE + "/api/v1/timelines/tag/{tag}"

# London boroughs with representative lat/lon (centroid approximations)
BOROUGH_TAGS: dict[str, tuple[float, float]] = {
    "camden": (51.5517, -0.1588),
    "hackney": (51.5450, -0.0553),
    "westminster": (51.4975, -0.1357),
    "islington": (51.5362, -0.1027),
    "southwark": (51.4733, -0.0750),
    "lambeth": (51.4571, -0.1231),
    "towerhamlets": (51.5150, -0.0340),
    "greenwich": (51.4769, 0.0005),
    "lewisham": (51.4415, -0.0117),
    "newham": (51.5077, 0.0469),
    "shoreditch": (51.5264, -0.0780),
    "brixton": (51.4613, -0.1156),
    "croydon": (51.3714, -0.0977),
    "ealing": (51.5130, -0.3089),
    "barnet": (51.6252, -0.1517),
    "brent": (51.5588, -0.2817),
    "haringey": (51.5906, -0.1110),
    "wandsworth": (51.4567, -0.1910),
    "hammersmith": (51.4927, -0.2248),
    "kensington": (51.5020, -0.1947),
    "deptford": (51.4742, -0.0259),
    "peckham": (51.4740, -0.0691),
    "stratford": (51.5416, -0.0033),
    "canarywharf": (51.5054, -0.0235),
    "woolwich": (51.4907, 0.0634),
}

# Additional general London hashtags (not borough-specific)
GENERAL_TAGS = ["london", "londonlife", "londontown"]

# Stop words to exclude from keyword extraction
_STOP_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "shall",
    "should", "may", "might", "can", "could", "to", "of", "in", "for",
    "on", "with", "at", "by", "from", "as", "into", "through", "during",
    "before", "after", "above", "below", "between", "out", "off", "over",
    "under", "again", "further", "then", "once", "here", "there", "when",
    "where", "why", "how", "all", "each", "every", "both", "few", "more",
    "most", "other", "some", "such", "no", "not", "only", "own", "same",
    "so", "than", "too", "very", "just", "and", "but", "or", "if", "it",
    "its", "this", "that", "these", "those", "i", "me", "my", "we", "you",
    "he", "she", "they", "them", "his", "her", "our", "your", "what",
    "which", "who", "whom", "amp", "http", "https", "www", "com",
}

# Minimum word length for keyword extraction
_MIN_WORD_LEN = 4


def _extract_keywords(texts: list[str], top_n: int = 10) -> list[str]:
    """Extract top keywords from a list of post texts."""
    word_counts: Counter[str] = Counter()
    for text in texts:
        words = text.lower().split()
        for word in words:
            # Strip punctuation
            clean = word.strip(".,!?;:\"'()[]{}#@/\\<>-_")
            if (
                len(clean) >= _MIN_WORD_LEN
                and clean not in _STOP_WORDS
                and not clean.startswith("http")
                and clean.isalpha()
            ):
                word_counts[clean] += 1
    return [w for w, _ in word_counts.most_common(top_n)]


class MastodonSocialDensityIngestor(BaseIngestor):
    source_name = "mastodon_social_density"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, list[dict]] = {}
        fetch_failures = 0
        total_tags = len(BOROUGH_TAGS) + len(GENERAL_TAGS)

        # Fetch borough-specific hashtags
        for tag in list(BOROUGH_TAGS.keys()) + GENERAL_TAGS:
            url = HASHTAG_URL.format(tag=tag)
            params = {"limit": "20"}
            try:
                data = await self.fetch(url, params=params)
            except Exception:
                fetch_failures += 1
                continue
            if data and isinstance(data, list):
                posts = []
                for post in data:
                    content = post.get("content", "")
                    # Strip HTML tags (Mastodon returns HTML)
                    import re
                    clean_text = re.sub(r"<[^>]+>", " ", content).strip()
                    if not clean_text:
                        continue
                    posts.append({
                        "text": clean_text,
                        "created_at": post.get("created_at", ""),
                        "reblogs_count": post.get("reblogs_count", 0),
                        "favourites_count": post.get("favourites_count", 0),
                        "replies_count": post.get("replies_count", 0),
                        "language": post.get("language", ""),
                    })
                results[tag] = posts
            else:
                fetch_failures += 1

            # Gentle rate limiting — 200ms between requests
            await asyncio.sleep(0.2)

        if fetch_failures == total_tags:
            return None
        return results

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("No Mastodon social density data")
            return

        processed = 0

        # Process borough-specific tags
        for tag, posts in data.items():
            if tag in BOROUGH_TAGS:
                lat, lon = BOROUGH_TAGS[tag]
                area_name = tag.title()
            elif tag in GENERAL_TAGS:
                # General London tags — use central London coords
                lat, lon = 51.5074, -0.1278
                area_name = f"London (#{tag})"
            else:
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)
            post_count = len(posts)
            texts = [p["text"] for p in posts]
            keywords = _extract_keywords(texts, top_n=8)
            total_engagement = sum(
                p["reblogs_count"] + p["favourites_count"] + p["replies_count"]
                for p in posts
            )
            avg_engagement = total_engagement / max(post_count, 1)

            # Store density as numeric observation
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(post_count),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "area": area_name,
                    "hashtag": tag,
                    "post_count": post_count,
                    "keywords": keywords,
                    "avg_engagement": round(avg_engagement, 1),
                    "total_engagement": total_engagement,
                },
            )
            await self.board.store_observation(obs)

            keywords_str = ", ".join(keywords[:5]) if keywords else "none"
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Social density [{area_name}] "
                    f"posts={post_count} engagement={total_engagement} "
                    f"keywords=[{keywords_str}]"
                ),
                data={
                    "area": area_name,
                    "hashtag": tag,
                    "post_count": post_count,
                    "keywords": keywords,
                    "avg_engagement": round(avg_engagement, 1),
                    "total_engagement": total_engagement,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Mastodon social density: processed=%d areas from %d hashtags",
            processed, len(data),
        )


async def ingest_mastodon_social_density(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Mastodon social density cycle."""
    ingestor = MastodonSocialDensityIngestor(board, graph, scheduler)
    await ingestor.run()
