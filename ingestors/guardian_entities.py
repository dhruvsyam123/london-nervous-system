"""Guardian Open Platform ingestor — entity-enriched UK news.

Fetches articles from The Guardian's free Content API with structured entity
tags (people, organisations, keywords). This directly addresses the Brain's
blind spot: verifying predictions that reference specific actors like
'Health Secretary', 'Prostate Cancer UK', or 'ONS statistics'.

The Guardian API returns tags typed as:
  - keyword   → topic/subject tags
  - contributor → article authors
  - tone       → editorial tone (e.g. "analysis", "obituaries")
  - type       → content format

We also query the API's "tag" field for person and organisation entities
by using section filters and show-tags=all.

Free tier: 12 requests/minute, 5,000/day — more than enough at 30-min cycles.
API key: free registration at https://open-platform.theguardian.com/access/

Brain suggestion origin: 'A real-time news API with robust entity extraction
(e.g., a paid service like NewsAPI.io or Aylien).'
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.guardian_entities")

GUARDIAN_API_URL = "https://content.guardianapis.com/search"

# London-relevant sections to query (rotate through them)
SECTION_QUERIES = [
    {"section": "uk-news", "q": "london"},
    {"section": "politics", "q": "london OR parliament OR westminster"},
    {"section": "environment", "q": "london OR thames OR pollution"},
    {"section": "business", "q": "london OR city OR economy"},
    {"q": "london health OR NHS OR air quality"},
]

# Tag types we extract as entities
ENTITY_TAG_TYPES = {"keyword", "contributor", "tone", "type"}

_LONDON_CENTRE = (51.5074, -0.1278)

# Simple person/org extraction from tag IDs when tag type is 'keyword'
_PERSON_PATTERN = re.compile(
    r"^(?:profile|politics|business|uk-news)/(.+)$"
)


def _dedup_key(api_url: str) -> str:
    """Short hash for deduplication."""
    return hashlib.md5(api_url.encode()).hexdigest()[:12]


def _extract_entities(tags: list[dict]) -> dict[str, list[str]]:
    """Extract structured entities from Guardian tag metadata."""
    entities: dict[str, list[str]] = {
        "people": [],
        "organisations": [],
        "keywords": [],
        "topics": [],
    }

    for tag in tags:
        tag_type = tag.get("type", "")
        web_title = tag.get("webTitle", "")
        tag_id = tag.get("id", "")

        if tag_type == "keyword":
            # Guardian keywords are rich — they include people, orgs, and topics
            section = tag.get("sectionId", "")
            if section in ("profile", "politics", "business"):
                # Likely a person or organisation
                if any(w in web_title.lower() for w in (
                    "secretary", "minister", "mp", "mayor", "chief",
                    "director", "chair", "president", "ceo",
                )):
                    entities["people"].append(web_title)
                elif any(w in web_title.lower() for w in (
                    "ltd", "plc", "council", "nhs", "tfl",
                    "authority", "agency", "commission", "trust",
                    "university", "police", "service",
                )):
                    entities["organisations"].append(web_title)
                else:
                    entities["keywords"].append(web_title)
            else:
                entities["keywords"].append(web_title)

        elif tag_type == "contributor":
            entities["people"].append(web_title)

    # Deduplicate while preserving order
    for key in entities:
        seen: set[str] = set()
        deduped = []
        for v in entities[key]:
            if v not in seen:
                seen.add(v)
                deduped.append(v)
        entities[key] = deduped

    return entities


class GuardianEntitiesIngestor(BaseIngestor):
    source_name = "guardian_entities"
    rate_limit_name = "guardian"

    def __init__(
        self,
        board: MessageBoard,
        graph: LondonGraph,
        scheduler: AsyncScheduler,
    ) -> None:
        super().__init__(board, graph, scheduler)
        self._cycle_index = 0
        self._api_key = os.environ.get("GUARDIAN_API_KEY", "test")

    async def fetch_data(self) -> Any:
        """Fetch one query set per cycle, rotating through SECTION_QUERIES."""
        query_set = SECTION_QUERIES[self._cycle_index % len(SECTION_QUERIES)]
        self._cycle_index += 1

        params: dict[str, Any] = {
            "api-key": self._api_key,
            "show-tags": "all",
            "show-fields": "trailText,byline,wordcount",
            "page-size": 20,
            "order-by": "newest",
        }
        params.update(query_set)

        data = await self.fetch(GUARDIAN_API_URL, params=params)
        if isinstance(data, dict):
            data["_query_set"] = query_set
        return data

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected Guardian response type: %s", type(data))
            return

        response = data.get("response", {})
        results = response.get("results", [])
        query_set = data.get("_query_set", {})
        query_label = query_set.get("section", query_set.get("q", "unknown"))[:30]

        if not results:
            self.log.info("Guardian [%s]: 0 results", query_label)
            return

        seen: set[str] = set()
        processed = 0

        for article in results:
            api_url = article.get("apiUrl", "")
            if not api_url:
                continue

            key = _dedup_key(api_url)
            if key in seen:
                continue
            seen.add(key)

            web_title = article.get("webTitle", "")
            web_url = article.get("webUrl", "")
            section = article.get("sectionName", "")
            pub_date = article.get("webPublicationDate", "")
            fields = article.get("fields", {})
            trail_text = fields.get("trailText", "")
            byline = fields.get("byline", "")

            # Extract entities from tags
            tags = article.get("tags", [])
            entities = _extract_entities(tags)

            # Count total entities for observation value
            total_entities = sum(len(v) for v in entities.values())

            lat, lon = _LONDON_CENTRE
            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=float(total_entities),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "title": web_title[:300],
                    "trail_text": trail_text[:500],
                    "byline": byline,
                    "section": section,
                    "published_at": pub_date,
                    "url": web_url,
                    "entities": entities,
                    "entity_count": total_entities,
                    "tag_count": len(tags),
                },
            )
            await self.board.store_observation(obs)

            # Build a concise entity summary for the message
            entity_parts = []
            if entities["people"]:
                entity_parts.append(f"people={','.join(entities['people'][:3])}")
            if entities["organisations"]:
                entity_parts.append(f"orgs={','.join(entities['organisations'][:3])}")
            if entities["keywords"]:
                entity_parts.append(f"kw={','.join(entities['keywords'][:3])}")
            entity_str = "; ".join(entity_parts) if entity_parts else "no entities"

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Guardian [{section}] ({total_entities} entities): "
                    f"{web_title[:120]} | {entity_str}"
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "text",
                    "title": web_title[:300],
                    "section": section,
                    "byline": byline,
                    "published_at": pub_date,
                    "url": web_url,
                    "entities": entities,
                    "entity_count": total_entities,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Guardian [%s]: processed=%d articles with entities",
            query_label, processed,
        )


async def ingest_guardian_entities(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Guardian fetch cycle."""
    ingestor = GuardianEntitiesIngestor(board, graph, scheduler)
    await ingestor.run()
