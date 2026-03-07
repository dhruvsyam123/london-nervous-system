"""Guardian energy/pollution narrative ingestor — tracks media framing of
carbon intensity, grid status, and pollution topics.

Addresses the Brain's hypothesis that the 'carbon_intensity -> social_sentiment'
link is mediated by media narratives: when the grid is dirty or pollution spikes,
do news outlets amplify alarm, shifting public mood? This ingestor captures the
narrative layer between physical measurements and social sentiment.

Uses The Guardian Open Platform API (same as guardian_news.py) but with focused
energy/environment queries and topic-specific framing analysis.

API: https://open-platform.theguardian.com/
Free tier: 12 req/s with key, 1 req/s with 'test' key. We use ~1 req/30min.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.guardian_energy")

GUARDIAN_URL = "https://content.guardianapis.com/search"

# Query terms targeting the carbon_intensity -> social_sentiment hypothesis
_ENERGY_QUERY = (
    "(carbon intensity OR carbon emissions OR grid electricity OR fossil fuel OR "
    "renewable energy OR wind power OR solar power OR gas power OR coal power) OR "
    "(air pollution OR air quality OR NO2 OR PM2.5 OR particulate OR smog OR "
    "toxic air OR clean air zone OR ULEZ) OR "
    "(energy grid OR blackout OR power cut OR national grid OR energy crisis OR "
    "energy prices OR fuel poverty)"
)

# Guardian sections most relevant to energy/environment coverage
_SECTIONS = "environment|business|uk-news|science|politics"

# Topic categories for downstream correlation with carbon_intensity data
_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "carbon_grid": [
        "carbon intensity", "carbon emissions", "grid", "electricity",
        "generation mix", "gas power", "coal", "renewable",
        "wind power", "solar power", "nuclear",
    ],
    "air_pollution": [
        "air pollution", "air quality", "no2", "pm2.5", "pm10",
        "particulate", "smog", "toxic air", "ulez", "clean air",
        "nitrogen dioxide", "emissions",
    ],
    "energy_crisis": [
        "energy crisis", "blackout", "power cut", "fuel poverty",
        "energy prices", "energy bills", "national grid", "power outage",
    ],
}

# Narrative framing keywords — is the article alarmist, neutral, or optimistic?
_ALARM_WORDS = frozenset([
    "crisis", "emergency", "toxic", "deadly", "dangerous", "choking",
    "worst", "alarming", "soaring", "surge", "breach", "illegal",
    "warning", "threat", "scandal", "failing", "spiralling", "record high",
])
_OPTIMISM_WORDS = frozenset([
    "record low", "cleanest", "breakthrough", "green", "progress",
    "milestone", "success", "improve", "decline", "target met",
    "renewable record", "zero carbon", "transition", "investment",
])

_LONDON_LAT = 51.509865
_LONDON_LON = -0.118092


def _classify_topics(text: str) -> list[str]:
    """Return list of matching topic categories for an article."""
    text_lower = text.lower()
    return [
        topic for topic, keywords in _TOPIC_KEYWORDS.items()
        if any(kw in text_lower for kw in keywords)
    ]


def _score_framing(text: str) -> tuple[float, str]:
    """Score narrative framing from -1 (alarmist) to +1 (optimistic).

    Returns (score, label).
    """
    text_lower = text.lower()
    alarm = sum(1 for w in _ALARM_WORDS if w in text_lower)
    optimism = sum(1 for w in _OPTIMISM_WORDS if w in text_lower)
    total = alarm + optimism
    if total == 0:
        return 0.0, "neutral"
    score = (optimism - alarm) / total
    if score > 0.2:
        return score, "optimistic"
    if score < -0.2:
        return score, "alarmist"
    return score, "neutral"


class GuardianEnergyIngestor(BaseIngestor):
    source_name = "guardian_energy"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: LondonGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self.api_key = os.environ.get("GUARDIAN_API_KEY", "test")

    async def fetch_data(self) -> Any:
        params = {
            "api-key": self.api_key,
            "q": _ENERGY_QUERY,
            "section": _SECTIONS,
            "page-size": "30",
            "order-by": "newest",
            "show-fields": "headline,trailText",
        }
        return await self.fetch(GUARDIAN_URL, params=params)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected Guardian response type: %s", type(data))
            return

        response = data.get("response", {})
        results = response.get("results", [])
        if not results:
            self.log.info("Guardian energy: 0 results")
            return

        processed = 0
        skipped = 0
        framing_totals = {"alarmist": 0, "neutral": 0, "optimistic": 0}
        topic_totals: dict[str, int] = {}

        cell_id = self.graph.latlon_to_cell(_LONDON_LAT, _LONDON_LON)

        for article in results:
            try:
                title = (article.get("fields") or {}).get("headline", "") or article.get("webTitle", "")
                trail = (article.get("fields") or {}).get("trailText", "")
                url = article.get("webUrl", "")
                section = article.get("sectionName", "")
                pub_date = article.get("webPublicationDate", "")

                if not title:
                    skipped += 1
                    continue

                full_text = f"{title} {trail}"
                topics = _classify_topics(full_text)
                framing_score, framing_label = _score_framing(full_text)
                framing_totals[framing_label] += 1
                for t in topics:
                    topic_totals[t] = topic_totals.get(t, 0) + 1

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=framing_score,
                    location_id=cell_id,
                    lat=_LONDON_LAT,
                    lon=_LONDON_LON,
                    metadata={
                        "url": url,
                        "title": title,
                        "trail": trail[:200] if trail else "",
                        "section": section,
                        "pub_date": pub_date,
                        "topics": topics,
                        "framing_score": framing_score,
                        "framing_label": framing_label,
                        "hypothesis_link": "carbon_intensity_social_sentiment",
                    },
                )
                await self.board.store_observation(obs)

                topic_str = ", ".join(topics) if topics else "general_energy"
                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Energy narrative [{section}] ({framing_label} {framing_score:+.1f}) "
                        f"[{topic_str}] {title[:120]}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "url": url,
                        "title": title,
                        "section": section,
                        "pub_date": pub_date,
                        "topics": topics,
                        "framing_score": framing_score,
                        "framing_label": framing_label,
                        "hypothesis_link": "carbon_intensity_social_sentiment",
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing Guardian energy article")
                skipped += 1

        self.log.info(
            "Guardian energy narratives: processed=%d skipped=%d framing=%s topics=%s",
            processed, skipped, dict(framing_totals), dict(topic_totals),
        )


async def ingest_guardian_energy(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Guardian energy narrative cycle."""
    ingestor = GuardianEnergyIngestor(board, graph, scheduler)
    await ingestor.run()
