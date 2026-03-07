"""Transport union feeds ingestor — RMT news + UK Strike Calendar.

Provides leading indicators on staffing issues and potential industrial action
affecting London transport services. Combines:
1. RMT union RSS feed (news, statements, campaign updates)
2. UK Strike Calendar iCal feed (structured upcoming strike dates)

Brain suggestion origin: Public statements and social media feeds from
transport worker unions (e.g., RMT, ASLEF, Unite) to provide leading
indicators on staffing issues or potential industrial action affecting
transport services.

No API keys required — both sources are publicly accessible.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.union_feeds")

# RMT RSS feed — official union news
RMT_RSS_URL = "https://www.rmt.org.uk/news/feed.xml"

# UK Strike Calendar iCal feed — structured industrial action dates
STRIKE_ICAL_URL = "https://www.strikecalendar.co.uk/ical"

# Transport-related keywords for filtering strike calendar entries
_TRANSPORT_KEYWORDS = {
    "rmt", "aslef", "unite", "tssa", "tfl", "tube", "underground",
    "rail", "bus", "train", "transport", "overground", "dlr",
    "elizabeth line", "southern", "southeastern", "thameslink",
    "avanti", "lner", "gwr", "network rail", "crossrail",
    "london underground", "london bus", "driver", "signaller",
    "windrush", "piccadilly", "northern line", "central line",
    "jubilee line", "victoria line", "bakerloo", "metropolitan",
    "district line", "circle line", "hammersmith",
}

# Keywords indicating industrial action / labour tension
_ACTION_KEYWORDS = {
    "strike", "walkout", "ballot", "dispute", "action", "picket",
    "industrial", "stoppage", "overtime ban", "work to rule",
    "negotiations", "deadlock", "breakdown", "rejected", "mandate",
}

# Keywords indicating staffing / safety concerns
_STAFFING_KEYWORDS = {
    "staffing", "shortage", "vacancy", "recruitment", "safety",
    "assaults", "violence", "conditions", "pay", "wage", "roster",
    "redundancy", "cuts", "job losses", "privatisation", "outsourcing",
}

# Central London coords for generic transport union observations
_LONDON_CENTRE = (51.5074, -0.1278)


def _is_transport_related(text: str) -> bool:
    """Check if text is related to London transport."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in _TRANSPORT_KEYWORDS)


def _classify_content(text: str) -> str:
    """Classify the type of union content."""
    text_lower = text.lower()
    if any(kw in text_lower for kw in ("strike", "walkout", "stoppage", "picket")):
        return "industrial_action"
    if any(kw in text_lower for kw in ("ballot", "vote", "mandate")):
        return "ballot"
    if any(kw in text_lower for kw in ("dispute", "deadlock", "breakdown", "rejected")):
        return "dispute"
    if any(kw in text_lower for kw in _STAFFING_KEYWORDS):
        return "staffing_concern"
    if any(kw in text_lower for kw in ("negotiate", "deal", "agreement", "settlement")):
        return "negotiation"
    return "general_statement"


def _urgency_score(text: str) -> float:
    """Score 0-1 indicating how urgent/actionable for transport disruption."""
    text_lower = text.lower()
    score = 0.0
    if any(kw in text_lower for kw in ("strike", "walkout", "stoppage")):
        score += 0.4
    if any(kw in text_lower for kw in ("ballot", "vote", "mandate")):
        score += 0.3
    if any(kw in text_lower for kw in ("dispute", "deadlock", "rejected")):
        score += 0.2
    if any(kw in text_lower for kw in ("london", "tfl", "tube", "underground")):
        score += 0.2
    if any(kw in text_lower for kw in ("tomorrow", "this week", "imminent", "confirmed")):
        score += 0.2
    return min(score, 1.0)


def _parse_rss(xml_text: str) -> list[dict]:
    """Parse RSS XML into a list of item dicts."""
    items = []
    try:
        root = ET.fromstring(xml_text)
        # Handle RSS 2.0 structure
        channel = root.find("channel")
        if channel is None:
            return items
        for item in channel.findall("item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            description = item.findtext("description", "")
            pub_date = item.findtext("pubDate", "")
            guid = item.findtext("guid", "")
            items.append({
                "title": title.strip(),
                "link": link.strip(),
                "description": _strip_html(description.strip()),
                "pub_date": pub_date.strip(),
                "guid": guid.strip(),
            })
    except ET.ParseError as exc:
        log.warning("Failed to parse RMT RSS: %s", exc)
    return items


def _strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    return re.sub(r"<[^>]+>", "", text).strip()


def _parse_ical_events(ical_text: str) -> list[dict]:
    """Parse iCal text into a list of event dicts (transport-related only)."""
    events = []
    current_event: dict[str, str] | None = None

    for line in ical_text.splitlines():
        line = line.strip()
        if line == "BEGIN:VEVENT":
            current_event = {}
        elif line == "END:VEVENT" and current_event is not None:
            summary = current_event.get("SUMMARY", "")
            if _is_transport_related(summary):
                events.append({
                    "summary": summary,
                    "dtstart": current_event.get("DTSTART", ""),
                    "dtend": current_event.get("DTEND", ""),
                    "uid": current_event.get("UID", ""),
                })
            current_event = None
        elif current_event is not None and ":" in line:
            # Handle properties like DTSTART;VALUE=DATE:20260326
            key_part, _, value = line.partition(":")
            key = key_part.split(";")[0]
            current_event[key] = value

    return events


def _parse_ical_date(date_str: str) -> str | None:
    """Parse iCal date string to ISO format."""
    try:
        if len(date_str) == 8:  # YYYYMMDD
            dt = datetime.strptime(date_str, "%Y%m%d")
            return dt.strftime("%Y-%m-%d")
        elif len(date_str) >= 15:  # YYYYMMDDTHHmmSS
            dt = datetime.strptime(date_str[:15], "%Y%m%dT%H%M%S")
            return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        pass
    return date_str


class UnionFeedsIngestor(BaseIngestor):
    source_name = "union_feeds"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {"rss_items": [], "ical_events": []}
        failures = 0

        # Fetch RMT RSS
        rss_data = await self.fetch(RMT_RSS_URL)
        if rss_data and isinstance(rss_data, str):
            results["rss_items"] = _parse_rss(rss_data)
        else:
            failures += 1
            self.log.warning("Failed to fetch RMT RSS feed")

        # Fetch Strike Calendar iCal
        ical_data = await self.fetch(STRIKE_ICAL_URL)
        if ical_data and isinstance(ical_data, str):
            results["ical_events"] = _parse_ical_events(ical_data)
        else:
            failures += 1
            self.log.warning("Failed to fetch Strike Calendar iCal")

        # If both failed, signal circuit breaker
        if failures == 2:
            return None

        return results

    async def process(self, data: Any) -> None:
        processed = 0

        # Process RMT RSS items
        for item in data.get("rss_items", []):
            title = item["title"]
            description = item["description"]
            full_text = f"{title} {description}"

            # Only process transport-related items
            if not _is_transport_related(full_text):
                continue

            content_type = _classify_content(full_text)
            urgency = _urgency_score(full_text)
            lat, lon = _LONDON_CENTRE
            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=urgency,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "feed": "rmt_rss",
                    "title": title[:200],
                    "description": description[:500],
                    "link": item["link"],
                    "pub_date": item["pub_date"],
                    "content_type": content_type,
                    "urgency": urgency,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Union [{content_type}] (urgency={urgency:.1f}): "
                    f"{title[:200]}"
                ),
                data={
                    "feed": "rmt_rss",
                    "title": title[:200],
                    "content_type": content_type,
                    "urgency": urgency,
                    "pub_date": item["pub_date"],
                    "link": item["link"],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        # Process Strike Calendar events
        now = datetime.now(timezone.utc)
        for event in data.get("ical_events", []):
            summary = event["summary"]
            dtstart = _parse_ical_date(event["dtstart"])
            dtend = _parse_ical_date(event["dtend"])

            # Filter to upcoming events (crude but effective)
            if dtstart and len(dtstart) >= 10:
                try:
                    event_date = datetime.strptime(dtstart[:10], "%Y-%m-%d")
                    event_date = event_date.replace(tzinfo=timezone.utc)
                    days_until = (event_date - now).days
                    if days_until < -7:  # skip events more than a week old
                        continue
                except ValueError:
                    days_until = None
            else:
                days_until = None

            urgency = _urgency_score(summary)
            # Boost urgency for imminent strikes
            if days_until is not None and days_until <= 7:
                urgency = min(urgency + 0.3, 1.0)
            if days_until is not None and days_until <= 1:
                urgency = min(urgency + 0.2, 1.0)

            lat, lon = _LONDON_CENTRE
            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=urgency,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "feed": "strike_calendar",
                    "summary": summary[:300],
                    "dtstart": dtstart,
                    "dtend": dtend,
                    "days_until": days_until,
                    "content_type": "scheduled_action",
                    "urgency": urgency,
                },
            )
            await self.board.store_observation(obs)

            days_str = (
                f" (in {days_until}d)" if days_until is not None and days_until >= 0
                else f" ({abs(days_until)}d ago)" if days_until is not None
                else ""
            )
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Strike calendar [scheduled_action] (urgency={urgency:.1f}): "
                    f"{summary[:200]} on {dtstart}{days_str}"
                ),
                data={
                    "feed": "strike_calendar",
                    "summary": summary[:200],
                    "dtstart": dtstart,
                    "dtend": dtend,
                    "days_until": days_until,
                    "urgency": urgency,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Union feeds: processed=%d (rss=%d, ical=%d)",
            processed,
            len(data.get("rss_items", [])),
            len(data.get("ical_events", [])),
        )


async def ingest_union_feeds(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one union feeds fetch cycle."""
    ingestor = UnionFeedsIngestor(board, graph, scheduler)
    await ingestor.run()
