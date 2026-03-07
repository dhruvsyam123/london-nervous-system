"""Government calendar ingestor — parliamentary schedules and GLA press releases.

Fetches forward-looking 'planned event' data from:
  1. UK Parliament What's On API (Commons, Lords, Committee sittings)
  2. GLA (Greater London Authority) RSS press releases

This provides context to distinguish coordinated/planned activity from
spontaneous events — addressing the system's 'intent' blind spot.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from xml.etree import ElementTree

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.govt_calendar")

# Parliament What's On API — no key required
_WHATSON_URL = "https://whatson-api.parliament.uk/calendar/events/list"

# GLA press release RSS feeds
_GLA_RSS_FEEDS = [
    "https://www.london.gov.uk/rss-feeds/mayor-press-releases",
    "https://www.london.gov.uk/rss-feeds/london-assembly-press-releases",
]

# Westminster Palace coordinates
_WESTMINSTER_LAT = 51.4995
_WESTMINSTER_LON = -0.1248

# City Hall (Newham) coordinates
_CITY_HALL_LAT = 51.5082
_CITY_HALL_LON = 0.0057


class GovtCalendarIngestor(BaseIngestor):
    source_name = "govt_calendar"
    rate_limit_name = "default"

    async def fetch_data(self) -> dict[str, Any]:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Parliament calendar — upcoming 7 days
        parliament_events = await self.fetch(
            _WHATSON_URL,
            params={
                "startDate": today,
                "take": "30",
                "house": "Commons,Lords",
            },
        )

        # GLA press releases via RSS
        gla_items: list[dict] = []
        for feed_url in _GLA_RSS_FEEDS:
            rss_text = await self.fetch(feed_url)
            if isinstance(rss_text, str):
                gla_items.extend(self._parse_rss(rss_text))

        return {"parliament": parliament_events, "gla": gla_items}

    async def process(self, data: dict[str, Any]) -> None:
        westminster_cell = self.graph.latlon_to_cell(
            _WESTMINSTER_LAT, _WESTMINSTER_LON
        )
        city_hall_cell = self.graph.latlon_to_cell(_CITY_HALL_LAT, _CITY_HALL_LON)

        parl_count = await self._process_parliament(
            data.get("parliament"), westminster_cell
        )
        gla_count = await self._process_gla(data.get("gla", []), city_hall_cell)

        self.log.info(
            "Govt calendar: parliament_events=%d gla_releases=%d",
            parl_count,
            gla_count,
        )

    async def _process_parliament(
        self, data: Any, cell_id: str | None
    ) -> int:
        if not data:
            return 0

        # The API may return a list or a dict with a results key
        events = data if isinstance(data, list) else data.get("results", data.get("items", []))
        if not isinstance(events, list):
            self.log.info("Unexpected parliament calendar format: %s", type(data))
            return 0

        processed = 0
        for event in events[:30]:
            try:
                title = (
                    event.get("title", "")
                    or event.get("Title", "")
                    or event.get("name", "")
                )
                description = event.get("description", "") or event.get("Description", "")
                event_date = event.get("startDate", "") or event.get("date", "") or event.get("StartDate", "")
                house = event.get("house", "") or event.get("House", "")
                category = event.get("category", "") or event.get("Category", "")
                location = event.get("location", "") or event.get("Location", "")

                if not title:
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"Parliament event: {title}",
                    location_id=cell_id,
                    lat=_WESTMINSTER_LAT,
                    lon=_WESTMINSTER_LON,
                    metadata={
                        "event_type": "parliament_calendar",
                        "title": title,
                        "description": description[:500],
                        "date": event_date,
                        "house": house,
                        "category": category,
                        "location": location,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Parliament calendar: {title} — "
                        f"{house} {category} on {event_date}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "event_type": "parliament_calendar",
                        "title": title,
                        "date": event_date,
                        "house": house,
                        "category": category,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1
            except Exception:
                self.log.exception("Error processing parliament event")

        return processed

    async def _process_gla(
        self, items: list[dict], cell_id: str | None
    ) -> int:
        processed = 0
        for item in items[:20]:
            try:
                title = item.get("title", "")
                link = item.get("link", "")
                pub_date = item.get("pub_date", "")
                description = item.get("description", "")

                if not title:
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"GLA press release: {title}",
                    location_id=cell_id,
                    lat=_CITY_HALL_LAT,
                    lon=_CITY_HALL_LON,
                    metadata={
                        "event_type": "gla_press_release",
                        "title": title,
                        "link": link,
                        "pub_date": pub_date,
                        "description": description[:500],
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"GLA press release: {title} ({pub_date})",
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "event_type": "gla_press_release",
                        "title": title,
                        "link": link,
                        "pub_date": pub_date,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1
            except Exception:
                self.log.exception("Error processing GLA press release")

        return processed

    @staticmethod
    def _parse_rss(xml_text: str) -> list[dict]:
        """Parse RSS XML into a list of dicts with title, link, pub_date, description."""
        items: list[dict] = []
        try:
            root = ElementTree.fromstring(xml_text)
            for item_el in root.iter("item"):
                title_el = item_el.find("title")
                link_el = item_el.find("link")
                date_el = item_el.find("pubDate")
                desc_el = item_el.find("description")
                items.append({
                    "title": title_el.text.strip() if title_el is not None and title_el.text else "",
                    "link": link_el.text.strip() if link_el is not None and link_el.text else "",
                    "pub_date": date_el.text.strip() if date_el is not None and date_el.text else "",
                    "description": desc_el.text.strip() if desc_el is not None and desc_el.text else "",
                })
        except ElementTree.ParseError:
            log.warning("Failed to parse GLA RSS feed")
        return items


async def ingest_govt_calendar(
    board: MessageBoard, graph: LondonGraph, scheduler: AsyncScheduler,
) -> None:
    ingestor = GovtCalendarIngestor(board, graph, scheduler)
    await ingestor.run()
