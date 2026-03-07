"""TfL venue-adjacent station crowding — entertainment venue footfall proxy.

Monitors crowding at tube/rail stations serving major entertainment venues
(The O2, West End theatres, Wembley, ExCeL, etc.) to test the hypothesis
that carbon_intensity/social_sentiment links are mediated by leisure activity.

Uses the same TfL crowding API as tfl_crowding.py but with a venue-focused
station set, tagging each observation with the associated venue for the
connectors to exploit.

No additional API key required — uses the public TfL Unified API.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.tfl_venue_stations")

TFL_CROWDING_URL = "https://api.tfl.gov.uk/crowding/{naptan}"

# Stations chosen specifically for proximity to major entertainment venues.
# Each entry maps a station to the venue(s) it serves, enabling the
# connectors to correlate crowding spikes with event schedules and
# cultural/leisure activity patterns.
VENUE_STATIONS: list[dict[str, Any]] = [
    # The O2 Arena — concerts, exhibitions
    {"naptan": "940GZZLUNGW", "name": "North Greenwich", "lat": 51.5005, "lon": 0.0039,
     "venues": ["The O2 Arena"], "venue_type": "arena"},
    # West End theatre district
    {"naptan": "940GZZLULSQ", "name": "Leicester Square", "lat": 51.5113, "lon": -0.1281,
     "venues": ["West End Theatres", "Leicester Square Cinemas"], "venue_type": "theatre"},
    {"naptan": "940GZZLUPCC", "name": "Piccadilly Circus", "lat": 51.5100, "lon": -0.1347,
     "venues": ["West End Theatres", "Criterion Theatre"], "venue_type": "theatre"},
    {"naptan": "940GZZLUCGN", "name": "Covent Garden", "lat": 51.5129, "lon": -0.1243,
     "venues": ["Royal Opera House", "Covent Garden Piazza"], "venue_type": "theatre"},
    # Wembley — stadium events, concerts
    {"naptan": "940GZZLUWRP", "name": "Wembley Park", "lat": 51.5635, "lon": -0.2795,
     "venues": ["Wembley Stadium", "Wembley Arena"], "venue_type": "stadium"},
    # ExCeL London — exhibitions, conventions
    {"naptan": "940GZZALDCK", "name": "Custom House for ExCeL", "lat": 51.5095, "lon": 0.0276,
     "venues": ["ExCeL London"], "venue_type": "exhibition"},
    # South Bank arts complex
    {"naptan": "940GZZLUWLO", "name": "Waterloo", "lat": 51.5036, "lon": -0.1143,
     "venues": ["Southbank Centre", "National Theatre", "BFI IMAX"], "venue_type": "arts"},
    # Stratford — Olympic Park, London Stadium
    {"naptan": "940GZZLUSTR", "name": "Stratford", "lat": 51.5416, "lon": -0.0042,
     "venues": ["London Stadium", "Queen Elizabeth Olympic Park"], "venue_type": "stadium"},
    # Kensington — museums, Albert Hall
    {"naptan": "940GZZLUSKS", "name": "South Kensington", "lat": 51.4941, "lon": -0.1738,
     "venues": ["Royal Albert Hall", "Natural History Museum", "V&A Museum"], "venue_type": "museum"},
    # Emirates Stadium — Arsenal FC
    {"naptan": "940GZZLUASL", "name": "Arsenal", "lat": 51.5586, "lon": -0.1059,
     "venues": ["Emirates Stadium"], "venue_type": "stadium"},
    # Tottenham — Spurs stadium
    {"naptan": "910GWHTHART", "name": "White Hart Lane", "lat": 51.6050, "lon": -0.0681,
     "venues": ["Tottenham Hotspur Stadium"], "venue_type": "stadium"},
    # Stamford Bridge area — Chelsea FC
    {"naptan": "940GZZLUFBY", "name": "Fulham Broadway", "lat": 51.4804, "lon": -0.1953,
     "venues": ["Stamford Bridge"], "venue_type": "stadium"},
    # The Oval — cricket
    {"naptan": "940GZZLUOVL", "name": "Oval", "lat": 51.4816, "lon": -0.1131,
     "venues": ["The Oval Cricket Ground"], "venue_type": "stadium"},
]

_DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _current_day_name() -> str:
    return _DAY_NAMES[datetime.now(timezone.utc).weekday()]


def _match_time_band(bands: list[dict], now_minutes: int) -> dict | None:
    for band in bands:
        try:
            start = band.get("startTime", "")
            end = band.get("endTime", "")
            sh, sm = (int(x) for x in start.split(":"))
            eh, em = (int(x) for x in end.split(":"))
            start_min = sh * 60 + sm
            end_min = eh * 60 + em
            if start_min <= now_minutes < end_min:
                return band
        except (ValueError, AttributeError):
            continue
    return None


class TflVenueStationsIngestor(BaseIngestor):
    """Monitors crowding at stations near major entertainment venues.

    Tags observations with venue names and types so downstream connectors
    can correlate leisure-driven footfall with energy, sentiment, and air
    quality patterns.
    """

    source_name = "tfl_venue_stations"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        day = _current_day_name()
        results = []
        for station in VENUE_STATIONS:
            url = TFL_CROWDING_URL.format(naptan=station["naptan"])
            data = await self.fetch(url, params={"dayOfTheWeek": day})
            if data is not None:
                results.append({"station": station, "data": data})
        return results if results else None

    async def process(self, data: Any) -> None:
        now = datetime.now(timezone.utc)
        now_minutes = now.hour * 60 + now.minute
        processed = 0
        skipped = 0

        for item in data:
            station = item["station"]
            raw = item["data"]

            bands: list[dict] = []
            if isinstance(raw, list):
                bands = raw
            elif isinstance(raw, dict):
                bands = raw.get("timeBands", raw.get("data", []))
                if isinstance(bands, dict):
                    bands = [bands]

            if not bands:
                skipped += 1
                continue

            band = _match_time_band(bands, now_minutes)
            if band is None:
                skipped += 1
                continue

            crowding_pct = band.get("percentageOfBaseline") or band.get("percentage")
            if crowding_pct is None:
                skipped += 1
                continue

            try:
                crowding_pct = float(crowding_pct)
            except (ValueError, TypeError):
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(station["lat"], station["lon"])

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=crowding_pct,
                location_id=cell_id,
                lat=station["lat"],
                lon=station["lon"],
                metadata={
                    "station": station["name"],
                    "naptan": station["naptan"],
                    "venues": station["venues"],
                    "venue_type": station["venue_type"],
                    "time_band": band.get("timeBand", ""),
                },
            )
            await self.board.store_observation(obs)

            venue_str = ", ".join(station["venues"])
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Venue station [{station['name']}] "
                    f"{crowding_pct:.0f}% of baseline "
                    f"({band.get('timeBand', '')}) — serves {venue_str}"
                ),
                data={
                    "station": station["name"],
                    "naptan": station["naptan"],
                    "crowding_pct": crowding_pct,
                    "time_band": band.get("timeBand", ""),
                    "venues": station["venues"],
                    "venue_type": station["venue_type"],
                    "lat": station["lat"],
                    "lon": station["lon"],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "TfL venue stations: processed=%d skipped=%d", processed, skipped
        )


async def ingest_tfl_venue_stations(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL venue stations cycle."""
    ingestor = TflVenueStationsIngestor(board, graph, scheduler)
    await ingestor.run()
