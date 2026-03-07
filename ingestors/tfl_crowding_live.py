"""TfL live station crowding — near-real-time busyness from tap data.

Uses TfL's Crowding Live API to fetch current busyness ratios for
Underground stations.  The ratio (0.0–1.0) represents how busy a station
is *right now* relative to its historical peak since July 2019.

This complements tfl_crowding.py (historical time-band profiles) and
tfl_passenger_counts.py (StopPoint baseline data).  The delta between the
live ratio and the historical profile is the key signal for detecting
passenger displacement during disruptions.

Endpoint: GET https://api.tfl.gov.uk/crowding/{naptan}/Live
Updates:  ~every 5 minutes
Auth:     Free TfL API key (optional, higher rate limits with key)
Coverage: Most Underground stations except Kensington Olympia,
          Heathrow T5, Willesden Junction.
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

log = logging.getLogger("london.ingestors.tfl_crowding_live")

TFL_CROWDING_LIVE_URL = "https://api.tfl.gov.uk/crowding/{naptan}/Live"

# Key interchange stations — same core set as tfl_crowding.py for
# cross-referencing live vs historical profiles.
STATIONS: list[dict[str, Any]] = [
    {"naptan": "940GZZLUKSX", "name": "King's Cross St Pancras", "lat": 51.5308, "lon": -0.1238},
    {"naptan": "940GZZLUWLO", "name": "Waterloo", "lat": 51.5036, "lon": -0.1143},
    {"naptan": "940GZZLUVIC", "name": "Victoria", "lat": 51.4965, "lon": -0.1447},
    {"naptan": "940GZZLULVT", "name": "Liverpool Street", "lat": 51.5178, "lon": -0.0823},
    {"naptan": "940GZZLUPAC", "name": "Paddington", "lat": 51.5154, "lon": -0.1755},
    {"naptan": "940GZZLUOXC", "name": "Oxford Circus", "lat": 51.5152, "lon": -0.1415},
    {"naptan": "940GZZLULNB", "name": "London Bridge", "lat": 51.5052, "lon": -0.0864},
    {"naptan": "940GZZLUBNK", "name": "Bank", "lat": 51.5133, "lon": -0.0886},
    {"naptan": "940GZZLUGPK", "name": "Green Park", "lat": 51.5067, "lon": -0.1428},
    {"naptan": "940GZZLUEUS", "name": "Euston", "lat": 51.5282, "lon": -0.1337},
    {"naptan": "940GZZLUBND", "name": "Bond Street", "lat": 51.5142, "lon": -0.1494},
    {"naptan": "940GZZLUBST", "name": "Baker Street", "lat": 51.5226, "lon": -0.1571},
    {"naptan": "940GZZLUWSM", "name": "Westminster", "lat": 51.5010, "lon": -0.1254},
    {"naptan": "940GZZLUTCR", "name": "Tottenham Court Road", "lat": 51.5165, "lon": -0.1310},
    {"naptan": "940GZZLUSTR", "name": "Stratford", "lat": 51.5416, "lon": -0.0042},
    {"naptan": "940GZZLUCPK", "name": "Canary Wharf", "lat": 51.5035, "lon": -0.0187},
    {"naptan": "940GZZLUCWR", "name": "Canada Water", "lat": 51.4982, "lon": -0.0502},
    {"naptan": "940GZZLUKNG", "name": "Kennington", "lat": 51.4884, "lon": -0.1053},
    {"naptan": "940GZZLUFCO", "name": "Farringdon", "lat": 51.5203, "lon": -0.1053},
    {"naptan": "940GZZLUMGT", "name": "Moorgate", "lat": 51.5186, "lon": -0.0886},
]


class TflCrowdingLiveIngestor(BaseIngestor):
    source_name = "tfl_crowding_live"
    rate_limit_name = "tfl"

    def _build_params(self) -> dict[str, str]:
        """Include TfL app key if available for higher rate limits."""
        params: dict[str, str] = {}
        app_key = os.environ.get("TFL_APP_KEY")
        if app_key:
            params["app_key"] = app_key
        return params

    async def fetch_data(self) -> Any:
        """Fetch live crowding for all monitored stations."""
        params = self._build_params()
        results = []
        for station in STATIONS:
            url = TFL_CROWDING_LIVE_URL.format(naptan=station["naptan"])
            data = await self.fetch(url, params=params)
            if data is not None:
                results.append({"station": station, "data": data})
        return results if results else None

    async def process(self, data: Any) -> None:
        processed = 0
        skipped = 0

        for item in data:
            station = item["station"]
            raw = item["data"]

            # The Live endpoint returns a busyness ratio (0.0–1.0).
            # Response shape varies: may be a dict with "percentageOfBaseline"
            # or a nested structure under "dataPoints" / "timeBands".
            busyness = None

            if isinstance(raw, dict):
                # Direct ratio field
                busyness = (
                    raw.get("percentageOfBaseline")
                    or raw.get("liveCrowding")
                    or raw.get("busyness")
                )
                # Nested under dataPoints
                if busyness is None:
                    data_points = raw.get("dataPoints", [])
                    if isinstance(data_points, list) and data_points:
                        # Take the most recent data point
                        latest = data_points[-1]
                        busyness = latest.get("value") or latest.get("percentageOfBaseline")
            elif isinstance(raw, (int, float)):
                busyness = raw

            if busyness is None:
                skipped += 1
                continue

            try:
                busyness = float(busyness)
            except (ValueError, TypeError):
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(station["lat"], station["lon"])

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=busyness,
                location_id=cell_id,
                lat=station["lat"],
                lon=station["lon"],
                metadata={
                    "station": station["name"],
                    "naptan": station["naptan"],
                    "metric": "live_busyness_ratio",
                },
            )
            await self.board.store_observation(obs)

            # Human-readable: show as percentage
            pct_str = f"{busyness * 100:.0f}%" if busyness <= 1.0 else f"{busyness:.1f}"

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Live crowding [{station['name']}] "
                    f"busyness={pct_str} of historical peak"
                ),
                data={
                    "station": station["name"],
                    "naptan": station["naptan"],
                    "busyness_ratio": busyness,
                    "lat": station["lat"],
                    "lon": station["lon"],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "TfL live crowding: processed=%d skipped=%d", processed, skipped
        )


async def ingest_tfl_crowding_live(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL live crowding cycle."""
    ingestor = TflCrowdingLiveIngestor(board, graph, scheduler)
    await ingestor.run()
