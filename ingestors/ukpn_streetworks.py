"""UKPN scheduled streetworks ingestor — planned & active engineering works.

Pulls two datasets from the UKPN Open Data Portal (Opendatasoft API v2.1):

1. **Proposed Streetworks** — permit applications for upcoming road/street works
   by UK Power Networks.  Refreshed daily from Street Manager.

2. **Open Streetworks** — currently active (in-progress) works on the highway
   within the UKPN footprint.

These feed the Brain's ability to contextualise operational anomalies: a traffic
spike near a UKPN excavation site, or a localised power blip during cable
replacement, can be explained rather than flagged as a mystery.

API key: optional (same UKPN_API_KEY used by ukpn_substation ingestor).
Source: https://ukpowernetworks.opendatasoft.com/
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LONDON_BBOX
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.ukpn_streetworks")

BASE_URL = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1"

PROPOSED_DS = "ukpn-proposed-streetworks"
OPEN_DS = "ukpn-open-streetworks"

# Bounding-box filter for Greater London (opendatasoft geo_point_2d syntax)
_LON_C = (LONDON_BBOX["min_lon"] + LONDON_BBOX["max_lon"]) / 2
_LAT_C = (LONDON_BBOX["min_lat"] + LONDON_BBOX["max_lat"]) / 2
_LONDON_GEO_FILTER = (
    f"within_distance(geo_point_2d, geom'POINT({_LON_C} {_LAT_C})', 30km)"
)


class UKPNStreetworksIngestor(BaseIngestor):
    source_name = "ukpn_streetworks"
    rate_limit_name = "default"

    def _headers(self) -> dict[str, str] | None:
        key = os.environ.get("UKPN_API_KEY", "")
        return {"Authorization": f"Apikey {key}"} if key else None

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {}

        for label, ds_id in [("proposed", PROPOSED_DS), ("open", OPEN_DS)]:
            url = f"{BASE_URL}/catalog/datasets/{ds_id}/records"
            params: dict[str, Any] = {"limit": 100}
            # Try geo filter; fall back to unfiltered if the dataset lacks geo_point_2d
            resp = await self.fetch(url, params={**params, "where": _LONDON_GEO_FILTER}, headers=self._headers())
            if resp is None:
                resp = await self.fetch(url, params=params, headers=self._headers())
            if resp:
                results[label] = resp

        return results if results else None

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            return

        proposed_n = await self._process_works(data.get("proposed"), kind="proposed")
        open_n = await self._process_works(data.get("open"), kind="open")

        self.log.info(
            "UKPN streetworks: %d proposed, %d open works ingested",
            proposed_n, open_n,
        )

    async def _process_works(self, data: Any, *, kind: str) -> int:
        if not data or not isinstance(data, dict):
            return 0

        records = data.get("results", [])
        count = 0

        for rec in records:
            f = rec  # v2.1 API returns flat records

            work_ref = (
                f.get("work_reference_number")
                or f.get("permit_reference_number")
                or f.get("works_reference", "unknown")
            )
            street = f.get("street_name", f.get("usrn_description", ""))
            area = f.get("area_name", f.get("highway_authority", ""))
            category = f.get("work_category", f.get("works_category", ""))
            activity = f.get("activity_type", f.get("description_of_work", ""))
            start_date = f.get("proposed_start_date", f.get("actual_start_date", ""))
            end_date = f.get("proposed_end_date", f.get("actual_end_date", ""))
            traffic_mgmt = f.get("traffic_management_type", "")
            status = f.get("permit_status", f.get("work_status", ""))

            # Extract location
            lat, lon = None, None
            geo = f.get("geo_point_2d")
            if isinstance(geo, dict):
                lat = geo.get("lat")
                lon = geo.get("lon")
            elif isinstance(geo, (list, tuple)) and len(geo) == 2:
                lat, lon = geo[0], geo[1]

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=category or kind,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "type": f"streetworks_{kind}",
                    "work_ref": work_ref,
                    "street": street,
                    "area": area,
                    "category": category,
                    "activity": activity,
                    "start_date": start_date,
                    "end_date": end_date,
                    "traffic_management": traffic_mgmt,
                    "status": status,
                },
            )
            await self.board.store_observation(obs)

            # Severity hint based on work category
            severity = ""
            cat_lower = (category or "").lower()
            if "major" in cat_lower:
                severity = " [MAJOR]"
            elif "standard" in cat_lower:
                severity = " [standard]"

            location_str = f"{street}, {area}" if street and area else (street or area or "unknown location")
            dates_str = ""
            if start_date and end_date:
                dates_str = f" | {start_date} → {end_date}"
            elif start_date:
                dates_str = f" | from {start_date}"

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"UKPN {kind} works{severity}: {activity or category or 'engineering'} "
                    f"at {location_str}{dates_str}"
                    f"{f' | traffic: {traffic_mgmt}' if traffic_mgmt else ''}"
                ),
                data={
                    "type": f"streetworks_{kind}",
                    "work_ref": work_ref,
                    "category": category,
                    "street": street,
                    "area": area,
                    "start_date": start_date,
                    "end_date": end_date,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            count += 1

        return count


async def ingest_ukpn_streetworks(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one UKPN streetworks fetch cycle."""
    ingestor = UKPNStreetworksIngestor(board, graph, scheduler)
    await ingestor.run()
