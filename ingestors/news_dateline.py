"""News dateline context ingestor — distinguishes journalist location from event location.

Addresses the 'Westminster Digital Phantom' problem: news articles geocoded to
Westminster/Millbank because the journalist is physically there, not because
an event occurred at that location. A BBC political editor reporting on national
statistics from Millbank does NOT constitute a statistical event at Millbank.

Uses GDELT's artgeo mode (GeoJSON) which provides geocoded article locations,
then cross-references with a static registry of known London media hubs
(broadcast centres, newsrooms, press offices). Articles geolocated to a media
hub from a national-scope outlet are flagged as dateline locations.

The spatial_connector can use `is_dateline_location` metadata to discount these
observations when clustering anomalies.

No additional API key required — GDELT is free.

Brain suggestion origin: 'Integrate a news dateline or journalist location API
to differentiate between the location an article is about versus the location
it was filed from.'
"""

from __future__ import annotations

import logging
import math
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.news_dateline")

# GDELT doc API — artgeo mode returns GeoJSON with article geolocations
GDELT_GEO_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_GEO_PARAMS = {
    "query": "london",
    "mode": "artgeo",
    "maxrecords": "75",
    "format": "geojson",
}

# ── London media hub registry ──────────────────────────────────────────────
# Known locations where journalists file from. Articles geocoded to these
# coordinates (within ~300m) from matching domains are likely dateline, not event.
# Format: (lat, lon, name, matching_domain_substrings)

_MEDIA_HUBS: list[tuple[float, float, str, list[str]]] = [
    # BBC — Millbank / New Broadcasting House
    (51.4963, -0.1267, "BBC Millbank", ["bbc."]),
    (51.5188, -0.1441, "BBC Broadcasting House", ["bbc."]),
    # Sky News — Osterley / Millbank studio
    (51.4817, -0.3526, "Sky Osterley", ["sky."]),
    (51.4946, -0.1259, "Sky Millbank Studio", ["sky."]),
    # ITV — South Bank
    (51.5068, -0.1131, "ITV South Bank", ["itv."]),
    # Channel 4 — Horseferry Road
    (51.4946, -0.1323, "Channel 4 HQ", ["channel4."]),
    # The Guardian — Kings Place, King's Cross
    (51.5347, -0.1246, "Guardian Kings Place", ["theguardian."]),
    # The Times / News UK — London Bridge
    (51.5045, -0.0865, "News UK London Bridge", ["thetimes.", "thesun.", "newsuk."]),
    # Daily Mail / Associated Newspapers — Kensington
    (51.5008, -0.1916, "Daily Mail Kensington", ["dailymail.", "mailonline."]),
    # Financial Times — Bracken House / Southwark Bridge
    (51.5117, -0.0963, "FT Bracken House", ["ft."]),
    # Reuters — Canary Wharf
    (51.5054, -0.0235, "Reuters Canary Wharf", ["reuters."]),
    # Press Association / PA Media — Victoria
    (51.4966, -0.1438, "PA Media Victoria", ["pa."]),
    # Westminster / Parliament press gallery
    (51.4995, -0.1248, "Westminster Press Gallery", []),
    # Millbank media village (used by multiple broadcasters)
    (51.4953, -0.1265, "Millbank Media Village", []),
]

# Match radius in metres — articles within this distance of a hub are flagged
_HUB_MATCH_RADIUS_M = 350

# National-scope outlet domains — articles from these about national topics
# are likely filed from a hub but not about that location
_NATIONAL_OUTLET_DOMAINS = frozenset([
    "bbc.", "sky.", "itv.", "channel4.", "theguardian.", "thetimes.",
    "telegraph.", "ft.", "reuters.", "independent.", "dailymail.",
    "mirror.", "express.", "standard.", "metro.", "thesun.",
    "huffingtonpost.", "buzzfeed.", "inews.",
])

# National-scope topic keywords in article titles/URLs — if present,
# the article is about a broad topic, not a hyperlocal event
_NATIONAL_TOPIC_SIGNALS = frozenset([
    "budget", "election", "minister", "government", "parliament",
    "nhs", "economy", "inflation", "interest rate", "chancellor",
    "prime minister", "cabinet", "downing street", "whitehall",
    "home office", "foreign office", "treasury", "statistics",
    "ons", "census", "national", "uk-wide", "country",
    "policy", "legislation", "bill", "act", "reform",
])


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in metres between two lat/lon points."""
    R = 6_371_000  # Earth radius in metres
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _find_media_hub(
    lat: float, lon: float, domain: str
) -> tuple[str, float] | None:
    """Check if a coordinate is near a known media hub.

    Returns (hub_name, distance_m) or None.
    """
    best: tuple[str, float] | None = None
    for hub_lat, hub_lon, hub_name, hub_domains in _MEDIA_HUBS:
        dist = _haversine_m(lat, lon, hub_lat, hub_lon)
        if dist > _HUB_MATCH_RADIUS_M:
            continue
        # If hub has domain constraints, check them
        if hub_domains and not any(d in domain for d in hub_domains):
            continue
        if best is None or dist < best[1]:
            best = (hub_name, dist)
    return best


def _is_national_scope(title: str, url: str) -> bool:
    """Heuristic: does this article appear to be about a national topic?"""
    text = f"{title} {url}".lower()
    return any(kw in text for kw in _NATIONAL_TOPIC_SIGNALS)


def _is_national_outlet(domain: str) -> bool:
    """Check if domain belongs to a national news outlet."""
    domain_lower = domain.lower()
    return any(nd in domain_lower for nd in _NATIONAL_OUTLET_DOMAINS)


class NewsDatelineIngestor(BaseIngestor):
    source_name = "news_dateline"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(GDELT_GEO_URL, params=GDELT_GEO_PARAMS)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected GDELT GeoJSON type: %s", type(data))
            return

        features = data.get("features", [])
        if not features:
            self.log.info("GDELT artgeo returned 0 features")
            return

        processed = 0
        dateline_flagged = 0
        skipped = 0

        for feature in features:
            try:
                props = feature.get("properties", {})
                geom = feature.get("geometry", {})
                coords = geom.get("coordinates", [])

                if not coords or len(coords) < 2:
                    skipped += 1
                    continue

                # GeoJSON is [lon, lat]
                lon, lat = float(coords[0]), float(coords[1])

                # Skip articles outside Greater London bounding box
                if not (51.28 <= lat <= 51.70 and -0.51 <= lon <= 0.33):
                    skipped += 1
                    continue

                url = props.get("url", "") or props.get("name", "")
                title = props.get("name", "") or props.get("html", "")
                domain = props.get("domain", "") or props.get("urltone", "")
                # GDELT artgeo sometimes encodes tone/counts in properties
                tone = props.get("urltone", "")
                shareimage = props.get("shareimage", "")

                # Extract domain from URL if not in props
                if not domain and url:
                    parts = url.split("/")
                    if len(parts) >= 3:
                        domain = parts[2]

                cell_id = self.graph.latlon_to_cell(lat, lon)

                # ── Core dateline analysis ──
                hub_match = _find_media_hub(lat, lon, domain)
                national_outlet = _is_national_outlet(domain)
                national_scope = _is_national_scope(title, url)

                # Dateline confidence: high if near media hub + national outlet + national topic
                is_dateline = False
                dateline_confidence = 0.0

                if hub_match:
                    hub_name, hub_dist = hub_match
                    dateline_confidence = 0.4  # base: near a media hub
                    if national_outlet:
                        dateline_confidence += 0.3
                    if national_scope:
                        dateline_confidence += 0.3
                    is_dateline = dateline_confidence >= 0.6
                else:
                    hub_name = None
                    hub_dist = None
                    # Even without hub proximity, a national outlet writing
                    # about national topics at Westminster is suspect
                    if national_outlet and national_scope:
                        dateline_confidence = 0.3

                if is_dateline:
                    dateline_flagged += 1

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=title[:500] if title else url[:500],
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "url": url,
                        "title": title[:300],
                        "domain": domain,
                        "is_dateline_location": is_dateline,
                        "dateline_confidence": round(dateline_confidence, 2),
                        "media_hub": hub_name,
                        "media_hub_distance_m": round(hub_dist, 1) if hub_dist else None,
                        "is_national_outlet": national_outlet,
                        "is_national_scope": national_scope,
                        "article_geo_type": "dateline" if is_dateline else "event",
                    },
                )
                await self.board.store_observation(obs)

                # Build descriptive content
                flag = "[DATELINE]" if is_dateline else "[EVENT]"
                hub_info = f" near {hub_name}" if hub_name else ""

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"News geo {flag} [{domain}]{hub_info}: "
                        f"{title[:120] if title else url[:120]}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "url": url,
                        "title": title[:300],
                        "domain": domain,
                        "lat": lat,
                        "lon": lon,
                        "is_dateline_location": is_dateline,
                        "dateline_confidence": round(dateline_confidence, 2),
                        "media_hub": hub_name,
                        "is_national_outlet": national_outlet,
                        "is_national_scope": national_scope,
                        "article_geo_type": "dateline" if is_dateline else "event",
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing GDELT artgeo feature")
                skipped += 1

        self.log.info(
            "News dateline: processed=%d dateline_flagged=%d skipped=%d",
            processed, dateline_flagged, skipped,
        )


async def ingest_news_dateline(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one GDELT dateline context cycle."""
    ingestor = NewsDatelineIngestor(board, graph, scheduler)
    await ingestor.run()
