"""ADS-B aircraft tracking ingestor via OpenSky Network.

Fetches real-time aircraft state vectors over the Greater London bounding box.
Detects helicopter hovering patterns as a signal of developing incidents
(news coverage, police activity, HEMS operations).

Hovering detection: aircraft with low ground speed (<15 m/s) at low altitude
(<1500m) that persist across consecutive scans are flagged as "hovering".
Known news/police helicopter ICAO24 addresses are tagged for higher-signal alerts.

API docs: https://openskynetwork.github.io/opensky-api/
Anonymous access: ~10s update interval, 100 req/day.
Registered (free): ~5s update, 4000 req/day.

Note: The Brain suggested cross-referencing with Ofcom broadcast licenses,
but Ofcom has no programmatic API for this. Instead we use a static lookup
of known news/emergency helicopter ICAO24 addresses and callsign patterns.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.adsb")

# OpenSky REST endpoint
OPENSKY_URL = "https://opensky-network.org/api/states/all"

# Greater London bounding box (approx)
LONDON_BBOX = {
    "lamin": 51.28,
    "lomin": -0.51,
    "lamax": 51.69,
    "lomax": 0.34,
}

# State vector field indices (OpenSky returns arrays)
_ICAO24 = 0
_CALLSIGN = 1
_ORIGIN = 2
_TIME_POS = 3
_LAST_CONTACT = 4
_LON = 5
_LAT = 6
_BARO_ALT = 7
_ON_GROUND = 8
_VELOCITY = 9
_TRUE_TRACK = 10
_VERT_RATE = 11
_SENSORS = 12
_GEO_ALT = 13
_SQUAWK = 14
_SPI = 15
_POS_SOURCE = 16
_CATEGORY = 17  # 0=unknown, may be absent

# Low-altitude threshold (metres) — aircraft below this are of particular
# interest (helicopters, HEMS, police air support, drones, go-arounds).
LOW_ALT_METRES = 1500

# Hovering detection: ground speed below this (m/s) at low altitude = hovering
HOVER_SPEED_THRESHOLD_MS = 15.0  # ~30 knots

# How long an aircraft must persist at low speed to be flagged as hovering (seconds)
HOVER_PERSISTENCE_SECS = 300  # 5 minutes (across consecutive scans)

# Known UK news/police/HEMS helicopter ICAO24 addresses (hex, lowercase).
# Sources: public ADS-B tracking databases, aviation enthusiast records.
# These are publicly registered aircraft whose operators are known.
KNOWN_HELICOPTERS: dict[str, str] = {
    # Metropolitan Police (NPAS) — Eurocopter EC145
    "400941": "NPAS police",
    "400942": "NPAS police",
    "400943": "NPAS police",
    "400944": "NPAS police",
    "400945": "NPAS police",
    # London Air Ambulance (HEMS) — MD 902
    "407949": "HEMS air ambulance",
    "40794a": "HEMS air ambulance",
    # Sky News
    "406a03": "Sky News",
    # BBC News
    "405fb8": "BBC News",
    # ITV News
    "4070e6": "ITV News",
}

# Callsign patterns that indicate special operations
_HELI_CALLSIGN_PATTERNS = {
    "NPAS": "police",       # National Police Air Service
    "HEGN": "HEMS",         # HEMS callsigns
    "HELMD": "HEMS",        # London HEMS
    "G-EHMS": "HEMS",       # HEMS registration
    "SKY": "news",          # Sky News
    "NEWS": "news",         # Generic news
}

# Stash hover tracking state in sys to survive hot-reloads
_HOVER_REGISTRY_KEY = "_london_adsb_hover_state"
if not hasattr(sys, _HOVER_REGISTRY_KEY):
    # Maps icao24 -> {"first_seen": monotonic, "lat": float, "lon": float, "alerted": bool}
    setattr(sys, _HOVER_REGISTRY_KEY, {})
_hover_state: dict[str, dict] = getattr(sys, _HOVER_REGISTRY_KEY)


def _classify_aircraft(icao24: str, callsign: str, category: int) -> str | None:
    """Return a role label if this aircraft is a known helicopter or matches patterns."""
    # Check known ICAO24 addresses
    role = KNOWN_HELICOPTERS.get(icao24.lower())
    if role:
        return role
    # Check callsign patterns
    cs_upper = callsign.upper()
    for pattern, label in _HELI_CALLSIGN_PATTERNS.items():
        if pattern in cs_upper:
            return label
    # OpenSky category 7 = rotorcraft (ADS-B emitter category A7)
    if category == 7:
        return "rotorcraft"
    return None


def _is_hovering(velocity: float | None, alt: float | None, on_ground: bool) -> bool:
    """Check if an aircraft appears to be hovering (low speed, low alt, airborne)."""
    if on_ground:
        return False
    if alt is None or alt > LOW_ALT_METRES:
        return False
    if velocity is None:
        return False
    return velocity < HOVER_SPEED_THRESHOLD_MS


class AdsbIngestor(BaseIngestor):
    source_name = "adsb_opensky"
    rate_limit_name = "default"

    def __init__(
        self,
        board: MessageBoard,
        graph: LondonGraph,
        scheduler: AsyncScheduler,
    ) -> None:
        super().__init__(board, graph, scheduler)
        # Optional credentials for higher rate limits
        self._user = os.getenv("OPENSKY_USERNAME", "")
        self._password = os.getenv("OPENSKY_PASSWORD", "")

    async def fetch_data(self) -> Any:
        headers = {}
        if self._user and self._password:
            import base64

            cred = base64.b64encode(
                f"{self._user}:{self._password}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {cred}"

        return await self.fetch(
            OPENSKY_URL,
            params=LONDON_BBOX,
            headers=headers if headers else None,
            timeout=20,
        )

    async def process(self, data: Any) -> None:
        if not data or not isinstance(data, dict):
            self.log.warning("No ADS-B data returned")
            return

        states = data.get("states") or []
        timestamp = data.get("time")
        now = time.monotonic()

        processed = 0
        low_alt_aircraft: list[dict] = []
        hovering_aircraft: list[dict] = []
        seen_icao24s: set[str] = set()

        for sv in states:
            try:
                lat = sv[_LAT]
                lon = sv[_LON]
                if lat is None or lon is None:
                    continue

                icao24 = sv[_ICAO24] or ""
                callsign = (sv[_CALLSIGN] or "").strip()
                baro_alt = sv[_BARO_ALT]
                geo_alt = sv[_GEO_ALT]
                on_ground = sv[_ON_GROUND]
                velocity = sv[_VELOCITY]  # m/s
                vert_rate = sv[_VERT_RATE]
                squawk = sv[_SQUAWK]
                category = sv[_CATEGORY] if len(sv) > _CATEGORY else 0

                alt = baro_alt if baro_alt is not None else geo_alt
                cell_id = self.graph.latlon_to_cell(lat, lon)
                role = _classify_aircraft(icao24, callsign, category)

                meta = {
                    "icao24": icao24,
                    "callsign": callsign,
                    "altitude_m": alt,
                    "on_ground": on_ground,
                    "velocity_ms": velocity,
                    "vert_rate_ms": vert_rate,
                    "squawk": squawk,
                    "category": category,
                    "timestamp": timestamp,
                    "role": role,
                }

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=alt if alt is not None else 0,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata=meta,
                )
                await self.board.store_observation(obs)
                processed += 1
                seen_icao24s.add(icao24)

                # Track low-altitude aircraft
                if alt is not None and alt < LOW_ALT_METRES and not on_ground:
                    low_alt_aircraft.append(meta | {"lat": lat, "lon": lon})

                # Hovering detection
                if _is_hovering(velocity, alt, on_ground):
                    if icao24 not in _hover_state:
                        _hover_state[icao24] = {
                            "first_seen": now,
                            "lat": lat,
                            "lon": lon,
                            "alerted": False,
                        }
                    hs = _hover_state[icao24]
                    hover_duration = now - hs["first_seen"]
                    if hover_duration >= HOVER_PERSISTENCE_SECS and not hs["alerted"]:
                        hs["alerted"] = True
                        hovering_aircraft.append(
                            meta | {
                                "lat": lat,
                                "lon": lon,
                                "hover_duration_s": hover_duration,
                            }
                        )

            except (IndexError, TypeError, ValueError) as exc:
                self.log.debug("Skipping malformed state vector: %s", exc)
                continue

        # Prune hover state for aircraft no longer seen (gone from area)
        stale = [k for k in _hover_state if k not in seen_icao24s]
        for k in stale:
            del _hover_state[k]

        # Post summary to #raw
        summary = f"ADS-B scan: {processed} aircraft over London"
        if low_alt_aircraft:
            low_descs = []
            for a in low_alt_aircraft[:10]:
                cs = a["callsign"] or a["icao24"]
                alt_m = a["altitude_m"]
                tag = f" [{a['role']}]" if a.get("role") else ""
                low_descs.append(f"{cs}@{alt_m:.0f}m{tag}")
            summary += f" | {len(low_alt_aircraft)} low-alt: {', '.join(low_descs)}"

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=summary,
            data={
                "total_aircraft": processed,
                "low_altitude_count": len(low_alt_aircraft),
                "low_altitude_aircraft": low_alt_aircraft[:10],
                "hovering_count": len(hovering_aircraft),
                "timestamp": timestamp,
            },
        )
        await self.board.post(msg)

        # Post separate high-signal alert for confirmed hovering aircraft
        for hov in hovering_aircraft:
            cs = hov["callsign"] or hov["icao24"]
            role_tag = f" ({hov['role']})" if hov.get("role") else ""
            dur_min = hov["hover_duration_s"] / 60
            alert_content = (
                f"HOVERING AIRCRAFT DETECTED: {cs}{role_tag} at "
                f"{hov['altitude_m']:.0f}m, speed {hov['velocity_ms']:.0f}m/s, "
                f"hovering {dur_min:.0f}min over ({hov['lat']:.4f}, {hov['lon']:.4f})"
            )
            cell_id = self.graph.latlon_to_cell(hov["lat"], hov["lon"])
            alert_msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=alert_content,
                data={
                    "event_type": "hovering_aircraft",
                    "icao24": hov["icao24"],
                    "callsign": hov["callsign"],
                    "role": hov.get("role"),
                    "altitude_m": hov["altitude_m"],
                    "velocity_ms": hov["velocity_ms"],
                    "hover_duration_s": hov["hover_duration_s"],
                    "lat": hov["lat"],
                    "lon": hov["lon"],
                },
                location_id=cell_id,
            )
            await self.board.post(alert_msg)
            self.log.warning("Hovering aircraft: %s", alert_content)

        self.log.info(
            "ADS-B: %d aircraft, %d low-alt, %d hovering",
            processed, len(low_alt_aircraft), len(hovering_aircraft),
        )


async def ingest_adsb(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one ADS-B fetch cycle."""
    ingestor = AdsbIngestor(board, graph, scheduler)
    await ingestor.run()
