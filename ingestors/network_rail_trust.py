"""Network Rail TRUST train movement ingestor — real-time disruption ground truth.

Connects to Network Rail's open data STOMP feed to collect train movement events
(activations, cancellations, movements, reinstatements) for London-area services.
Uses a connect → subscribe → collect → disconnect polling pattern to fit the
coordinator's periodic scheduling model.

Provides ground-truth data on rail disruption root causes (signal failures,
points failures, traction supply issues) rather than inferring from secondary
effects like bus overcrowding.

Requires free registration at https://publicdatafeeds.networkrail.co.uk/
Set NETWORK_RAIL_USER and NETWORK_RAIL_PASS in .env.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.network_rail_trust")

STOMP_HOST = "publicdatafeeds.networkrail.co.uk"
STOMP_PORT = 61618
TRUST_TOPIC = "/topic/TRAIN_MVT_ALL_TOC"

# Collect window: connect, gather messages for this many seconds, then disconnect.
COLLECT_SECONDS = 15

# STANOX codes for major London-area stations and junctions.
# Messages with these as reporting STANOX are London-relevant.
LONDON_STANOX = {
    # Terminals
    "87701",  # Waterloo
    "87100",  # Victoria
    "87141",  # London Bridge
    "86501",  # Liverpool Street
    "81800",  # Paddington
    "78600",  # Euston
    "78401",  # King's Cross
    "78400",  # St Pancras International
    "87800",  # Charing Cross
    "87200",  # Cannon Street
    "86600",  # Fenchurch Street
    "82300",  # Marylebone
    "87500",  # Blackfriars
    "87501",  # City Thameslink
    "87504",  # Farringdon
    # Key junctions
    "87703",  # Clapham Junction
    "87009",  # East Croydon
    "86400",  # Stratford
    "87710",  # Wimbledon
    "78301",  # Finsbury Park
    "81201",  # Ealing Broadway
}

# STANOX → (name, lat, lon) for geo-located observations.
STANOX_GEO: dict[str, tuple[str, float, float]] = {
    "87701": ("Waterloo", 51.5031, -0.1132),
    "87100": ("Victoria", 51.4952, -0.1439),
    "87141": ("London Bridge", 51.5052, -0.0861),
    "86501": ("Liverpool Street", 51.5178, -0.0823),
    "81800": ("Paddington", 51.5154, -0.1755),
    "78600": ("Euston", 51.5282, -0.1337),
    "78401": ("King's Cross", 51.5320, -0.1240),
    "78400": ("St Pancras Intl", 51.5322, -0.1260),
    "87800": ("Charing Cross", 51.5083, -0.1245),
    "87200": ("Cannon Street", 51.5113, -0.0903),
    "86600": ("Fenchurch Street", 51.5115, -0.0786),
    "82300": ("Marylebone", 51.5225, -0.1631),
    "87500": ("Blackfriars", 51.5119, -0.1037),
    "87501": ("City Thameslink", 51.5147, -0.1037),
    "87504": ("Farringdon", 51.5203, -0.1050),
    "87703": ("Clapham Junction", 51.4641, -0.1703),
    "87009": ("East Croydon", 51.3753, -0.0927),
    "86400": ("Stratford", 51.5416, -0.0033),
    "87710": ("Wimbledon", 51.4213, -0.2063),
    "78301": ("Finsbury Park", 51.5642, -0.1065),
    "81201": ("Ealing Broadway", 51.5150, -0.3015),
}

# TRUST message type codes.
MSG_TYPE_ACTIVATION = "0001"
MSG_TYPE_CANCELLATION = "0002"
MSG_TYPE_MOVEMENT = "0003"
MSG_TYPE_REINSTATEMENT = "0005"

# Variation status codes for movements.
VARIATION_STATUS = {
    "EARLY": "early",
    "ON TIME": "on_time",
    "LATE": "late",
    "OFF ROUTE": "off_route",
}

# Cancellation reason codes (subset — common infrastructure causes).
CANCEL_REASONS = {
    "AA": "Signalling apparatus failure",
    "AB": "Points failure",
    "AC": "Track circuit failure",
    "AD": "Level crossing failure",
    "AE": "Power supply failure",
    "AF": "Track defect",
    "AG": "Overhead line/third rail fault",
    "AX": "Traction supply fault",
    "FA": "Severe flooding",
    "FB": "Extreme heat",
    "FC": "Snow/ice",
    "FD": "Fog",
    "FE": "Storm damage",
    "IA": "Signal passed at danger",
    "IB": "Trespass/vandalism",
    "IC": "Fatality",
    "ID": "Security alert",
    "JA": "Congestion",
    "JB": "Crew shortage",
    "JC": "Train failure",
    "JD": "Rolling stock shortage",
    "OA": "AWS/TPWS fault",
    "OB": "Telecom equipment failure",
    "OC": "Power supply tripped",
    "VA": "Severe weather",
    "VB": "Lightning strike",
    "VC": "Object on line",
}


def _collect_messages_sync(user: str, password: str, seconds: float) -> list[dict]:
    """Connect to STOMP, collect TRUST messages for *seconds*, then disconnect.

    Runs in a worker thread (called via asyncio.to_thread).
    """
    try:
        import stomp
    except ImportError:
        log.error("stomp.py not installed — run: pip install 'stomp.py>=8.1'")
        return []

    collected: list[dict] = []
    lock = threading.Lock()
    connected_event = threading.Event()
    error_msg: list[str] = []

    class Listener(stomp.ConnectionListener):
        def on_message(self, frame):
            try:
                body = json.loads(frame.body)
                if isinstance(body, list):
                    with lock:
                        collected.extend(body)
                else:
                    with lock:
                        collected.append(body)
            except (json.JSONDecodeError, AttributeError):
                pass

        def on_connected(self, frame):
            connected_event.set()

        def on_error(self, frame):
            error_msg.append(str(getattr(frame, "body", frame)))

    conn = stomp.Connection(
        [(STOMP_HOST, STOMP_PORT)],
        heartbeats=(15000, 15000),
    )
    conn.set_listener("trust", Listener())

    try:
        conn.connect(user, password, wait=True, headers={"client-id": "london-ns"})
        if not connected_event.wait(timeout=10):
            log.warning("STOMP connect timeout")
            return []

        conn.subscribe(
            destination=TRUST_TOPIC,
            id="london-trust-sub",
            ack="auto",
            headers={"activemq.subscriptionName": "london-ns-trust"},
        )

        time.sleep(seconds)
    except Exception as exc:
        log.warning("STOMP connection error: %s", exc)
        return []
    finally:
        try:
            conn.disconnect()
        except Exception:
            pass

    if error_msg:
        log.warning("STOMP errors: %s", "; ".join(error_msg[:3]))

    with lock:
        return list(collected)


class NetworkRailTrustIngestor(BaseIngestor):
    source_name = "network_rail_trust"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: LondonGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self._user = os.environ.get("NETWORK_RAIL_USER", "")
        self._password = os.environ.get("NETWORK_RAIL_PASS", "")

    async def fetch_data(self) -> Any:
        if not self._user or not self._password:
            self.log.warning("NETWORK_RAIL_USER/PASS not set — skipping TRUST ingest")
            return None

        messages = await asyncio.to_thread(
            _collect_messages_sync, self._user, self._password, COLLECT_SECONDS
        )
        return messages if messages else None

    async def process(self, data: Any) -> None:
        messages: list[dict] = data
        cancellations = 0
        late_arrivals = 0
        movements_seen = 0
        activations = 0

        for envelope in messages:
            header = envelope.get("header", {})
            body = envelope.get("body", {})
            msg_type = header.get("msg_type", "")

            # Filter to London-area events.
            reporting_stanox = body.get("loc_stanox", "") or body.get("dep_stanox", "")
            if reporting_stanox not in LONDON_STANOX:
                continue

            geo = STANOX_GEO.get(reporting_stanox)
            if geo:
                name, lat, lon = geo
                cell_id = self.graph.latlon_to_cell(lat, lon)
            else:
                name, lat, lon, cell_id = reporting_stanox, None, None, None

            train_id = body.get("train_id", "unknown")

            if msg_type == MSG_TYPE_CANCELLATION:
                cancellations += 1
                reason_code = body.get("canc_reason_code", "")
                reason_text = CANCEL_REASONS.get(reason_code, f"code {reason_code}")
                cancel_type = body.get("canc_type", "")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=1.0,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "station": name,
                        "metric": "cancellation",
                        "train_id": train_id,
                        "reason_code": reason_code,
                        "reason_text": reason_text,
                        "cancel_type": cancel_type,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"TRUST [{name}] cancellation: {train_id} — {reason_text}"
                    ),
                    data={
                        "station": name,
                        "train_id": train_id,
                        "event": "cancellation",
                        "reason_code": reason_code,
                        "reason_text": reason_text,
                        "cancel_type": cancel_type,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)

            elif msg_type == MSG_TYPE_MOVEMENT:
                movements_seen += 1
                variation = body.get("variation_status", "")
                timetable_variation = body.get("timetable_variation", "0")

                try:
                    delay_mins = int(timetable_variation)
                except (ValueError, TypeError):
                    delay_mins = 0

                # Only record significantly late arrivals (>5 min).
                if variation == "LATE" and delay_mins > 5:
                    late_arrivals += 1

                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=float(delay_mins),
                        location_id=cell_id,
                        lat=lat,
                        lon=lon,
                        metadata={
                            "station": name,
                            "metric": "delay_minutes",
                            "train_id": train_id,
                            "variation_status": variation,
                        },
                    )
                    await self.board.store_observation(obs)

                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=(
                            f"TRUST [{name}] late: {train_id} +{delay_mins}min"
                        ),
                        data={
                            "station": name,
                            "train_id": train_id,
                            "event": "late_arrival",
                            "delay_minutes": delay_mins,
                            "lat": lat,
                            "lon": lon,
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)

            elif msg_type == MSG_TYPE_ACTIVATION:
                activations += 1

            elif msg_type == MSG_TYPE_REINSTATEMENT:
                train_id = body.get("train_id", "unknown")
                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=1.0,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "station": name,
                        "metric": "reinstatement",
                        "train_id": train_id,
                    },
                )
                await self.board.store_observation(obs)

        self.log.info(
            "TRUST: %d raw msgs, London-filtered: %d movements, "
            "%d cancellations, %d late (>5min), %d activations",
            len(messages), movements_seen, cancellations, late_arrivals, activations,
        )


async def ingest_network_rail_trust(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TRUST collection cycle."""
    ingestor = NetworkRailTrustIngestor(board, graph, scheduler)
    await ingestor.run()
