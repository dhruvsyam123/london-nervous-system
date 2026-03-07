"""NTP server health ingestor — monitors UK time servers for sync issues.

Queries well-known UK NTP servers via the NTP protocol (UDP port 123)
and reports offset, delay, stratum, and reachability. Useful for detecting
timestamp-related data quality issues across other ingestors.

No API key required — uses raw NTP protocol queries.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.ntp_health")

# Well-known UK and global NTP servers relevant to London infrastructure
NTP_SERVERS = [
    ("0.uk.pool.ntp.org", "UK NTP Pool 0"),
    ("1.uk.pool.ntp.org", "UK NTP Pool 1"),
    ("2.uk.pool.ntp.org", "UK NTP Pool 2"),
    ("ntp.ubuntu.com", "Ubuntu NTP (common Linux default)"),
    ("time.cloudflare.com", "Cloudflare Time"),
    ("time.google.com", "Google Time"),
]

# NTP epoch offset: seconds between 1900-01-01 and 1970-01-01
_NTP_EPOCH_OFFSET = 2208988800

# Thresholds
_HIGH_OFFSET_MS = 100.0       # flag offsets > 100ms
_HIGH_DELAY_MS = 500.0        # flag delays > 500ms
_QUERY_TIMEOUT_S = 5.0        # per-server timeout

# London centre for location tagging
_LONDON_LAT = 51.5074
_LONDON_LON = -0.1278


def _build_ntp_packet() -> bytes:
    """Build a minimal NTPv4 client request packet (48 bytes)."""
    # LI=0, VN=4, Mode=3 (client) → first byte = 0b00_100_011 = 0x23
    packet = bytearray(48)
    packet[0] = 0x23
    # Set transmit timestamp (bytes 40-47) to current time
    now = time.time() + _NTP_EPOCH_OFFSET
    secs = int(now)
    frac = int((now - secs) * (2**32))
    struct.pack_into("!II", packet, 40, secs, frac)
    return bytes(packet)


def _parse_ntp_response(data: bytes, t_sent: float, t_recv: float) -> dict[str, Any]:
    """Parse NTP response and compute offset/delay."""
    if len(data) < 48:
        raise ValueError(f"NTP response too short: {len(data)} bytes")

    # Unpack fields
    li_vn_mode = data[0]
    stratum = data[1]
    poll = data[2]
    precision = struct.unpack("!b", data[3:4])[0]

    # Reference, origin, receive, transmit timestamps (NTP epoch)
    def _ts(offset: int) -> float:
        secs, frac = struct.unpack("!II", data[offset : offset + 8])
        return secs - _NTP_EPOCH_OFFSET + frac / (2**32)

    t1 = t_sent                  # client send time
    t2 = _ts(32)                 # server receive time
    t3 = _ts(40)                 # server transmit time
    t4 = t_recv                  # client receive time

    # RFC 5905 offset and delay
    offset = ((t2 - t1) + (t3 - t4)) / 2.0
    delay = (t4 - t1) - (t3 - t2)

    return {
        "stratum": stratum,
        "poll_interval": poll,
        "precision": precision,
        "offset_s": offset,
        "offset_ms": offset * 1000.0,
        "delay_s": delay,
        "delay_ms": delay * 1000.0,
        "leap_indicator": (li_vn_mode >> 6) & 0x3,
    }


async def _query_ntp_server(host: str, timeout: float = _QUERY_TIMEOUT_S) -> dict[str, Any] | None:
    """Query a single NTP server. Returns parsed result or None on failure."""
    loop = asyncio.get_event_loop()
    try:
        # Resolve hostname
        infos = await loop.getaddrinfo(host, 123, type=socket.SOCK_DGRAM)
        if not infos:
            return None
        family, stype, proto, _, addr = infos[0]

        sock = socket.socket(family, socket.SOCK_DGRAM)
        sock.setblocking(False)
        sock.settimeout(0)

        packet = _build_ntp_packet()
        t_sent = time.time()
        await asyncio.wait_for(
            loop.sock_sendto(sock, packet, addr),
            timeout=timeout,
        )
        data = await asyncio.wait_for(
            loop.sock_recv(sock, 1024),
            timeout=timeout,
        )
        t_recv = time.time()
        sock.close()

        result = _parse_ntp_response(data, t_sent, t_recv)
        result["reachable"] = True
        result["resolved_ip"] = addr[0]
        return result
    except Exception as exc:
        log.debug("NTP query to %s failed: %s", host, exc)
        return None


class NtpHealthIngestor(BaseIngestor):
    source_name = "ntp_health"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Query all NTP servers concurrently."""
        tasks = [_query_ntp_server(host) for host, _ in NTP_SERVERS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        server_results = []
        for (host, label), result in zip(NTP_SERVERS, results):
            if isinstance(result, Exception):
                server_results.append({"host": host, "label": label, "reachable": False})
            elif result is None:
                server_results.append({"host": host, "label": label, "reachable": False})
            else:
                result["host"] = host
                result["label"] = label
                server_results.append(result)

        return server_results

    async def process(self, data: Any) -> None:
        reachable = 0
        unreachable = 0
        high_offset = 0
        offsets = []

        for srv in data:
            host = srv["host"]
            label = srv["label"]

            if not srv.get("reachable", False):
                unreachable += 1
                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=0.0,
                    lat=_LONDON_LAT,
                    lon=_LONDON_LON,
                    location_id=self.graph.latlon_to_cell(_LONDON_LAT, _LONDON_LON),
                    metadata={
                        "server": host,
                        "label": label,
                        "reachable": False,
                        "metric": "ntp_reachable",
                    },
                )
                await self.board.store_observation(obs)
                await self.board.post(AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"NTP server UNREACHABLE: {label} ({host})",
                    data={"server": host, "label": label, "reachable": False},
                    location_id=obs.location_id,
                ))
                continue

            reachable += 1
            offset_ms = srv["offset_ms"]
            delay_ms = srv["delay_ms"]
            stratum = srv["stratum"]
            offsets.append(offset_ms)

            cell_id = self.graph.latlon_to_cell(_LONDON_LAT, _LONDON_LON)

            # Store offset observation
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=offset_ms,
                lat=_LONDON_LAT,
                lon=_LONDON_LON,
                location_id=cell_id,
                metadata={
                    "server": host,
                    "label": label,
                    "reachable": True,
                    "metric": "ntp_offset_ms",
                    "delay_ms": round(delay_ms, 2),
                    "stratum": stratum,
                    "leap_indicator": srv.get("leap_indicator", 0),
                },
            )
            await self.board.store_observation(obs)

            is_anomalous = abs(offset_ms) > _HIGH_OFFSET_MS or delay_ms > _HIGH_DELAY_MS
            if is_anomalous:
                high_offset += 1

            status = "DRIFT" if abs(offset_ms) > _HIGH_OFFSET_MS else "OK"
            if delay_ms > _HIGH_DELAY_MS:
                status = "HIGH_DELAY"

            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"NTP [{label}] offset={offset_ms:+.1f}ms "
                    f"delay={delay_ms:.1f}ms stratum={stratum} [{status}]"
                ),
                data={
                    "server": host,
                    "label": label,
                    "offset_ms": round(offset_ms, 2),
                    "delay_ms": round(delay_ms, 2),
                    "stratum": stratum,
                    "status": status,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            ))

        # Summary
        avg_offset = sum(offsets) / len(offsets) if offsets else 0.0
        self.log.info(
            "NTP health: reachable=%d unreachable=%d high_offset=%d avg_offset=%.1fms",
            reachable, unreachable, high_offset, avg_offset,
        )


async def ingest_ntp_health(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one NTP health check cycle."""
    ingestor = NtpHealthIngestor(board, graph, scheduler)
    await ingestor.run()
