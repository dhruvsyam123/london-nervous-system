"""RIPE Atlas internet health ingestor — probe connectivity and status across London.

Queries the RIPE Atlas public API for probes within Greater London's bounding box.
Tracks probe status (connected/disconnected), ASN, and uptime to help correlate
data source outages with underlying internet connectivity issues.
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

log = logging.getLogger("london.ingestors.ripe_atlas")

# RIPE Atlas v2 API — probes endpoint with London bounding box
# Greater London bbox: roughly 51.28°N–51.69°N, -0.51°W–0.33°E
PROBES_URL = "https://atlas.ripe.net/api/v2/probes/"
LONDON_BBOX = "51.28,-0.51,51.69,0.33"  # south,west,north,east

# Status ID mapping from RIPE Atlas API
STATUS_MAP = {
    1: "connected",
    2: "disconnected",
    3: "abandoned",
    4: "never_connected",
}


class RipeAtlasIngestor(BaseIngestor):
    source_name = "ripe_atlas"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("RIPE_ATLAS_API_KEY", "")
        headers = {"Authorization": f"Key {api_key}"} if api_key else None
        params = {
            "format": "json",
            "status_name": "Connected,Disconnected",
            "sort": "id",
            "page_size": "500",
        }
        return await self.fetch(
            PROBES_URL,
            params=params,
            headers=headers,
        )

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected RIPE Atlas response type: %s", type(data))
            return

        results = data.get("results", [])
        if not results:
            self.log.info("No RIPE Atlas probes returned")
            return

        processed = 0
        skipped = 0
        connected = 0
        disconnected = 0

        for probe in results:
            geometry = probe.get("geometry")
            if not geometry or geometry.get("type") != "Point":
                skipped += 1
                continue

            coords = geometry.get("coordinates", [])
            if len(coords) < 2:
                skipped += 1
                continue

            lon, lat = coords[0], coords[1]

            # Filter to London bbox (API may return slightly outside)
            if not (51.28 <= lat <= 51.69 and -0.51 <= lon <= 0.33):
                skipped += 1
                continue

            probe_id = probe.get("id")
            status_id = probe.get("status", {}).get("id", 0)
            status_name = STATUS_MAP.get(status_id, "unknown")
            asn_v4 = probe.get("asn_v4")
            is_connected = 1.0 if status_id == 1 else 0.0

            if is_connected:
                connected += 1
            else:
                disconnected += 1

            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=is_connected,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "probe_id": probe_id,
                    "status": status_name,
                    "asn_v4": asn_v4,
                    "address_v4": probe.get("address_v4"),
                },
            )
            await self.board.store_observation(obs)
            processed += 1

        # Post a single summary message to #raw
        total = connected + disconnected
        pct = (connected / total * 100) if total else 0
        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"RIPE Atlas London: {total} probes, "
                f"{connected} connected ({pct:.0f}%), "
                f"{disconnected} disconnected"
            ),
            data={
                "total_probes": total,
                "connected": connected,
                "disconnected": disconnected,
                "connectivity_pct": round(pct, 1),
                "skipped": skipped,
            },
        )
        await self.board.post(msg)
        self.log.info(
            "RIPE Atlas: processed=%d connected=%d disconnected=%d skipped=%d",
            processed, connected, disconnected, skipped,
        )


async def ingest_ripe_atlas(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one RIPE Atlas fetch cycle."""
    ingestor = RipeAtlasIngestor(board, graph, scheduler)
    await ingestor.run()
