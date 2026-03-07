"""TfL Bus Depot Health ingestor — detects depot-level failures via route status aggregation.

TfL does not expose internal depot operational data publicly. This ingestor uses
the public Line Status API as a proxy: by mapping bus routes to their operating
depots and aggregating disruption counts, it can distinguish between isolated
on-route incidents and systemic depot-level failures (e.g., vehicle shortages,
maintenance backlogs, staffing issues) where many routes from one depot degrade
simultaneously.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.bus_depot_health")

TFL_BUS_STATUS_URL = "https://api.tfl.gov.uk/Line/Mode/bus/Status"

# Mapping of major London bus depots → representative routes operated from each.
# Source: TfL route allocation data. Not exhaustive — covers ~120 high-frequency routes.
DEPOT_ROUTES: dict[str, dict[str, Any]] = {
    "camberwell": {
        "routes": {"12", "35", "36", "40", "42", "45", "68", "171", "176", "185", "345", "484"},
        "lat": 51.4740, "lon": -0.0935,
    },
    "holloway": {
        "routes": {"17", "29", "43", "91", "153", "234", "253", "259", "271", "390", "W7"},
        "lat": 51.5560, "lon": -0.1180,
    },
    "stockwell": {
        "routes": {"2", "88", "155", "196", "333", "415", "432", "G1"},
        "lat": 51.4720, "lon": -0.1230,
    },
    "battersea": {
        "routes": {"19", "22", "44", "137", "156", "170", "319", "344", "452"},
        "lat": 51.4690, "lon": -0.1710,
    },
    "clapton": {
        "routes": {"38", "48", "55", "56", "106", "254", "393", "488", "D6"},
        "lat": 51.5580, "lon": -0.0500,
    },
    "putney": {
        "routes": {"14", "74", "85", "93", "265", "270", "337", "424", "430"},
        "lat": 51.4610, "lon": -0.2170,
    },
    "brixton": {
        "routes": {"3", "59", "109", "118", "133", "159", "250", "322", "417"},
        "lat": 51.4630, "lon": -0.1140,
    },
    "ash_grove": {
        "routes": {"26", "30", "48", "55", "205", "254", "388", "394", "D6"},
        "lat": 51.5340, "lon": -0.0570,
    },
    "west_ham": {
        "routes": {"25", "86", "104", "115", "147", "241", "262", "276", "300", "330"},
        "lat": 51.5280, "lon": 0.0050,
    },
    "lea_interchange": {
        "routes": {"20", "69", "97", "158", "212", "257", "308", "339", "W19"},
        "lat": 51.5580, "lon": -0.0200,
    },
    "croydon": {
        "routes": {"60", "64", "109", "119", "130", "157", "197", "198", "312", "403", "410"},
        "lat": 51.3760, "lon": -0.0990,
    },
    "plough_lane": {
        "routes": {"57", "77", "131", "152", "200", "219", "264", "280", "493"},
        "lat": 51.4290, "lon": -0.1870,
    },
    "northumberland_park": {
        "routes": {"41", "76", "123", "149", "192", "243", "318", "341", "444"},
        "lat": 51.6040, "lon": -0.0650,
    },
    "edmonton": {
        "routes": {"34", "102", "144", "191", "217", "231", "279", "349", "W6"},
        "lat": 51.6150, "lon": -0.0700,
    },
}

# Reverse lookup: route → depot
_ROUTE_TO_DEPOT: dict[str, str] = {}
for _depot, _info in DEPOT_ROUTES.items():
    for _route in _info["routes"]:
        _ROUTE_TO_DEPOT[_route] = _depot

# TfL status severity: 10 = Good Service, lower = worse
_GOOD_SERVICE_SEVERITY = 10


class BusDepotHealthIngestor(BaseIngestor):
    source_name = "bus_depot_health"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_BUS_STATUS_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected bus status response type: %s", type(data))
            return

        # Collect disrupted routes per depot
        depot_disrupted: dict[str, list[dict]] = {d: [] for d in DEPOT_ROUTES}
        depot_total: dict[str, int] = {d: 0 for d in DEPOT_ROUTES}
        unmapped_disrupted = 0
        total_lines = 0

        for line in data:
            line_id = line.get("id", "")
            total_lines += 1
            depot = _ROUTE_TO_DEPOT.get(line_id)
            if not depot:
                # Route not in our depot mapping — still count disruptions
                statuses = line.get("lineStatuses", [])
                for s in statuses:
                    if s.get("statusSeverity", 10) < _GOOD_SERVICE_SEVERITY:
                        unmapped_disrupted += 1
                        break
                continue

            depot_total[depot] = depot_total.get(depot, 0) + 1
            statuses = line.get("lineStatuses", [])
            for s in statuses:
                severity = s.get("statusSeverity", 10)
                if severity < _GOOD_SERVICE_SEVERITY:
                    depot_disrupted[depot].append({
                        "route": line_id,
                        "severity": severity,
                        "description": s.get("statusSeverityDescription", ""),
                        "reason": (s.get("reason") or "")[:200],
                    })
                    break  # one disruption status per line is enough

        # Emit per-depot health observations
        observations = 0
        for depot_name, info in DEPOT_ROUTES.items():
            disrupted = depot_disrupted[depot_name]
            total = depot_total[depot_name]
            if total == 0:
                continue

            disrupted_count = len(disrupted)
            health_pct = round(100 * (1 - disrupted_count / total), 1)
            lat, lon = info["lat"], info["lon"]
            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=health_pct,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "depot": depot_name,
                    "metric": "depot_health_pct",
                    "total_routes": total,
                    "disrupted_routes": disrupted_count,
                    "disrupted_details": disrupted[:5],  # cap detail size
                },
            )
            await self.board.store_observation(obs)

            # Build a concise summary
            if disrupted_count == 0:
                summary = f"Depot [{depot_name}] health=100% ({total} routes all good)"
            else:
                route_list = ", ".join(d["route"] for d in disrupted[:5])
                summary = (
                    f"Depot [{depot_name}] health={health_pct}% "
                    f"({disrupted_count}/{total} routes disrupted: {route_list})"
                )

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=summary,
                data={
                    "depot": depot_name,
                    "health_pct": health_pct,
                    "total_routes": total,
                    "disrupted_count": disrupted_count,
                    "disrupted_routes": [d["route"] for d in disrupted],
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            observations += 1

        self.log.info(
            "Bus depot health: depots=%d total_lines=%d unmapped_disrupted=%d observations=%d",
            len(DEPOT_ROUTES), total_lines, unmapped_disrupted, observations,
        )


async def ingest_bus_depot_health(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one bus depot health fetch cycle."""
    ingestor = BusDepotHealthIngestor(board, graph, scheduler)
    await ingestor.run()
