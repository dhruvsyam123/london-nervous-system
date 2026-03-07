"""TfL Bus Disruption Incidents ingestor — per-incident cause detail for bus fleet.

Fetches active bus disruptions from TfL's Line Disruption API, providing
per-incident data with cause extraction from free-text descriptions. Extracts
structured cause types (mechanical failure, passenger action, diversion, etc.)
that enable downstream agents to ground transport anomaly analysis in
operational reality.

Fills the bus gap in tfl_disruption_incidents.py (which covers tube/dlr/rail only).
No API key required.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.tfl_bus_disruptions")

# TfL Line Disruption endpoint for buses — no API key required
TFL_BUS_DISRUPTION_URL = "https://api.tfl.gov.uk/Line/Mode/bus/Disruption"

# Also fetch stop-level disruptions (closures, access issues)
TFL_BUS_STOP_DISRUPTION_URL = "https://api.tfl.gov.uk/StopPoint/Mode/bus/Disruption"

# Bus-specific cause patterns extracted from free-text descriptions
_BUS_CAUSE_PATTERNS = [
    (re.compile(r"mechanic(?:al)?\s+(?:failure|fault|problem|breakdown)", re.I), "mechanical_failure"),
    (re.compile(r"(?:vehicle|bus)\s+(?:breakdown|fault|failure)", re.I), "mechanical_failure"),
    (re.compile(r"(?:broke(?:n)?\s+down|broken\s+down\s+(?:bus|vehicle))", re.I), "mechanical_failure"),
    (re.compile(r"passenger\s+(?:action|incident|emergency)", re.I), "passenger_action"),
    (re.compile(r"diversion\s+(?:instructed|in\s+place|due\s+to)", re.I), "diversion"),
    (re.compile(r"divert(?:ed|ing)", re.I), "diversion"),
    (re.compile(r"road\s*works?", re.I), "roadworks"),
    (re.compile(r"(?:road|street)\s+closure", re.I), "road_closure"),
    (re.compile(r"(?:traffic\s+(?:congestion|incident|accident|collision))", re.I), "traffic_incident"),
    (re.compile(r"(?:accident|collision|crash|RTC)", re.I), "traffic_incident"),
    (re.compile(r"police\s+(?:incident|action|closure|investigation)", re.I), "police_incident"),
    (re.compile(r"emergency\s+(?:services|roadworks|road\s*works)", re.I), "emergency_services"),
    (re.compile(r"staff\s+(?:shortage|unavailab)", re.I), "staff_shortage"),
    (re.compile(r"driver\s+(?:shortage|unavailab)", re.I), "staff_shortage"),
    (re.compile(r"(?:fire\s+alert|fire\s+alarm|fire\b)", re.I), "fire_alert"),
    (re.compile(r"flood", re.I), "flooding"),
    (re.compile(r"(?:burst|water)\s+(?:main|pipe)", re.I), "burst_main"),
    (re.compile(r"gas\s+(?:leak|main|emergency)", re.I), "gas_leak"),
    (re.compile(r"(?:power\s+(?:failure|cut|outage)|electricity)", re.I), "power_failure"),
    (re.compile(r"(?:event|march|protest|demonstration|parade|carnival)", re.I), "public_event"),
    (re.compile(r"(?:security\s+(?:alert|incident))", re.I), "security_alert"),
    (re.compile(r"(?:planned\s+closure|planned\s+(?:re)?route)", re.I), "planned_closure"),
    (re.compile(r"(?:bridge|tunnel)\s+(?:closure|closed|opening|open)", re.I), "bridge_closure"),
    (re.compile(r"overcrowding", re.I), "overcrowding"),
    (re.compile(r"curtail", re.I), "curtailment"),
]

_CATEGORY_SEVERITY = {
    "RealTime": 4,
    "PlannedWork": 2,
    "Information": 1,
}


def _extract_bus_cause(description: str) -> str:
    """Extract structured cause type from free-text bus disruption description."""
    for pattern, cause in _BUS_CAUSE_PATTERNS:
        if pattern.search(description):
            return cause
    return "unknown"


def _extract_route_ids(description: str) -> list[str]:
    """Extract bus route numbers mentioned in description text."""
    # Match patterns like "route 38", "bus 73", standalone route numbers near bus context
    routes: list[str] = []
    for m in re.finditer(r"(?:route|bus|line)\s+(\d{1,3}\w?)", description, re.I):
        routes.append(m.group(1))
    return list(dict.fromkeys(routes))[:20]


class TflBusDisruptionIngestor(BaseIngestor):
    source_name = "tfl_bus_disruptions"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        line_data = await self.fetch(TFL_BUS_DISRUPTION_URL)
        stop_data = await self.fetch(TFL_BUS_STOP_DISRUPTION_URL)
        return {"line": line_data, "stop": stop_data}

    async def process(self, data: Any) -> None:
        line_incidents = data.get("line") or []
        stop_incidents = data.get("stop") or []

        if not isinstance(line_incidents, list):
            line_incidents = []
        if not isinstance(stop_incidents, list):
            stop_incidents = []

        processed = 0

        # Process line-level bus disruptions
        for incident in line_incidents:
            category = incident.get("category", "")
            category_desc = incident.get("categoryDescription", category)
            description = incident.get("description", "")
            closure_text = incident.get("closureText", "")
            disruption_type = incident.get("type", "")

            severity_score = _CATEGORY_SEVERITY.get(category, 3)
            cause = _extract_bus_cause(description)
            mentioned_routes = _extract_route_ids(description)

            # Extract affected routes/stops from structured fields
            affected_routes = []
            affected_stops = []
            for route in incident.get("affectedRoutes", []) or []:
                for section in route.get("routeSections", []) or []:
                    orig = section.get("originator", "")
                    dest = section.get("destination", "")
                    if orig and dest:
                        affected_routes.append(f"{orig} to {dest}")

            for stop in incident.get("affectedStops", []) or []:
                name = stop.get("commonName", "")
                if name:
                    affected_stops.append(name)

            is_realtime = category == "RealTime"

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=severity_score,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "incident_level": "line",
                    "category": category,
                    "category_description": category_desc,
                    "disruption_type": disruption_type,
                    "cause": cause,
                    "closure_text": closure_text,
                    "description": description[:500],
                    "is_realtime": is_realtime,
                    "mentioned_routes": mentioned_routes[:10],
                    "affected_routes": affected_routes[:10],
                    "affected_stops": affected_stops[:20],
                },
            )
            await self.board.store_observation(obs)

            tag = " [REALTIME]" if is_realtime else " [PLANNED]"
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Bus disruption{tag} cause={cause} "
                    f"type={disruption_type} severity={severity_score}"
                    + (f" routes={','.join(mentioned_routes[:5])}" if mentioned_routes else "")
                    + (f" — {description[:200]}" if description else "")
                ),
                data={
                    "incident_level": "line",
                    "category": category,
                    "cause": cause,
                    "disruption_type": disruption_type,
                    "severity_score": severity_score,
                    "is_realtime": is_realtime,
                    "mentioned_routes": mentioned_routes[:10],
                    "affected_stops": affected_stops[:10],
                    "observation_id": obs.id,
                },
                location_id=None,
            )
            await self.board.post(msg)
            processed += 1

        # Process stop-level bus disruptions (closures, temporary stops)
        stop_processed = 0
        for stop_inc in stop_incidents:
            description = stop_inc.get("description", "")
            stop_name = stop_inc.get("commonName", "")
            appearance = stop_inc.get("appearance", "")
            from_date = stop_inc.get("fromDate", "")
            to_date = stop_inc.get("toDate", "")

            cause = _extract_bus_cause(description)
            lat = stop_inc.get("lat")
            lon = stop_inc.get("lon")
            cell_id = None
            if lat and lon:
                try:
                    cell_id = self.graph.latlon_to_cell(float(lat), float(lon))
                except (ValueError, TypeError):
                    pass

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=3,  # stop disruptions are moderate severity
                location_id=cell_id,
                lat=float(lat) if lat else None,
                lon=float(lon) if lon else None,
                metadata={
                    "incident_level": "stop",
                    "stop_name": stop_name,
                    "cause": cause,
                    "appearance": appearance,
                    "description": description[:500],
                    "from_date": from_date,
                    "to_date": to_date,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Bus stop disruption: {stop_name} cause={cause}"
                    + (f" — {description[:200]}" if description else "")
                ),
                data={
                    "incident_level": "stop",
                    "stop_name": stop_name,
                    "cause": cause,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            stop_processed += 1

        self.log.info(
            "TfL bus disruptions: line_incidents=%d stop_incidents=%d",
            processed, stop_processed,
        )


async def ingest_tfl_bus_disruptions(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL bus disruption fetch cycle."""
    ingestor = TflBusDisruptionIngestor(board, graph, scheduler)
    await ingestor.run()
