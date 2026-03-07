"""TfL Scheduled Works ingestor — forward-looking engineering works and planned closures.

Queries TfL's date-range line status endpoint to fetch planned closures,
engineering works, and testing schedules for the next 7 days across all
rail/metro modes. Unlike planned_works.py (which queries current status only),
this ingestor provides advance notice of upcoming infrastructure events,
allowing downstream agents to differentiate pre-planned high-impact events
from emergent anomalies.

API: https://api.tfl.gov.uk/Line/{ids}/Status/{startDate}/to/{endDate}
No API key required (public TfL Unified API).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.tfl_scheduled_works")

# All TfL rail/metro line IDs
_LINE_IDS = (
    "bakerloo,central,circle,district,hammersmith-city,"
    "jubilee,metropolitan,northern,piccadilly,victoria,"
    "waterloo-city,elizabeth,dlr,london-overground,tram"
)

# Look-ahead window
_LOOKAHEAD_DAYS = 7

# TfL status severity codes indicating planned/scheduled activity
_PLANNED_SEVERITIES = {
    0,   # Special service
    1,   # Closed
    4,   # Planned closure
    5,   # Part closure
    8,   # Bus service (replacement during closure)
}

# Keywords in reason text that signal engineering/testing activity
_ENGINEERING_KEYWORDS = [
    "engineering work",
    "planned closure",
    "weekend closure",
    "improvement work",
    "upgrade work",
    "maintenance",
    "testing",
    "signal",
    "track work",
    "infrastructure",
    "renewal",
    "modernisation",
    "commissioning",
    "trial running",
    "stress test",
]


class TflScheduledWorksIngestor(BaseIngestor):
    source_name = "tfl_scheduled_works"
    rate_limit_name = "tfl"

    def _build_url(self) -> str:
        now = datetime.now(tz=timezone.utc)
        start = now + timedelta(hours=1)
        end = now + timedelta(days=_LOOKAHEAD_DAYS)
        start_str = start.strftime("%Y-%m-%dT%H:%M")
        end_str = end.strftime("%Y-%m-%dT%H:%M")
        return (
            f"https://api.tfl.gov.uk/Line/{_LINE_IDS}"
            f"/Status/{start_str}/to/{end_str}?detail=true"
        )

    async def fetch_data(self) -> Any:
        url = self._build_url()
        return await self.fetch(url)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected data format: %s", type(data))
            return

        scheduled_count = 0

        for line in data:
            line_id = line.get("id", "unknown")
            line_name = line.get("name", line_id)
            mode = line.get("modeName", "unknown")

            for status in line.get("lineStatuses", []):
                severity = status.get("statusSeverity", 10)
                status_desc = status.get("statusSeverityDescription", "")
                reason = status.get("reason", "")
                disruption = status.get("disruption", {}) or {}
                category_desc = disruption.get("categoryDescription", "")

                # Only interested in planned/scheduled items
                is_planned = severity in _PLANNED_SEVERITIES
                reason_lower = reason.lower()
                has_engineering_keyword = any(
                    kw in reason_lower for kw in _ENGINEERING_KEYWORDS
                )
                is_planned_category = category_desc in {
                    "PlannedWork", "PlannedClosure",
                }

                if not (is_planned or has_engineering_keyword or is_planned_category):
                    continue

                # Skip "good service" entries that somehow passed
                if severity >= 10:
                    continue

                # Extract validity windows
                validity_periods = []
                for vp in status.get("validityPeriods", []):
                    from_date = vp.get("fromDate", "")
                    to_date = vp.get("toDate", "")
                    if from_date:
                        validity_periods.append({
                            "from": from_date,
                            "to": to_date,
                        })

                # Extract affected sections
                affected_sections = []
                for route in disruption.get("affectedRoutes", []):
                    for section in route.get("routeSections", []):
                        orig = section.get("originator", "")
                        dest = section.get("destination", "")
                        if orig and dest:
                            affected_sections.append(f"{orig} to {dest}")

                affected_stops = [
                    s.get("commonName", "")
                    for s in disruption.get("affectedStops", [])
                    if s.get("commonName")
                ]

                # Classify the type of scheduled work
                work_type = "planned_closure"
                if has_engineering_keyword:
                    for kw in ("testing", "trial running", "stress test", "commissioning"):
                        if kw in reason_lower:
                            work_type = "testing_schedule"
                            break
                    else:
                        if any(kw in reason_lower for kw in ("upgrade", "modernisation", "renewal")):
                            work_type = "upgrade_works"
                        elif any(kw in reason_lower for kw in ("signal", "track work", "infrastructure")):
                            work_type = "engineering_works"
                        elif "maintenance" in reason_lower:
                            work_type = "maintenance"

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=severity,
                    location_id=None,
                    lat=None,
                    lon=None,
                    metadata={
                        "line_id": line_id,
                        "line_name": line_name,
                        "mode": mode,
                        "status": status_desc,
                        "status_severity": severity,
                        "reason": reason[:500],
                        "work_type": work_type,
                        "is_future": True,
                        "validity_periods": validity_periods[:5],
                        "affected_sections": affected_sections[:10],
                        "affected_stops": affected_stops[:20],
                        "closure_text": disruption.get("closureText", ""),
                        "category": category_desc,
                    },
                )
                await self.board.store_observation(obs)

                # Build readable time window
                time_info = ""
                if validity_periods:
                    vp = validity_periods[0]
                    time_info = f" ({vp['from'][:16]} → {vp['to'][:16]})"

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Scheduled {work_type.replace('_', ' ')} [{line_name}] "
                        f"{status_desc}{time_info}"
                        + (f" — {reason[:200]}" if reason else "")
                    ),
                    data={
                        "line_id": line_id,
                        "line_name": line_name,
                        "mode": mode,
                        "status": status_desc,
                        "work_type": work_type,
                        "is_future": True,
                        "validity_periods": validity_periods[:3],
                        "observation_id": obs.id,
                    },
                    location_id=None,
                )
                await self.board.post(msg)
                scheduled_count += 1

        self.log.info(
            "TfL scheduled works: %d upcoming entries for next %d days",
            scheduled_count, _LOOKAHEAD_DAYS,
        )


async def ingest_tfl_scheduled_works(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL scheduled works fetch cycle."""
    ingestor = TflScheduledWorksIngestor(board, graph, scheduler)
    await ingestor.run()
