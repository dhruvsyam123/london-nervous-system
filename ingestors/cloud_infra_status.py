"""Cloud infrastructure status ingestor — AWS, GCP, Azure for London region.

Monitors public status pages of major cloud providers covering the London region
(AWS eu-west-2, GCP europe-west2, Azure UK South/UK West). Helps differentiate
application-level failures from underlying infrastructure outages.

All endpoints are public and free — no API keys required.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.cloud_infra_status")

# Public endpoints
_AWS_EVENTS_URL = "https://health.aws.amazon.com/public/currentevents"
_GCP_INCIDENTS_URL = "https://status.cloud.google.com/incidents.json"
_AZURE_RSS_URL = "https://azure.status.microsoft/en-us/status/feed/"

# Region keywords to filter London-relevant events
_AWS_LONDON_REGIONS = {"eu-west-2", "europe", "global"}
_GCP_LONDON_REGIONS = {"europe-west2", "europe", "global", "multi-region"}
_AZURE_LONDON_KEYWORDS = {"uk south", "uk west", "europe", "global"}


class CloudInfraStatusIngestor(BaseIngestor):
    source_name = "cloud_infra_status"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Fetch status from all three cloud providers."""
        results: dict[str, Any] = {}

        # AWS
        aws_data = await self.fetch(_AWS_EVENTS_URL)
        results["aws"] = aws_data

        # GCP
        gcp_data = await self.fetch(_GCP_INCIDENTS_URL)
        results["gcp"] = gcp_data

        # Azure RSS
        azure_text = await self.fetch(_AZURE_RSS_URL)
        results["azure"] = azure_text

        return results

    async def process(self, data: Any) -> None:
        aws_events = self._parse_aws(data.get("aws"))
        gcp_events = self._parse_gcp(data.get("gcp"))
        azure_events = self._parse_azure(data.get("azure"))

        all_events = aws_events + gcp_events + azure_events
        active_count = sum(1 for e in all_events if e.get("active"))

        # Overall cloud infra health observation
        health = 0.0 if active_count > 0 else 1.0
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=health,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_infra_health",
                "active_incidents": active_count,
                "total_events": len(all_events),
                "providers_checked": ["aws", "gcp", "azure"],
            },
        )
        await self.board.store_observation(obs)

        summary = (
            f"Cloud infra status: {active_count} active London-region incidents "
            f"across AWS/GCP/Azure ({len(all_events)} total events)"
        )
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=summary,
            data={
                "metric": "cloud_infra_health",
                "health": health,
                "active_incidents": active_count,
                "observation_id": obs.id,
            },
        ))

        # Post individual active incidents
        for event in all_events:
            if not event.get("active"):
                continue
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=event["summary"][:500],
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "cloud_incident",
                    "provider": event["provider"],
                    "service": event.get("service", "unknown"),
                    "region": event.get("region", "unknown"),
                    "severity": event.get("severity", "unknown"),
                    "started": event.get("started"),
                },
            )
            await self.board.store_observation(obs)
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Cloud incident [{event['provider'].upper()}] "
                    f"{event.get('service', '?')}: {event['summary'][:200]}"
                ),
                data={
                    "provider": event["provider"],
                    "service": event.get("service"),
                    "region": event.get("region"),
                    "observation_id": obs.id,
                },
                location_id=None,
            ))

        self.log.info(
            "Cloud infra status: %d active incidents, %d total events "
            "(AWS=%d, GCP=%d, Azure=%d)",
            active_count, len(all_events),
            len(aws_events), len(gcp_events), len(azure_events),
        )

    def _parse_aws(self, data: Any) -> list[dict]:
        """Parse AWS current events, filtering for London-region relevance."""
        if not data or not isinstance(data, list):
            return []
        events = []
        for item in data:
            region = str(item.get("region_name", "")).lower()
            service = item.get("service_name", item.get("service", "unknown"))
            # Check if this event is relevant to London region
            if not any(r in region for r in _AWS_LONDON_REGIONS):
                arn = str(item.get("arn", "")).lower()
                if "eu-west-2" not in arn:
                    continue
            status = item.get("status", 0)
            summary = item.get("summary", "")
            # Get latest update text if available
            event_log = item.get("event_log", [])
            if event_log and isinstance(event_log, list):
                latest = event_log[0] if event_log else {}
                summary = latest.get("summary", summary)
            events.append({
                "provider": "aws",
                "service": service,
                "region": item.get("region_name", "unknown"),
                "summary": summary,
                "severity": "high" if status >= 2 else "low",
                "active": status >= 1,
                "started": item.get("date"),
            })
        return events

    def _parse_gcp(self, data: Any) -> list[dict]:
        """Parse GCP incidents, filtering for London-region and recent."""
        if not data or not isinstance(data, list):
            return []
        events = []
        now = datetime.now(timezone.utc)
        for item in data:
            # Only look at incidents from the last 7 days
            end_str = item.get("end")
            if end_str:
                try:
                    end_dt = datetime.fromisoformat(
                        end_str.replace("Z", "+00:00")
                    )
                    if (now - end_dt).days > 7:
                        continue
                except (ValueError, TypeError):
                    pass

            # Check if London region affected
            affected = item.get("affected_locations", [])
            london_relevant = False
            if not affected:
                london_relevant = True  # global incident
            else:
                for loc in affected:
                    loc_lower = str(loc).lower() if isinstance(loc, str) else ""
                    if isinstance(loc, dict):
                        loc_lower = str(loc.get("id", "") or loc.get("title", "")).lower()
                    if any(r in loc_lower for r in _GCP_LONDON_REGIONS):
                        london_relevant = True
                        break
            if not london_relevant:
                continue

            is_active = item.get("end") is None
            desc = item.get("external_desc", "")
            events.append({
                "provider": "gcp",
                "service": desc.split(" ")[0] if desc else "unknown",
                "region": "europe-west2",
                "summary": desc[:500],
                "severity": "high" if is_active else "resolved",
                "active": is_active,
                "started": item.get("begin"),
            })
        return events

    def _parse_azure(self, data: Any) -> list[dict]:
        """Parse Azure RSS feed for London-relevant incidents."""
        if not data or not isinstance(data, str):
            return []
        events = []
        try:
            root = ET.fromstring(data)
            for item in root.iter("item"):
                title = item.findtext("title", "")
                description = item.findtext("description", "")
                pub_date = item.findtext("pubDate", "")
                combined = (title + " " + description).lower()
                # Check for UK/London relevance
                if not any(kw in combined for kw in _AZURE_LONDON_KEYWORDS):
                    continue
                events.append({
                    "provider": "azure",
                    "service": title.split("-")[0].strip() if "-" in title else title,
                    "region": "uk south",
                    "summary": title,
                    "severity": "high",
                    "active": True,
                    "started": pub_date,
                })
        except ET.ParseError:
            self.log.warning("Failed to parse Azure RSS feed")
        return events


async def ingest_cloud_infra_status(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for cloud infrastructure status."""
    ingestor = CloudInfraStatusIngestor(board, graph, scheduler)
    await ingestor.run()
