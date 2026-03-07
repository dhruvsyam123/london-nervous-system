"""Cloud infrastructure status ingestor — AWS, Azure, Azure DevOps, Google Cloud.

Monitors the real-time operational status of major cloud providers'
EU/London availability zones via their public status feeds.

This lets the system correlate sensor/API failures with known external
cloud infrastructure outages — distinguishing "our API failed" from
"the cloud region is down."

Endpoints (all free, no auth required):
- AWS:        RSS feeds per service per region (eu-west-2 = London)
- Azure:      Global RSS status feed
- Azure DevOps: JSON status API with UK geography filter
- GCP:        JSON incidents feed with region-level detail (europe-west2 = London)
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.cloud_status")

# ── Endpoints ─────────────────────────────────────────────────────────────────

# AWS eu-west-2 (London) RSS feeds for key services
_AWS_RSS_FEEDS = [
    ("ec2", "https://status.aws.amazon.com/rss/ec2-eu-west-2.rss"),
    ("s3", "https://status.aws.amazon.com/rss/s3-eu-west-2.rss"),
    ("lambda", "https://status.aws.amazon.com/rss/lambda-eu-west-2.rss"),
    ("rds", "https://status.aws.amazon.com/rss/rds-eu-west-2.rss"),
    ("ecs", "https://status.aws.amazon.com/rss/ecs-eu-west-2.rss"),
    ("dynamodb", "https://status.aws.amazon.com/rss/dynamodb-eu-west-2.rss"),
    ("sqs", "https://status.aws.amazon.com/rss/sqs-eu-west-2.rss"),
    ("elb", "https://status.aws.amazon.com/rss/elb-eu-west-2.rss"),
    ("cloudfront", "https://status.aws.amazon.com/rss/cloudfront-eu-west-2.rss"),
    ("route53", "https://status.aws.amazon.com/rss/route53-eu-west-2.rss"),
]

# Azure global status RSS
_AZURE_RSS_URL = "https://azure.status.microsoft/en-us/status/feed/"

# Azure DevOps status JSON (UK geography, no auth required)
_AZURE_DEVOPS_URL = (
    "https://status.dev.azure.com/_apis/status/health"
    "?geographies=UK&api-version=7.1-preview.1"
)

# Google Cloud JSON incidents
_GCP_INCIDENTS_URL = "https://status.cloud.google.com/incidents.json"

# London-area GCP region
_GCP_LONDON_REGIONS = {"europe-west2"}

_TIMEOUT = aiohttp.ClientTimeout(total=20, connect=10)


def _parse_rss_items(xml_text: str, max_items: int = 5) -> list[dict[str, str]]:
    """Parse RSS XML and return recent items as dicts."""
    items = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.iter("item"):
            title = item.findtext("title", "").strip()
            desc = item.findtext("description", "").strip()
            pub_date = item.findtext("pubDate", "").strip()
            if title:
                items.append({
                    "title": title,
                    "description": desc[:500],
                    "pub_date": pub_date,
                })
            if len(items) >= max_items:
                break
    except ET.ParseError as exc:
        log.warning("RSS parse error: %s", exc)
    return items


class CloudStatusIngestor(BaseIngestor):
    source_name = "cloud_status"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {"aws": {}, "azure": {}, "azure_devops": {}, "gcp": {}}
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            # AWS RSS feeds
            for service, url in _AWS_RSS_FEEDS:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            items = _parse_rss_items(text)
                            results["aws"][service] = {
                                "ok": True,
                                "incidents": items,
                            }
                        else:
                            results["aws"][service] = {
                                "ok": False,
                                "error": f"HTTP {resp.status}",
                            }
                except (aiohttp.ClientError, OSError) as exc:
                    results["aws"][service] = {
                        "ok": False,
                        "error": str(exc)[:200],
                    }

            # Azure RSS
            try:
                async with session.get(_AZURE_RSS_URL) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        items = _parse_rss_items(text, max_items=10)
                        # Filter for EU/UK related items
                        eu_items = [
                            i for i in items
                            if any(
                                kw in i["title"].lower() or kw in i["description"].lower()
                                for kw in ("uk", "europe", "west europe", "north europe", "uk south")
                            )
                        ]
                        results["azure"] = {
                            "ok": True,
                            "incidents_total": len(items),
                            "incidents_eu": eu_items,
                        }
                    else:
                        results["azure"] = {"ok": False, "error": f"HTTP {resp.status}"}
            except (aiohttp.ClientError, OSError) as exc:
                results["azure"] = {"ok": False, "error": str(exc)[:200]}

            # Azure DevOps UK JSON
            try:
                async with session.get(_AZURE_DEVOPS_URL) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        status = data.get("status", {})
                        health = status.get("health", "unknown")
                        services = []
                        for svc in data.get("services", []):
                            svc_health = "unknown"
                            for geo in svc.get("geographies", []):
                                if geo.get("id") == "UK":
                                    svc_health = geo.get("health", "unknown")
                                    break
                            services.append({
                                "id": svc.get("id", ""),
                                "health": svc_health,
                            })
                        results["azure_devops"] = {
                            "ok": True,
                            "overall_health": health,
                            "healthy": health == "healthy",
                            "services": services,
                        }
                    else:
                        results["azure_devops"] = {
                            "ok": False,
                            "error": f"HTTP {resp.status}",
                        }
            except (aiohttp.ClientError, OSError) as exc:
                results["azure_devops"] = {
                    "ok": False,
                    "error": str(exc)[:200],
                }

            # GCP JSON
            try:
                async with session.get(_GCP_INCIDENTS_URL) as resp:
                    if resp.status == 200:
                        incidents = await resp.json(content_type=None)
                        if not isinstance(incidents, list):
                            incidents = []
                        # Filter for active incidents affecting London region
                        active_london = []
                        recent_london = []
                        now = datetime.now(timezone.utc)
                        for inc in incidents[:50]:  # check last 50
                            affected = set()
                            for loc in inc.get("currently_affected_locations", []):
                                affected.add(loc.get("id", ""))
                            prev_affected = set()
                            for loc in inc.get("previously_affected_locations", []):
                                prev_affected.add(loc.get("id", ""))
                            london_affected = bool(affected & _GCP_LONDON_REGIONS)
                            london_prev = bool(prev_affected & _GCP_LONDON_REGIONS)
                            if london_affected:
                                active_london.append({
                                    "id": inc.get("id", ""),
                                    "title": inc.get("external_desc", ""),
                                    "severity": inc.get("severity", ""),
                                    "service": inc.get("service_name", ""),
                                    "status": inc.get("status_impact", ""),
                                    "begin": inc.get("begin", ""),
                                })
                            elif london_prev:
                                recent_london.append({
                                    "id": inc.get("id", ""),
                                    "title": inc.get("external_desc", ""),
                                    "severity": inc.get("severity", ""),
                                    "service": inc.get("service_name", ""),
                                    "end": inc.get("end", ""),
                                })
                                if len(recent_london) >= 5:
                                    break
                        results["gcp"] = {
                            "ok": True,
                            "active_london_incidents": active_london,
                            "recent_london_incidents": recent_london[:5],
                        }
                    else:
                        results["gcp"] = {"ok": False, "error": f"HTTP {resp.status}"}
            except (aiohttp.ClientError, OSError) as exc:
                results["gcp"] = {"ok": False, "error": str(exc)[:200]}

        return results

    async def process(self, data: Any) -> None:
        aws_data = data.get("aws", {})
        azure_data = data.get("azure", {})
        azure_devops_data = data.get("azure_devops", {})
        gcp_data = data.get("gcp", {})

        # ── AWS eu-west-2 ──
        aws_services_ok = sum(
            1 for v in aws_data.values() if v.get("ok")
        )
        aws_total = len(aws_data)
        aws_active_incidents = []
        for svc, info in aws_data.items():
            if info.get("ok") and info.get("incidents"):
                aws_active_incidents.extend(
                    {"service": svc, **i} for i in info["incidents"]
                )

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=aws_services_ok / aws_total if aws_total else 1.0,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_health_aws_london",
                "provider": "aws",
                "region": "eu-west-2",
                "services_reachable": aws_services_ok,
                "services_total": aws_total,
                "recent_incidents": len(aws_active_incidents),
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Cloud status AWS eu-west-2: {aws_services_ok}/{aws_total} feeds reachable, "
                f"{len(aws_active_incidents)} recent incident items"
            ),
            data={
                "provider": "aws",
                "region": "eu-west-2",
                "feeds_ok": aws_services_ok,
                "feeds_total": aws_total,
                "incident_count": len(aws_active_incidents),
                "observation_id": obs.id,
            },
        ))

        # ── Azure EU ──
        azure_ok = azure_data.get("ok", False)
        azure_eu_incidents = azure_data.get("incidents_eu", [])
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=1.0 if azure_ok and not azure_eu_incidents else 0.0,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_health_azure_eu",
                "provider": "azure",
                "region": "global (EU filtered)",
                "feed_ok": azure_ok,
                "eu_incidents": len(azure_eu_incidents),
                "eu_incident_titles": [
                    i.get("title", "")[:100] for i in azure_eu_incidents[:3]
                ],
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Cloud status Azure: feed={'UP' if azure_ok else 'DOWN'}, "
                f"{len(azure_eu_incidents)} EU-related incidents"
            ),
            data={
                "provider": "azure",
                "feed_ok": azure_ok,
                "eu_incident_count": len(azure_eu_incidents),
                "observation_id": obs.id,
            },
        ))

        # ── Azure DevOps UK ──
        devops_ok = azure_devops_data.get("ok", False)
        devops_healthy = azure_devops_data.get("healthy", False)
        if devops_ok:
            degraded = [
                s["id"] for s in azure_devops_data.get("services", [])
                if s.get("health") not in ("healthy", "unknown")
            ]
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=1.0 if devops_healthy else 0.0,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "cloud_health_azure_devops_uk",
                    "provider": "azure_devops",
                    "region": "UK",
                    "overall_health": azure_devops_data.get("overall_health"),
                    "degraded_services": degraded,
                },
            )
            await self.board.store_observation(obs)
            status_str = azure_devops_data.get("overall_health", "unknown")
            if degraded:
                status_str += f" — degraded: {', '.join(degraded)}"
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"Cloud status Azure DevOps UK: {status_str}",
                data={
                    "provider": "azure_devops",
                    "region": "UK",
                    "healthy": devops_healthy,
                    "degraded_services": degraded,
                    "observation_id": obs.id,
                },
            ))

        # ── GCP europe-west2 ──
        gcp_ok = gcp_data.get("ok", False)
        gcp_active = gcp_data.get("active_london_incidents", [])
        gcp_recent = gcp_data.get("recent_london_incidents", [])
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=1.0 if gcp_ok and not gcp_active else 0.0,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_health_gcp_london",
                "provider": "gcp",
                "region": "europe-west2",
                "feed_ok": gcp_ok,
                "active_london_incidents": len(gcp_active),
                "recent_london_incidents": len(gcp_recent),
                "active_details": gcp_active[:3],
            },
        )
        await self.board.store_observation(obs)

        active_str = (
            f"{len(gcp_active)} ACTIVE London incidents"
            if gcp_active
            else "no active London incidents"
        )
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Cloud status GCP europe-west2: feed={'UP' if gcp_ok else 'DOWN'}, "
                f"{active_str}, {len(gcp_recent)} recent resolved"
            ),
            data={
                "provider": "gcp",
                "region": "europe-west2",
                "feed_ok": gcp_ok,
                "active_incidents": len(gcp_active),
                "recent_incidents": len(gcp_recent),
                "observation_id": obs.id,
            },
        ))

        # ── Summary ──
        providers_healthy = sum([
            aws_services_ok == aws_total and aws_total > 0,
            azure_ok and not azure_eu_incidents,
            gcp_ok and not gcp_active,
        ])
        self.log.info(
            "Cloud status: %d/3 providers healthy (AWS: %d/%d feeds, Azure EU: %d incidents, GCP London: %d active)",
            providers_healthy,
            aws_services_ok, aws_total,
            len(azure_eu_incidents),
            len(gcp_active),
        )


async def ingest_cloud_status(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one cloud status check cycle."""
    ingestor = CloudStatusIngestor(board, graph, scheduler)
    await ingestor.run()
