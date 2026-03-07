"""Cloud infrastructure status ingestor — monitors major cloud platforms serving London.

Addresses the Brain's suggestion: track cloud provider health (AWS eu-west-2,
GCP europe-west2, Cloudflare LHR) and GOV.UK platform status to diagnose
whether sensor outages are isolated incidents or symptoms of wider platform
failures. Many London data sources (government APIs, TfL, Environment Agency)
run on these platforms, so outages here cascade into missing data.

Sources (all free, no API keys):
- GOV.UK Publishing Platform (Statuspage.io API)
- GCP Status (public incidents JSON)
- Cloudflare London PoP (Statuspage.io API)

AWS Health Dashboard doesn't expose a clean JSON API for public status, so
we probe it via response code check only.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.cloud_infrastructure")

# Statuspage.io-based status pages (standard JSON API)
_STATUSPAGE_SOURCES = [
    {
        "name": "govuk",
        "label": "GOV.UK Platform",
        "url": "https://status.publishing.service.gov.uk/api/v2/components.json",
    },
    {
        "name": "cloudflare",
        "label": "Cloudflare (London PoP)",
        "url": "https://www.cloudflarestatus.com/api/v2/components.json",
        # Only care about London data centre
        "filter_component": "London, United Kingdom",
    },
]

# GCP exposes incidents as a public JSON list
_GCP_INCIDENTS_URL = "https://status.cloud.google.com/incidents.json"

# AWS doesn't have a clean public JSON API — just probe reachability
_AWS_PROBE_URL = "https://health.aws.amazon.com/health/status"

_PROBE_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=8)


class CloudInfrastructureIngestor(BaseIngestor):
    source_name = "cloud_infrastructure"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {
            "statuspage": [],
            "gcp_incidents": [],
            "aws_reachable": None,
        }

        async with aiohttp.ClientSession(timeout=_PROBE_TIMEOUT) as session:
            # 1. Statuspage.io sources (GOV.UK, Cloudflare)
            for source in _STATUSPAGE_SOURCES:
                data = await self._fetch_statuspage(session, source)
                results["statuspage"].append(data)

            # 2. GCP incidents
            results["gcp_incidents"] = await self._fetch_gcp_incidents(session)

            # 3. AWS reachability probe
            results["aws_reachable"] = await self._probe_aws(session)

        return results

    async def _fetch_statuspage(
        self, session: aiohttp.ClientSession, source: dict
    ) -> dict[str, Any]:
        name = source["name"]
        t0 = time.monotonic()
        try:
            async with session.get(source["url"]) as resp:
                elapsed_ms = (time.monotonic() - t0) * 1000
                if resp.status != 200:
                    return {
                        "name": name,
                        "label": source["label"],
                        "healthy": False,
                        "response_ms": round(elapsed_ms, 1),
                        "error": f"HTTP {resp.status}",
                        "components": [],
                    }
                data = await resp.json(content_type=None)
                components = data.get("components", [])

                # Apply component filter if specified
                component_filter = source.get("filter_component")
                if component_filter:
                    components = [
                        c for c in components
                        if component_filter.lower() in c.get("name", "").lower()
                    ]

                non_operational = [
                    c for c in components if c.get("status") != "operational"
                ]

                return {
                    "name": name,
                    "label": source["label"],
                    "healthy": len(non_operational) == 0,
                    "response_ms": round(elapsed_ms, 1),
                    "error": None,
                    "total_components": len(components),
                    "degraded_components": [
                        {"name": c["name"], "status": c["status"]}
                        for c in non_operational
                    ],
                    "components": [
                        {"name": c["name"], "status": c["status"]}
                        for c in components
                    ],
                }
        except (aiohttp.ClientError, OSError, Exception) as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000
            return {
                "name": name,
                "label": source["label"],
                "healthy": False,
                "response_ms": round(elapsed_ms, 1),
                "error": str(exc)[:200],
                "components": [],
            }

    async def _fetch_gcp_incidents(
        self, session: aiohttp.ClientSession
    ) -> dict[str, Any]:
        t0 = time.monotonic()
        try:
            async with session.get(_GCP_INCIDENTS_URL) as resp:
                elapsed_ms = (time.monotonic() - t0) * 1000
                if resp.status != 200:
                    return {
                        "healthy": False,
                        "response_ms": round(elapsed_ms, 1),
                        "error": f"HTTP {resp.status}",
                        "active_incidents": [],
                    }
                incidents = await resp.json(content_type=None)
                # Active = no end time
                active = [
                    inc for inc in (incidents or [])
                    if not inc.get("end")
                ]
                # Filter for europe-west2 (London) related if possible
                london_active = [
                    inc for inc in active
                    if "europe" in str(inc.get("external_desc", "")).lower()
                    or "london" in str(inc.get("external_desc", "")).lower()
                    or "global" in str(inc.get("external_desc", "")).lower()
                    or "multiple" in str(inc.get("external_desc", "")).lower()
                ]

                return {
                    "healthy": len(active) == 0,
                    "response_ms": round(elapsed_ms, 1),
                    "error": None,
                    "total_active": len(active),
                    "london_relevant": len(london_active),
                    "active_incidents": [
                        {
                            "id": inc.get("id"),
                            "desc": (inc.get("external_desc") or "")[:200],
                            "begin": inc.get("begin"),
                        }
                        for inc in active[:5]
                    ],
                }
        except (aiohttp.ClientError, OSError, Exception) as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000
            return {
                "healthy": False,
                "response_ms": round(elapsed_ms, 1),
                "error": str(exc)[:200],
                "active_incidents": [],
            }

    async def _probe_aws(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        t0 = time.monotonic()
        try:
            async with session.get(_AWS_PROBE_URL) as resp:
                elapsed_ms = (time.monotonic() - t0) * 1000
                return {
                    "healthy": resp.status == 200,
                    "status_code": resp.status,
                    "response_ms": round(elapsed_ms, 1),
                    "error": None,
                }
        except (aiohttp.ClientError, OSError, Exception) as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000
            return {
                "healthy": False,
                "status_code": None,
                "response_ms": round(elapsed_ms, 1),
                "error": str(exc)[:200],
            }

    async def process(self, data: Any) -> None:
        statuspage_results = data.get("statuspage", [])
        gcp = data.get("gcp_incidents", {})
        aws = data.get("aws_reachable", {})

        all_healthy = True

        # --- Statuspage sources (GOV.UK, Cloudflare London) ---
        for result in statuspage_results:
            name = result["name"]
            label = result["label"]
            healthy = result.get("healthy", False)
            if not healthy:
                all_healthy = False

            health_val = 1.0 if healthy else 0.0
            degraded = result.get("degraded_components", [])

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=health_val,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "cloud_platform_health",
                    "platform": name,
                    "platform_label": label,
                    "healthy": healthy,
                    "response_ms": result.get("response_ms"),
                    "total_components": result.get("total_components", 0),
                    "degraded_count": len(degraded),
                    "degraded_components": degraded[:5],
                    "error": result.get("error"),
                },
            )
            await self.board.store_observation(obs)

            if degraded:
                degraded_str = ", ".join(
                    f"{c['name']}={c['status']}" for c in degraded[:3]
                )
                content = (
                    f"Cloud platform {label}: DEGRADED — {degraded_str}"
                )
            elif result.get("error"):
                content = (
                    f"Cloud platform {label}: UNREACHABLE — {result['error']}"
                )
            else:
                content = (
                    f"Cloud platform {label}: operational "
                    f"({result.get('total_components', 0)} components OK, "
                    f"{result.get('response_ms', 0):.0f}ms)"
                )

            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=content,
                data={
                    "metric": "cloud_platform_health",
                    "platform": name,
                    "healthy": healthy,
                    "response_ms": result.get("response_ms"),
                    "observation_id": obs.id,
                },
            ))

        # --- GCP ---
        gcp_healthy = gcp.get("healthy", False)
        if not gcp_healthy:
            all_healthy = False

        gcp_obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=1.0 if gcp_healthy else 0.0,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_platform_health",
                "platform": "gcp",
                "platform_label": "Google Cloud Platform",
                "healthy": gcp_healthy,
                "response_ms": gcp.get("response_ms"),
                "total_active_incidents": gcp.get("total_active", 0),
                "london_relevant_incidents": gcp.get("london_relevant", 0),
                "active_incidents": gcp.get("active_incidents", [])[:3],
                "error": gcp.get("error"),
            },
        )
        await self.board.store_observation(gcp_obs)

        if gcp.get("active_incidents"):
            inc_desc = gcp["active_incidents"][0].get("desc", "unknown")[:80]
            gcp_content = (
                f"Cloud platform GCP: {gcp.get('total_active', 0)} active incident(s) — {inc_desc}"
            )
        elif gcp.get("error"):
            gcp_content = f"Cloud platform GCP: status check failed — {gcp['error']}"
        else:
            gcp_content = (
                f"Cloud platform GCP: no active incidents ({gcp.get('response_ms', 0):.0f}ms)"
            )

        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=gcp_content,
            data={
                "metric": "cloud_platform_health",
                "platform": "gcp",
                "healthy": gcp_healthy,
                "observation_id": gcp_obs.id,
            },
        ))

        # --- AWS reachability ---
        aws_healthy = aws.get("healthy", False)
        if not aws_healthy:
            all_healthy = False

        aws_obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=1.0 if aws_healthy else 0.0,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_platform_health",
                "platform": "aws",
                "platform_label": "AWS Health Dashboard",
                "healthy": aws_healthy,
                "status_code": aws.get("status_code"),
                "response_ms": aws.get("response_ms"),
                "error": aws.get("error"),
            },
        )
        await self.board.store_observation(aws_obs)

        aws_status = f"HTTP {aws.get('status_code')}" if aws.get("status_code") else aws.get("error", "unreachable")
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Cloud platform AWS: {'reachable' if aws_healthy else 'UNREACHABLE'} "
                f"({aws_status}, {aws.get('response_ms', 0):.0f}ms)"
            ),
            data={
                "metric": "cloud_platform_health",
                "platform": "aws",
                "healthy": aws_healthy,
                "observation_id": aws_obs.id,
            },
        ))

        # --- Overall summary ---
        platforms_checked = len(statuspage_results) + 2  # +GCP +AWS
        healthy_count = (
            sum(1 for r in statuspage_results if r.get("healthy"))
            + (1 if gcp_healthy else 0)
            + (1 if aws_healthy else 0)
        )

        overall_obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=healthy_count / platforms_checked if platforms_checked else 0,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_infrastructure_overall",
                "healthy_count": healthy_count,
                "total_platforms": platforms_checked,
                "all_healthy": all_healthy,
            },
        )
        await self.board.store_observation(overall_obs)

        self.log.info(
            "Cloud infrastructure: %d/%d platforms healthy (GOV.UK=%s, CF=%s, GCP=%s, AWS=%s)",
            healthy_count,
            platforms_checked,
            "OK" if any(r["name"] == "govuk" and r.get("healthy") for r in statuspage_results) else "DOWN",
            "OK" if any(r["name"] == "cloudflare" and r.get("healthy") for r in statuspage_results) else "DOWN",
            "OK" if gcp_healthy else "DOWN",
            "OK" if aws_healthy else "DOWN",
        )


async def ingest_cloud_infrastructure(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one cloud infrastructure check cycle."""
    ingestor = CloudInfrastructureIngestor(board, graph, scheduler)
    await ingestor.run()
