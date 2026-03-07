"""Cloud infrastructure health ingestor — monitors status of cloud providers
and data platforms that underpin London's data ecosystem.

Addresses the Brain's suggestion: move from symptom detection (e.g. corrupted
TfL data) to root cause analysis by monitoring the health of shared cloud
infrastructure that multiple London data sources depend on.

Monitors (all free, no API keys):
- Azure (UK South) — via RSS status feed (many UK govt services use Azure)
- AWS (eu-west-2 London) — via status data.json
- GCP (europe-west2 London) — via incidents.json
- London Datastore — via CKAN status API

This complements provider_status.py (which probes individual API health) by
adding the *infrastructure layer beneath* those APIs.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any
from xml.etree import ElementTree

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("london.ingestors.cloud_infra_health")

# Azure status RSS (covers all regions, we filter for UK South)
AZURE_RSS_URL = "https://rssfeed.azure.status.microsoft/en-us/status/feed/"

# AWS status data (JSON, covers all regions, we filter for eu-west-2)
AWS_STATUS_URL = "https://status.aws.amazon.com/data.json"

# GCP incidents JSON (structured, filterable by region)
GCP_INCIDENTS_URL = "https://status.cloud.google.com/incidents.json"

# London Datastore CKAN API
LONDON_DATASTORE_URL = "https://data.london.gov.uk/api/action/status_show"

_PROBE_TIMEOUT = aiohttp.ClientTimeout(total=20, connect=10)

# Only surface incidents from the last 24 hours
_INCIDENT_MAX_AGE_SECS = 86400


class CloudInfraHealthIngestor(BaseIngestor):
    source_name = "cloud_infra_health"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Probe all cloud status endpoints."""
        results: dict[str, Any] = {}
        async with aiohttp.ClientSession(timeout=_PROBE_TIMEOUT) as session:
            results["azure"] = await self._fetch_azure(session)
            results["aws"] = await self._fetch_aws(session)
            results["gcp"] = await self._fetch_gcp(session)
            results["london_datastore"] = await self._fetch_london_datastore(session)
        return results

    async def _fetch_azure(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        """Parse Azure status RSS for UK-relevant incidents."""
        t0 = time.monotonic()
        try:
            async with session.get(AZURE_RSS_URL) as resp:
                elapsed = (time.monotonic() - t0) * 1000
                if resp.status != 200:
                    return {"healthy": False, "error": f"HTTP {resp.status}",
                            "response_ms": round(elapsed, 1), "incidents": []}
                text = await resp.text()
                root = ElementTree.fromstring(text)
                channel = root.find("channel")
                items = channel.findall("item") if channel is not None else []
                incidents = []
                now = time.time()
                for item in items:
                    title = (item.findtext("title") or "").strip()
                    desc = (item.findtext("description") or "").strip()
                    pubdate = (item.findtext("pubDate") or "").strip()
                    # Filter for UK-relevant keywords
                    combined = f"{title} {desc}".lower()
                    if any(kw in combined for kw in ("uk south", "uk west",
                                                      "europe", "global")):
                        incidents.append({
                            "title": title[:200],
                            "description": desc[:300],
                            "published": pubdate,
                        })
                return {
                    "healthy": len(incidents) == 0,
                    "response_ms": round(elapsed, 1),
                    "incidents": incidents[:5],
                    "total_items": len(items),
                    "error": None,
                }
        except Exception as exc:
            elapsed = (time.monotonic() - t0) * 1000
            return {"healthy": None, "error": str(exc)[:200],
                    "response_ms": round(elapsed, 1), "incidents": []}

    async def _fetch_aws(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        """Parse AWS status data.json for eu-west-2 (London) incidents."""
        t0 = time.monotonic()
        try:
            async with session.get(AWS_STATUS_URL) as resp:
                elapsed = (time.monotonic() - t0) * 1000
                if resp.status != 200:
                    return {"healthy": False, "error": f"HTTP {resp.status}",
                            "response_ms": round(elapsed, 1), "incidents": []}
                raw = await resp.read()
                # AWS returns UTF-16-LE with BOM
                text = raw.decode("utf-16-le", errors="replace")
                if text and text[0] == "\ufeff":
                    text = text[1:]
                import json
                data = json.loads(text)
                incidents = []
                now = time.time()
                for entry in data:
                    service = entry.get("service", "")
                    region = entry.get("region_name", "")
                    # Filter for London region or global
                    if "eu-west-2" in service or "london" in region.lower():
                        # Check recency
                        ts = entry.get("date")
                        if ts and (now - float(ts)) > _INCIDENT_MAX_AGE_SECS:
                            continue
                        incidents.append({
                            "service": entry.get("service_name", service)[:100],
                            "summary": entry.get("summary", "")[:200],
                            "status": entry.get("status"),
                            "region": region,
                        })
                return {
                    "healthy": len(incidents) == 0,
                    "response_ms": round(elapsed, 1),
                    "incidents": incidents[:5],
                    "total_events": len(data),
                    "error": None,
                }
        except Exception as exc:
            elapsed = (time.monotonic() - t0) * 1000
            return {"healthy": None, "error": str(exc)[:200],
                    "response_ms": round(elapsed, 1), "incidents": []}

    async def _fetch_gcp(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        """Parse GCP incidents.json for europe-west2 (London) incidents."""
        t0 = time.monotonic()
        try:
            async with session.get(GCP_INCIDENTS_URL) as resp:
                elapsed = (time.monotonic() - t0) * 1000
                if resp.status != 200:
                    return {"healthy": False, "error": f"HTTP {resp.status}",
                            "response_ms": round(elapsed, 1), "incidents": []}
                data = await resp.json(content_type=None)
                incidents = []
                now_dt = datetime.now(timezone.utc)
                for entry in data:
                    # Check if europe-west2 is affected
                    affected = entry.get("currently_affected_locations", [])
                    previously = entry.get("previously_affected_locations", [])
                    all_locations = []
                    for loc_group in affected + previously:
                        for loc in loc_group.get("locations", []):
                            all_locations.append(loc.get("id", ""))
                    if not any("europe-west2" in loc for loc in all_locations):
                        continue
                    # Check recency
                    end = entry.get("end")
                    if end:
                        # Ended incident — skip if older than 24h
                        try:
                            end_dt = datetime.fromisoformat(
                                end.replace("Z", "+00:00"))
                            if (now_dt - end_dt).total_seconds() > _INCIDENT_MAX_AGE_SECS:
                                continue
                        except (ValueError, TypeError):
                            pass
                    incidents.append({
                        "service": entry.get("service_name", "")[:100],
                        "summary": entry.get("external_desc", "")[:200],
                        "severity": entry.get("severity", ""),
                        "status": entry.get("status_impact", ""),
                        "active": entry.get("end") is None,
                    })
                return {
                    "healthy": len(incidents) == 0,
                    "response_ms": round(elapsed, 1),
                    "incidents": incidents[:5],
                    "total_events": len(data),
                    "error": None,
                }
        except Exception as exc:
            elapsed = (time.monotonic() - t0) * 1000
            return {"healthy": None, "error": str(exc)[:200],
                    "response_ms": round(elapsed, 1), "incidents": []}

    async def _fetch_london_datastore(
        self, session: aiohttp.ClientSession
    ) -> dict[str, Any]:
        """Probe London Datastore CKAN API health."""
        t0 = time.monotonic()
        try:
            async with session.get(LONDON_DATASTORE_URL) as resp:
                elapsed = (time.monotonic() - t0) * 1000
                if resp.status != 200:
                    return {"healthy": False, "error": f"HTTP {resp.status}",
                            "response_ms": round(elapsed, 1)}
                data = await resp.json(content_type=None)
                success = data.get("success", False)
                return {
                    "healthy": success,
                    "response_ms": round(elapsed, 1),
                    "ckan_version": data.get("result", {}).get("ckan_version"),
                    "error": None,
                }
        except Exception as exc:
            elapsed = (time.monotonic() - t0) * 1000
            return {"healthy": None, "error": str(exc)[:200],
                    "response_ms": round(elapsed, 1)}

    async def process(self, data: Any) -> None:
        providers = ["azure", "aws", "gcp", "london_datastore"]
        labels = {
            "azure": "Azure (UK South)",
            "aws": "AWS (eu-west-2 London)",
            "gcp": "GCP (europe-west2 London)",
            "london_datastore": "London Datastore",
        }

        healthy_count = 0
        total = len(providers)
        all_incidents: list[dict] = []

        for provider in providers:
            info = data.get(provider, {})
            is_healthy = info.get("healthy")
            incidents = info.get("incidents", [])
            response_ms = info.get("response_ms", -1)
            error = info.get("error")

            if is_healthy is True:
                healthy_count += 1
            health_val = 1.0 if is_healthy is True else (
                0.0 if is_healthy is False else -1.0)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=health_val,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "cloud_provider_health",
                    "provider": provider,
                    "provider_label": labels[provider],
                    "response_ms": response_ms,
                    "incident_count": len(incidents),
                    "incidents": incidents[:3],
                    "error": error,
                },
            )
            await self.board.store_observation(obs)

            status_str = "HEALTHY" if is_healthy is True else (
                "DEGRADED" if is_healthy is False else "UNKNOWN")
            incident_str = ""
            if incidents:
                first = incidents[0]
                incident_str = f" — {first.get('summary') or first.get('title', '')}"
                all_incidents.extend(incidents)

            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Cloud infra [{labels[provider]}] {status_str} "
                    f"({response_ms:.0f}ms, {len(incidents)} incidents)"
                    f"{incident_str}"
                ),
                data={
                    "metric": "cloud_provider_health",
                    "provider": provider,
                    "healthy": is_healthy,
                    "response_ms": response_ms,
                    "incident_count": len(incidents),
                    "observation_id": obs.id,
                },
            ))

        # Overall infrastructure health score
        overall = healthy_count / total if total else 0.0
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=overall,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_infra_health_overall",
                "healthy_count": healthy_count,
                "total_providers": total,
                "total_incidents": len(all_incidents),
                "providers": {p: data.get(p, {}).get("healthy") for p in providers},
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Cloud infrastructure health: {healthy_count}/{total} providers healthy, "
                f"{len(all_incidents)} active incidents"
            ),
            data={
                "metric": "cloud_infra_health_overall",
                "overall_health": round(overall, 3),
                "healthy_count": healthy_count,
                "total_providers": total,
                "total_incidents": len(all_incidents),
                "observation_id": obs.id,
            },
        ))

        self.log.info(
            "Cloud infra health: %d/%d healthy, %d incidents (latencies: %s)",
            healthy_count, total, len(all_incidents),
            ", ".join(
                f"{p}={data.get(p, {}).get('response_ms', -1):.0f}ms"
                for p in providers
            ),
        )


async def ingest_cloud_infra_health(
    board: MessageBoard,
    graph: LondonGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one cloud infra health check cycle."""
    ingestor = CloudInfraHealthIngestor(board, graph, scheduler)
    await ingestor.run()
