"""BaseIngestor — shared async HTTP client, rate limiting, and error handling.

Includes a circuit breaker: after repeated fetch failures, an ingestor
automatically disables itself for a cooldown period, then retries once
("half-open"). If the retry succeeds, the circuit closes; otherwise it
re-opens with a longer cooldown. This prevents log spam and wasted API
calls against persistently broken endpoints.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import LondonGraph
from ..core.models import AgentMessage
from ..core.scheduler import AsyncScheduler

log = logging.getLogger("london.ingestors.base")

_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0

# Circuit breaker defaults
_CB_FAILURE_THRESHOLD = 3   # consecutive failures before opening circuit
_CB_INITIAL_COOLDOWN = 300  # 5 min initial cooldown
_CB_MAX_COOLDOWN = 3600     # 1 hour max cooldown


class _CircuitState:
    """Per-source circuit breaker state (shared across ingestor instances)."""
    __slots__ = ("failures", "cooldown", "open_since", "is_open", "total_trips")

    def __init__(self):
        self.failures: int = 0
        self.cooldown: float = _CB_INITIAL_COOLDOWN
        self.open_since: float = 0.0
        self.is_open: bool = False
        self.total_trips: int = 0


# Global registry — stored in sys.modules to survive hot-reloads.
# When the daemon reloads this module, a new _circuit_states dict would be
# created, wiping all accumulated failure counts. By stashing the dict in
# sys.modules metadata, we preserve it across reloads.
import sys as _sys
_CB_REGISTRY_KEY = "_london_circuit_states"
if not hasattr(_sys, _CB_REGISTRY_KEY):
    setattr(_sys, _CB_REGISTRY_KEY, {})
_circuit_states: dict[str, _CircuitState] = getattr(_sys, _CB_REGISTRY_KEY)


def get_circuit_states() -> dict[str, _CircuitState]:
    """Expose circuit states for system_health / daemon inspection."""
    return _circuit_states


class BaseIngestor(ABC):
    """Abstract base for all London Nervous System ingestors.

    Subclasses must implement:
        async def fetch_data(self) -> Any
        async def process(self, data: Any) -> None
    """

    source_name: str = "base"
    rate_limit_name: str = "default"
    required_env_vars: list[str] = []

    @classmethod
    def check_keys(cls) -> tuple[bool, list[str]]:
        """Check whether all required env vars are set.

        Returns (all_present, missing_var_names).
        """
        import os
        missing = [v for v in cls.required_env_vars if not os.environ.get(v, "").strip()]
        return (len(missing) == 0, missing)

    def __init__(
        self,
        board: MessageBoard,
        graph: LondonGraph,
        scheduler: AsyncScheduler,
    ) -> None:
        self.board = board
        self.graph = graph
        self.scheduler = scheduler
        self.log = logging.getLogger(f"london.ingestors.{self.source_name}")
        # Ensure circuit state exists for this source
        if self.source_name not in _circuit_states:
            _circuit_states[self.source_name] = _CircuitState()

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    async def fetch(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
        retries: int = _MAX_RETRIES,
    ) -> Any:
        """GET a URL with exponential backoff retries. Returns parsed JSON or None."""
        await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
        _timeout = timeout or _DEFAULT_TIMEOUT
        backoff = _BACKOFF_BASE
        last_exc: Exception | None = None

        for attempt in range(1, retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=_timeout) as session:
                    async with session.get(url, params=params, headers=headers) as resp:
                        if resp.status == 200:
                            try:
                                return await resp.json(content_type=None)
                            except Exception:
                                text = await resp.text()
                                self.log.debug(
                                    "Non-JSON response from %s (len=%d)", url, len(text)
                                )
                                return text
                        elif resp.status == 429:
                            self.log.warning(
                                "Rate-limited by %s (attempt %d/%d), backing off %.1fs",
                                url, attempt, retries, backoff,
                            )
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 120.0)
                        else:
                            self.log.warning(
                                "HTTP %d from %s (attempt %d/%d)",
                                resp.status, url, attempt, retries,
                            )
                            last_exc = RuntimeError(f"HTTP {resp.status}")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 120.0)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                self.log.warning(
                    "Network error fetching %s (attempt %d/%d): %s",
                    url, attempt, retries, exc,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120.0)

        self.log.error("All %d retries exhausted for %s: %s", retries, url, last_exc)
        return None

    # ── Abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    async def fetch_data(self) -> Any:
        """Fetch raw data from external source. Return None on failure."""

    @abstractmethod
    async def process(self, data: Any) -> None:
        """Process fetched data, store observations, and post to the board."""

    # ── Startup health check ────────────────────────────────────────────────

    async def health_check(self) -> tuple[bool, str]:
        """Quick single-fetch probe to test if this ingestor's source is reachable.

        Returns (ok, detail) — ok=True means the source responded successfully.
        Subclasses can override for custom probes; default calls fetch_data()
        with a short timeout.
        """
        try:
            data = await asyncio.wait_for(self.fetch_data(), timeout=15.0)
            if data is None:
                return False, "fetch returned None"
            return True, "ok"
        except asyncio.TimeoutError:
            return False, "timeout (15s)"
        except Exception as exc:
            return False, str(exc)[:120]

    # ── Circuit breaker ──────────────────────────────────────────────────────

    def _get_circuit(self) -> _CircuitState:
        return _circuit_states[self.source_name]

    async def _trip_circuit(self, cb: _CircuitState) -> None:
        """Open the circuit breaker and notify the board."""
        cb.is_open = True
        cb.open_since = time.monotonic()
        cb.total_trips += 1
        cooldown_min = cb.cooldown / 60
        self.log.warning(
            "CIRCUIT OPEN for %s after %d consecutive failures — "
            "skipping for %.0f min (trip #%d)",
            self.source_name, cb.failures, cooldown_min, cb.total_trips,
        )
        # Post to board so brain/system_health can see it
        msg = AgentMessage(
            from_agent="circuit_breaker",
            channel="#system",
            content=(
                f"Circuit breaker OPEN for {self.source_name}: "
                f"{cb.failures} consecutive failures, "
                f"cooling down {cooldown_min:.0f} min (trip #{cb.total_trips})"
            ),
            data={
                "source": self.source_name,
                "failures": cb.failures,
                "cooldown_seconds": cb.cooldown,
                "total_trips": cb.total_trips,
            },
        )
        await self.board.post(msg)

    async def _close_circuit(self, cb: _CircuitState) -> None:
        """Close the circuit after a successful half-open probe."""
        was_open = cb.is_open
        cb.failures = 0
        cb.is_open = False
        cb.cooldown = _CB_INITIAL_COOLDOWN  # reset cooldown
        if was_open:
            self.log.info("CIRCUIT CLOSED for %s — recovered after trip #%d", self.source_name, cb.total_trips)
            msg = AgentMessage(
                from_agent="circuit_breaker",
                channel="#system",
                content=f"Circuit breaker CLOSED for {self.source_name}: source recovered",
                data={"source": self.source_name, "total_trips": cb.total_trips},
            )
            await self.board.post(msg)

    # ── Run one cycle ─────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Execute one fetch-process cycle with circuit breaker protection."""
        cb = self._get_circuit()

        # If circuit is open, check if cooldown has elapsed
        if cb.is_open:
            elapsed = time.monotonic() - cb.open_since
            if elapsed < cb.cooldown:
                remaining = (cb.cooldown - elapsed) / 60
                self.log.debug(
                    "Circuit open for %s — skipping (%.0f min remaining)",
                    self.source_name, remaining,
                )
                return
            # Cooldown elapsed — try a half-open probe
            self.log.info("Circuit half-open for %s — probing...", self.source_name)

        try:
            data = await self.fetch_data()
            if data is None:
                # Fetch returned nothing (all retries exhausted)
                cb.failures += 1
                if cb.failures >= _CB_FAILURE_THRESHOLD and not cb.is_open:
                    await self._trip_circuit(cb)
                elif cb.is_open:
                    # Half-open probe failed — re-open with longer cooldown
                    cb.cooldown = min(cb.cooldown * 2, _CB_MAX_COOLDOWN)
                    cb.open_since = time.monotonic()
                    self.log.warning(
                        "Half-open probe failed for %s — re-opening, cooldown now %.0f min",
                        self.source_name, cb.cooldown / 60,
                    )
                else:
                    self.log.warning(
                        "No data for %s (%d/%d failures before circuit opens)",
                        self.source_name, cb.failures, _CB_FAILURE_THRESHOLD,
                    )
                return

            await self.process(data)
            # Success — close circuit if it was open/half-open
            await self._close_circuit(cb)
        except Exception:
            self.log.exception("Unhandled error in ingestor %s", self.source_name)
            cb.failures += 1
            if cb.failures >= _CB_FAILURE_THRESHOLD and not cb.is_open:
                await self._trip_circuit(cb)
