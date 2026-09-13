"""Bounded, polite transport policy for Scryfall-owned endpoints."""
import asyncio
import logging
import time
import random
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from src.config import settings
from src.services.tor import async_http_client

logger = logging.getLogger("mtg_worker.scryfall_transport")

FALLBACK_TIMEOUT_SECONDS = 15.0
DIRECT_REQUEST_INTERVAL_SECONDS = 0.15
CARD_QUERY_INTERVAL_SECONDS = 0.65
MIN_429_COOLDOWN_SECONDS = 30.0
MAX_429_RETRIES = 10
MAX_TRANSPORT_RETRIES = 3
_DIRECT_HOSTS: set[str] = set()
@dataclass
class HostState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    next_request_at: float = 0.0
    next_query_at: float = 0.0
    next_manifest_at: float = 0.0
    cooldown_until: float = 0.0


_host_states: dict[str, HostState] = {}
_CARD_QUERIES = {"/cards/collection", "/cards/search", "/cards/named", "/cards/random"}


def _retry_delay(response: httpx.Response, retry_count: int) -> float:
    """Honor Retry-After seconds or HTTP dates, with a 30-second floor."""
    value = response.headers.get("Retry-After", "")
    try:
        requested = float(value)
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            requested = (date - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            requested = 0.0
    if not math.isfinite(requested):
        requested = 0.0
    return max(requested, min(MIN_429_COOLDOWN_SECONDS * 2 ** retry_count, 300.0)) + random.uniform(0.1, 1.0)


async def request(method: str, url: str, *, headers: dict[str, str], timeout: float = 30.0, max_429_retries: int | None = None, **kwargs: Any) -> httpx.Response:
    """Serialize requests per host across Tor/direct and share 429 cooldowns.

    State is process-local: other processes must use this worker as their
    gateway or coordinate through a distributed limiter.
    """
    host = urlsplit(url).netloc
    path = urlsplit(url).path.rstrip("/")
    state = _host_states.setdefault(host, HostState())
    using_tor = settings.TOR_ENABLED and host not in _DIRECT_HOSTS
    retry_limit = MAX_429_RETRIES if max_429_retries is None else max_429_retries
    retry_count = 0
    transport_retries = 0
    while True:
        try:
            client_factory = async_http_client if using_tor else httpx.AsyncClient
            request_timeout = timeout if using_tor else FALLBACK_TIMEOUT_SECONDS
            async with state.lock:
                # Hold the lock through the response so a 429 pauses queued
                # requests before another task can issue a new API call.
                while True:
                    ready = max(state.next_request_at, state.cooldown_until,
                                state.next_query_at if path in _CARD_QUERIES else 0.0,
                                state.next_manifest_at if path == "/cards/manifest" else 0.0)
                    delay = ready - time.monotonic()
                    if delay <= 0:
                        break
                    await asyncio.sleep(delay)
                now = time.monotonic()
                state.next_request_at = now + max(DIRECT_REQUEST_INTERVAL_SECONDS, settings.RATE_LIMIT_DELAY_SECONDS)
                if path in _CARD_QUERIES:
                    state.next_query_at = now + CARD_QUERY_INTERVAL_SECONDS
                if path == "/cards/manifest":
                    state.next_manifest_at = now + 6.1
                async with client_factory(headers=headers, timeout=request_timeout, follow_redirects=True) as client:
                    response = await client.request(method, url, **kwargs)
                if response.status_code == httpx.codes.TOO_MANY_REQUESTS:
                    delay = _retry_delay(response, retry_count)
                    state.cooldown_until = max(state.cooldown_until, time.monotonic() + delay)
        except httpx.TransportError as error:
            if using_tor:
                logger.warning("Tor request failed for %s; retrying directly with a %.0fs timeout: %s", url, FALLBACK_TIMEOUT_SECONDS, error)
                _DIRECT_HOSTS.add(host)
                using_tor = False
                continue
            transport_retries += 1
            if transport_retries > MAX_TRANSPORT_RETRIES:
                raise
            delay = min(2 ** transport_retries, 8)
            logger.warning(
                "Direct Scryfall request failed for %s (%s); retry %s/%s in %ss",
                url,
                error,
                transport_retries,
                MAX_TRANSPORT_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)
            continue

        if response.status_code == httpx.codes.TOO_MANY_REQUESTS:
            if retry_count >= retry_limit:
                return response
            retry_count += 1
            logger.warning("Scryfall returned 429 for %s; retry %s/%s in %.0fs", url, retry_count, retry_limit, delay)
            continue

        # Tor exits are commonly rejected by Scryfall/CDN. Preserve a normal
        # 404 response, but fall back directly for blocked/server-failure paths.
        if using_tor and (response.status_code == httpx.codes.FORBIDDEN or response.status_code >= 500):
            logger.warning(
                "Tor returned HTTP %s for %s; using direct %.0fs fallback",
                response.status_code,
                url,
                FALLBACK_TIMEOUT_SECONDS,
            )
            _DIRECT_HOSTS.add(host)
            using_tor = False
            continue
        return response
