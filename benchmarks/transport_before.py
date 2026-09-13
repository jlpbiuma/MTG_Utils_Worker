"""Bounded, polite transport policy for Scryfall-owned endpoints."""
import asyncio
import logging
import time
from typing import Any
from urllib.parse import urlsplit

import httpx

from src.config import settings
from src.services.tor import async_http_client

logger = logging.getLogger("mtg_worker.scryfall_transport")

FALLBACK_TIMEOUT_SECONDS = 15.0
DIRECT_REQUEST_INTERVAL_SECONDS = 0.1
MAX_429_RETRIES = 10
MAX_TRANSPORT_RETRIES = 3
_DIRECT_HOSTS: set[str] = set()
_direct_request_lock = asyncio.Lock()
_last_direct_request_at = 0.0


async def _pace_direct_request() -> None:
    """Keep direct fallback traffic deliberately below Scryfall's limits."""
    global _last_direct_request_at
    async with _direct_request_lock:
        elapsed = time.monotonic() - _last_direct_request_at
        delay = max(0.0, DIRECT_REQUEST_INTERVAL_SECONDS - elapsed)
        if delay:
            await asyncio.sleep(delay)
        _last_direct_request_at = time.monotonic()


async def request(method: str, url: str, *, headers: dict[str, str], timeout: float = 30.0, **kwargs: Any) -> httpx.Response:
    """Request a Scryfall resource through Tor, then direct only if Tor fails.

    A 429 is never retried immediately: the identical request is retried after
    a linear 2s, 4s, ... backoff, for at most ten retries.
    """
    host = urlsplit(url).netloc
    using_tor = settings.TOR_ENABLED and host not in _DIRECT_HOSTS
    retry_count = 0
    transport_retries = 0
    while True:
        try:
            client_factory = async_http_client if using_tor else httpx.AsyncClient
            request_timeout = timeout if using_tor else FALLBACK_TIMEOUT_SECONDS
            if not using_tor:
                await _pace_direct_request()
            async with client_factory(headers=headers, timeout=request_timeout, follow_redirects=True) as client:
                response = await client.request(method, url, **kwargs)
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

        if response.status_code == httpx.codes.TOO_MANY_REQUESTS:
            if retry_count >= MAX_429_RETRIES:
                return response
            retry_count += 1
            delay = 2.0 * retry_count
            logger.warning("Scryfall returned 429 for %s; retry %s/%s in %.0fs", url, retry_count, MAX_429_RETRIES, delay)
            await asyncio.sleep(delay)
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
