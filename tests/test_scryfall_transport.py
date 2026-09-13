import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

from src.services import scryfall_transport


@pytest.fixture(autouse=True)
def reset_transport():
    scryfall_transport._host_states.clear()
    scryfall_transport._DIRECT_HOSTS.clear()


@pytest.fixture
def clock(monkeypatch):
    current = [100.0]
    original_sleep = asyncio.sleep

    async def sleep(delay):
        current[0] += delay
        await original_sleep(0)

    monkeypatch.setattr(scryfall_transport, "time", SimpleNamespace(monotonic=lambda: current[0]))
    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr(scryfall_transport.random, "uniform", lambda *_: 0.5)
    return current


class FakeClient:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def request(self, _method, _url, **_kwargs):
        return self.response


@pytest.mark.asyncio
async def test_403_from_tor_uses_bounded_direct_fallback():
    scryfall_transport._DIRECT_HOSTS.clear()
    blocked = httpx.Response(403, request=httpx.Request("GET", "https://api.scryfall.test/cards/x"))
    success = httpx.Response(200, request=httpx.Request("GET", "https://api.scryfall.test/cards/x"))
    with (
        patch.object(scryfall_transport.settings, "TOR_ENABLED", True),
        patch("src.services.scryfall_transport.async_http_client", return_value=FakeClient(blocked)),
        patch("src.services.scryfall_transport.httpx.AsyncClient", return_value=FakeClient(success)) as direct,
    ):
        response = await scryfall_transport.request("GET", "https://api.scryfall.test/cards/x", headers={})
    assert response.status_code == 200
    assert direct.call_args.kwargs["timeout"] == 15.0


@pytest.mark.asyncio
async def test_429_retries_the_same_request_after_retry_after(clock):
    rate_limited = httpx.Response(429, headers={"Retry-After": "45"}, request=httpx.Request("GET", "https://api.scryfall.test/cards/x"))
    success = httpx.Response(200, request=httpx.Request("GET", "https://api.scryfall.test/cards/x"))
    clients = [FakeClient(rate_limited), FakeClient(success)]
    with (
        patch.object(scryfall_transport.settings, "TOR_ENABLED", False),
        patch("src.services.scryfall_transport.httpx.AsyncClient", side_effect=clients),
    ):
        response = await scryfall_transport.request("GET", "https://api.scryfall.test/cards/x", headers={})
    assert response.status_code == 200
    assert clock[0] == 145.5


@pytest.mark.asyncio
@pytest.mark.parametrize("tor", [True, False])
async def test_concurrent_collection_requests_are_spaced_on_both_routes(clock, tor):
    times = []

    class Client(FakeClient):
        async def request(self, method, url, **kwargs):
            times.append(clock[0])
            return httpx.Response(200, request=httpx.Request(method, url))

    with (patch.object(scryfall_transport.settings, "TOR_ENABLED", tor),
          patch.object(scryfall_transport, "async_http_client", return_value=Client(None)),
          patch.object(httpx, "AsyncClient", return_value=Client(None))):
        await asyncio.gather(*(scryfall_transport.request("POST", "https://api.scryfall.test/cards/collection", headers={}) for _ in range(4)))
    assert len(times) == 4
    assert all(b - a >= 0.649 for a, b in zip(times, times[1:]))


@pytest.mark.asyncio
async def test_429_pauses_other_tasks_and_keeps_cooldown_when_retries_exhausted(clock):
    calls = []

    class Client(FakeClient):
        async def request(self, method, url, **kwargs):
            calls.append(clock[0])
            return httpx.Response(429 if len(calls) == 1 else 200,
                                  request=httpx.Request(method, url))

    with (patch.object(scryfall_transport.settings, "TOR_ENABLED", False),
          patch.object(scryfall_transport, "MAX_429_RETRIES", 0),
          patch.object(httpx, "AsyncClient", return_value=Client(None))):
        responses = await asyncio.gather(*(scryfall_transport.request("GET", "https://api.scryfall.test/cards/x", headers={}) for _ in range(2)))
    assert [r.status_code for r in responses] == [429, 200]
    assert calls[1] - calls[0] >= 30


@pytest.mark.parametrize("header", ["", "invalid", "-3", "2", "nan", "inf"])
def test_retry_after_never_shortens_cooldown(header):
    assert scryfall_transport._retry_delay(httpx.Response(429, headers={"Retry-After": header}), 0) >= 30


def test_retry_after_http_date():
    date = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=90))
    delay = scryfall_transport._retry_delay(httpx.Response(429, headers={"Retry-After": date}), 0)
    assert 89 <= delay <= 91


@pytest.mark.asyncio
async def test_transport_error_retries_without_accessing_missing_response(clock):
    class Client(FakeClient):
        attempts = 0

        async def request(self, method, url, **kwargs):
            self.attempts += 1
            if self.attempts == 1:
                raise httpx.ConnectError("temporary failure")
            return httpx.Response(200, request=httpx.Request(method, url))

    client = Client(None)
    with (patch.object(scryfall_transport.settings, "TOR_ENABLED", False),
          patch.object(httpx, "AsyncClient", return_value=client)):
        response = await scryfall_transport.request("GET", "https://api.scryfall.test/cards/x", headers={})
    assert response.status_code == 200
    assert client.attempts == 2
