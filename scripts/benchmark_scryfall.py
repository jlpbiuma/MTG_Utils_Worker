"""Read-only comparison. Live mode aborts on the first 429; never writes the DB.

Run from worker: .venv/bin/python scripts/benchmark_scryfall.py --mode simulated
Live measures direct egress with Tor disabled, not production Tor performance.
"""
import argparse
import asyncio
import importlib.util
import json
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.services import scryfall_transport as current

NAMES = ["Lightning Bolt", "Sol Ring", "Counterspell", "Swords to Plowshares"]
URL = "https://api.scryfall.com/cards/collection"
HEADERS = {"User-Agent": "MTGUtilsWorker/1.0 (read-only diagnostic)", "Accept": "application/json"}


async def run(args):
    transport = current
    if args.transport:
        spec = importlib.util.spec_from_file_location("baseline_transport", args.transport)
        transport = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(transport)
    transport.settings.TOR_ENABLED = args.mode == "simulated"
    started = time.monotonic()
    events, results = [], []
    original_sleep, monotonic = asyncio.sleep, time.monotonic
    scale = 50 if args.mode == "simulated" else 1
    now = lambda: (monotonic() - started) * scale
    blocked_until, last, injected = 0.0, -100.0, False

    class StopOn429(BaseException):
        pass

    original_request = httpx.AsyncClient.request

    async def live_request(client, method, url, **kwargs):
        response = await original_request(client, method, url, **kwargs)
        events.append({"at": round(now(), 3), "status": response.status_code})
        if response.status_code == 429:
            raise StopOn429()
        return response

    class Server:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def request(self, method, url, **kwargs):
            nonlocal blocked_until, last, injected
            at = now()
            headers = {}
            if args.scenario == "retry-after" and not injected:
                injected = True
                blocked_until = at + 45
                headers["Retry-After"] = "45"
                status = 429
            elif at < blocked_until:
                if args.extend_cooldown:
                    blocked_until = max(blocked_until, at + 30)
                status = 429
            elif at - last < 0.5:
                blocked_until = at + 30
                status = 429
            else:
                status = 200
            last = at
            events.append({"at": round(at, 3), "status": status})
            return httpx.Response(status, headers=headers,
                json={"data": [{"name": kwargs["json"]["identifiers"][0]["name"]}]},
                request=httpx.Request(method, url))

    async def lookup(name):
        begin = now()
        try:
            response = await transport.request("POST", URL, headers=HEADERS,
                json={"identifiers": [{"name": name}]})
            response.raise_for_status()
            found = [card["name"] for card in response.json().get("data", [])]
            results.append({"name": name, "ok": name in found, "returned": found,
                            "seconds": round(now() - begin, 3)})
        except (Exception, StopOn429) as error:
            results.append({"name": name, "ok": False, "error": type(error).__name__,
                            "seconds": round(now() - begin, 3)})
            if isinstance(error, StopOn429):
                raise

    if args.mode == "simulated":
        async def sleep(seconds):
            await original_sleep(seconds / scale)
        with (patch.object(transport, "async_http_client", Server),
              patch.object(httpx, "AsyncClient", Server),
              patch.object(transport, "time", SimpleNamespace(monotonic=now)),
              patch.object(transport.random, "uniform", return_value=0.5) if hasattr(transport, "random") else nullcontext(),
              patch.object(asyncio, "sleep", sleep)):
            await asyncio.gather(*(lookup(name) for name in NAMES))
    else:
        with patch.object(httpx.AsyncClient, "request", live_request):
            try:
                for name in NAMES:
                    await lookup(name)
                    await original_sleep(1.0)
            except StopOn429:
                pass
    report = {"label": args.label, "mode": args.mode, "scenario": args.scenario,
              "extends_cooldown_on_rejected_request": args.extend_cooldown,
              "route": "mock Tor" if args.mode == "simulated" else "direct",
              "requested": len(NAMES), "completed": sum(r["ok"] for r in results),
              "http_requests": len(events), "http_429": sum(e["status"] == 429 for e in events),
              "elapsed_seconds": round(now(), 3), "results": results, "events": events}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["live", "simulated"], required=True)
    parser.add_argument("--scenario", choices=["burst", "retry-after"], default="burst")
    parser.add_argument("--transport")
    parser.add_argument("--label", default="current")
    parser.add_argument("--extend-cooldown", action="store_true")
    asyncio.run(run(parser.parse_args()))
