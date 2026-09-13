"""Exercise real worker/client flow offline with real names and synthetic data.

HTTP, database and image storage are doubles; counts are meaningful, elapsed
times are deliberately not reported as production performance.
"""
import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.services.scryfall import ScryfallClient
from src.worker import Worker

ROOT = Path(__file__).resolve().parents[2]
NAMES = json.loads((ROOT / "backend/benchmarks/card_names.json").read_text())


async def scenario(label, *, streaming=False, fail_at=None, uploads=1, unique=5000, coalesce=False, omit=None):
    names = NAMES[:unique]
    counts = {"collection_http": 0, "spanish_http": 0, "rulings_http": 0,
              "image_store_calls": 0, "catalog_upserts": 0}
    stored = set()
    errors, results = [], []

    async def request(method, url, **kwargs):
        counts["collection_http"] += 1
        req = httpx.Request(method, url)
        if counts["collection_http"] == fail_at:
            # Transport already exhausted its retry budget, like production logs.
            return httpx.Response(429, request=req)
        data = []
        for item in kwargs["json"]["identifiers"]:
            name = item["name"]
            if name == omit:
                continue
            data.append({"id": "id:" + name, "oracle_id": "oracle:" + name,
                         "name": name, "set": "tst", "collector_number": "1"})
        return httpx.Response(200, json={"data": data, "not_found": [{"name": omit}] if omit else []}, request=req)

    async def spanish(*args):
        counts["spanish_http"] += 1
        return None

    async def images(*args):
        counts["image_store_calls"] += 1
        return {"image_uri": None}

    async def rulings(*args):
        counts["rulings_http"] += 1
        return 0

    async def upsert(**kwargs):
        counts["catalog_upserts"] += 1
        card = kwargs["data"]["create"]
        stored.add(card["name"])
        return SimpleNamespace(id=card["id"])

    client = ScryfallClient(rate_limit_delay=0)
    client.tor.request_new_identity = AsyncMock(return_value=False)
    client.fetch_printing_language = spanish
    db = MagicMock()
    db.cardcatalog.upsert = upsert
    db.cardset.find_unique = AsyncMock(return_value=None)
    db.collectioncard.update_many = AsyncMock()
    db.deckcard.update_many = AsyncMock()
    storage = MagicMock()
    storage.store_card_images = images
    worker = Worker(db_client=db, scryfall_client=client, image_storage=storage)
    worker._store_rulings = rulings

    async def run_upload():
        try:
            if streaming:
                for start in range(0, len(names), 75):
                    results.append(await worker.download_priority_cards(names[start:start + 75]))
            else:
                results.append(await worker.download_priority_cards(names))
        except httpx.HTTPStatusError as exc:
            errors.append(exc.response.status_code)

    with patch("src.services.scryfall.scryfall_request", request):
        await asyncio.gather(*(run_upload() for _ in range(1 if coalesce else uploads)))
    return {"label": label, "unique_names": unique, "uploads": uploads, "streaming": streaming,
            "coalesced": coalesce, "fail_collection_call": fail_at,
            "stored_unique": len(stored), "pending_unique": unique - len(stored),
            "counts": counts, "task_exceptions": errors,
            "reported_downloaded": sum(r["downloaded"] for r in results),
            "reported_errors": sum(r["errors"] for r in results)}


async def main():
    results = []
    for label, kwargs in [
        ("current_success_5000", {}),
        ("current_failure_last_batch", {"fail_at": 67}),
        ("streaming_failure_last_batch", {"fail_at": 67, "streaming": True}),
        ("current_two_overlapping_uploads", {"uploads": 2, "unique": 1000}),
        ("coalesced_two_overlapping_uploads", {"uploads": 2, "unique": 1000, "coalesce": True}),
        ("current_one_name_not_found", {"omit": NAMES[0]}),
    ]:
        result = await scenario(label, **kwargs)
        results.append(result)
        print(json.dumps(result), flush=True)
    (Path(__file__).parent / "bulk_priority_results.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
