from unittest.mock import AsyncMock, MagicMock

import pytest

from src.rules import RulesWorker


@pytest.mark.asyncio
async def test_unified_worker_syncs_rulings():
    db = MagicMock()
    db.cardcatalog.find_many = AsyncMock(return_value=[MagicMock(id="card-1")])
    db.cardruling.upsert = AsyncMock()
    scryfall = MagicMock(fetch_rulings=AsyncMock(return_value=[{
        "oracle_id": "oracle-1", "source": "wotc", "published_at": "2026-01-01", "comment": "A ruling.",
    }]))
    result = await RulesWorker(db, scryfall).sync_rulings()
    assert result == {"cards_checked": 1, "rulings_upserted": 1, "errors": 0}
    db.cardruling.upsert.assert_awaited_once()
