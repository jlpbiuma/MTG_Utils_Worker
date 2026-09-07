import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from src.worker import SetWorker

@pytest.mark.asyncio
async def test_sync_sets_catalog_creates_and_updates():
    mock_db = MagicMock()
    mock_db.cardset.find_unique = AsyncMock(side_effect=[None, {"code": "mh3"}])
    mock_db.cardset.create = AsyncMock()
    mock_db.cardset.update = AsyncMock()

    mock_scryfall = MagicMock()
    mock_scryfall.fetch_all_sets = AsyncMock(return_value=[
        {
            "id": "uuid-lea",
            "code": "lea",
            "name": "Limited Edition Alpha",
            "set_type": "core",
            "card_count": 295,
            "released_at": "1993-08-05",
            "search_uri": "https://api.scryfall.test/lea",
            "digital": False,
        },
        {
            "id": "uuid-mh3",
            "code": "mh3",
            "name": "Modern Horizons 3",
            "set_type": "expansion",
            "card_count": 300,
            "released_at": "2024-06-14",
            "search_uri": "https://api.scryfall.test/mh3",
            "digital": False,
        },
    ])

    worker = SetWorker(db_client=mock_db, scryfall_client=mock_scryfall)
    result = await worker.sync_sets_catalog()

    assert result["total_sets"] == 2
    assert result["new_sets_added"] == 1
    assert result["updated_sets"] == 1

    mock_db.cardset.create.assert_awaited_once()
    mock_db.cardset.update.assert_awaited_once()

@pytest.mark.asyncio
async def test_download_set():
    mock_db = MagicMock()
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardcatalog.find_unique = AsyncMock(return_value=None)
    mock_db.cardcatalog.create = AsyncMock()
    mock_db.cardset.update = AsyncMock()

    mock_scryfall = MagicMock()
    mock_scryfall.fetch_cards_for_set = AsyncMock(return_value=[
        {
            "id": "card-123",
            "name": "Sol Ring",
            "collector_number": "1",
            "mana_cost": "{1}",
            "type_line": "Artifact",
            "rarity": "uncommon",
            "image_uris": {"normal": "https://cards.scryfall.test/normal.jpg", "small": "https://cards.scryfall.test/small.jpg"},
            "prices": {"eur": "1.50", "usd": "2.00"},
            "released_at": "2024-06-14",
        }
    ])

    set_mock = MagicMock()
    set_mock.code = "mh3"
    set_mock.name = "Modern Horizons 3"
    set_mock.searchUri = "https://api.scryfall.test/cards/search?q=e:mh3"

    worker = SetWorker(db_client=mock_db, scryfall_client=mock_scryfall)
    summary = await worker.download_set(set_mock)

    assert summary["code"] == "mh3"
    assert summary["cards_fetched"] == 1
    assert summary["printings_processed"] == 1
    assert summary["catalog_cards_added"] == 1
    assert summary["errors"] == 0

    mock_db.cardprinting.upsert.assert_awaited_once()
    mock_db.cardcatalog.create.assert_awaited_once()
    mock_db.cardset.update.assert_awaited_once()

@pytest.mark.asyncio
async def test_worker_run_cycle():
    mock_db = MagicMock()
    mock_db.cardset.count = AsyncMock(return_value=42)

    worker = SetWorker(db_client=mock_db)
    worker.sync_sets_catalog = AsyncMock(return_value={"total_sets": 100, "new_sets_added": 2, "updated_sets": 98})

    set1 = MagicMock()
    set1.code = "set1"
    worker.get_pending_sets = AsyncMock(return_value=[set1])
    worker.download_set = AsyncMock(return_value={
        "code": "set1",
        "name": "Set One",
        "cards_fetched": 10,
        "printings_processed": 10,
        "catalog_cards_added": 2,
        "errors": 0,
    })

    result = await worker.run()

    assert result["status"] == "success"
    assert result["sets_downloaded_count"] == 1
    assert result["remaining_pending_sets"] == 42
    assert len(result["sets_downloaded"]) == 1
