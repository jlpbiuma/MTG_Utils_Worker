from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.price_history import PriceHistoryWorker, PrintingPrices, materially_changed


def test_threshold_keeps_a_price_when_movement_is_at_most_2_5_percent():
    previous = SimpleNamespace(trendPrice=100.0, minPrice=100.0, maxPrice=100.0)
    assert not materially_changed(previous, (102.5, 102.5, 102.5))
    assert materially_changed(previous, (102.51, 102.51, 102.51))


@pytest.mark.asyncio
async def test_price_history_creates_only_material_changes_and_marks_printing_checked():
    db = MagicMock()
    db.cardprinting.find_many = AsyncMock(return_value=[SimpleNamespace(id="card-1")])
    db.cardpricehistory.find_first = AsyncMock(side_effect=[
        SimpleNamespace(trendPrice=10.0, minPrice=10.0, maxPrice=10.0),
        SimpleNamespace(trendPrice=10.0, minPrice=8.5, maxPrice=13.5),
    ])
    db.cardpricehistory.create = AsyncMock()
    db.cardprinting.update = AsyncMock()
    scryfall = MagicMock(fetch_cards_by_ids=AsyncMock(return_value={
        "card-1": {"prices": {"eur": "10.20", "usd": "5.00"}}
    }))
    worker = PriceHistoryWorker(db, scryfall, now=lambda: datetime(2026, 9, 8, tzinfo=timezone.utc))

    result = await worker.run()

    assert result == {"checked": 1, "snapshots_created": 0}
    db.cardpricehistory.create.assert_not_awaited()
    db.cardprinting.update.assert_awaited_once()


@pytest.mark.asyncio
async def test_price_history_creates_initial_snapshot():
    db = MagicMock()
    db.cardprinting.find_many = AsyncMock(return_value=[SimpleNamespace(id="card-1")])
    db.cardpricehistory.find_first = AsyncMock(return_value=None)
    db.cardpricehistory.create = AsyncMock()
    db.cardprinting.update = AsyncMock()
    scryfall = MagicMock(fetch_cards_by_ids=AsyncMock(return_value={
        "card-1": {"prices": {"eur": "10.00", "usd": "5.00"}}
    }))
    result = await PriceHistoryWorker(db, scryfall).run()
    assert result == {"checked": 1, "snapshots_created": 2}
    assert db.cardpricehistory.create.await_args.kwargs["data"]["cardPrintingId"] == "card-1"


@pytest.mark.asyncio
async def test_cardmarket_and_cardtrader_thresholds_are_evaluated_independently():
    db = MagicMock()
    db.cardprinting.find_many = AsyncMock(return_value=[SimpleNamespace(id="card-1")])
    # Cardmarket moved 2%, while CardTrader's previous quote is sufficiently
    # different to require only a CardTrader historical point.
    db.cardpricehistory.find_first = AsyncMock(side_effect=[
        SimpleNamespace(trendPrice=10.0, minPrice=10.0, maxPrice=10.0),
        SimpleNamespace(trendPrice=9.0, minPrice=9.0, maxPrice=9.0),
    ])
    db.cardpricehistory.create = AsyncMock()
    db.cardprinting.update = AsyncMock()
    scryfall = MagicMock(fetch_cards_by_ids=AsyncMock(return_value={
        "card-1": {"prices": {"eur": "10.20"}}
    }))

    result = await PriceHistoryWorker(db, scryfall).run()

    assert result == {"checked": 1, "snapshots_created": 1}
    assert db.cardpricehistory.create.await_args.kwargs["data"]["provider"] == "cardtrader"
    current = db.cardprinting.update.await_args.kwargs["data"]
    assert current["priceCardmarketTrend"] == 10.2
    assert current["priceCardmarketMin"] == 10.2
    assert current["priceCardmarketMax"] == 10.2
    assert current["priceCardtraderTrend"] == 10.0
    assert current["priceCardtraderMin"] == 8.5
    assert current["priceCardtraderMax"] == 13.5
