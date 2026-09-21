from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.price_history import PriceHistoryWorker, PrintingPrices


def test_printing_prices_cardmarket_only():
    card = {
        "prices": {
            "eur": "12.50",
            "eur_foil": "25.00",
            "usd": "15.00",
            "usd_foil": "30.00",
        }
    }
    p = PrintingPrices.from_scryfall(card)
    assert p.eur == 12.50
    assert p.eur_foil == 25.00
    assert p.usd == 15.00
    assert p.usd_foil == 30.00

    current = p.current_provider_data()
    assert current["priceCardmarketTrend"] == 12.50
    assert current["priceCardmarketMin"] == 12.50
    assert current["priceCardmarketMax"] == 25.00
    assert "priceCardtraderTrend" not in current
    assert "priceCardtraderMin" not in current


@pytest.mark.asyncio
async def test_price_history_worker_delegates_to_sync_today():
    db = MagicMock()
    worker = PriceHistoryWorker(db)

    with patch("src.price_history.sync_today", new_callable=AsyncMock) as mock_sync:
        mock_stats = MagicMock(
            status="success",
            feed_date="2026-09-19",
            tracked=100,
            mapped=95,
            matched=90,
            inserted=120,
            anomalies=0,
            conflicts=0,
            notes=[],
        )
        mock_sync.return_value = mock_stats

        res = await worker.run(force=True)

        assert res["status"] == "success"
        assert res["feed_date"] == "2026-09-19"
        assert res["matched"] == 90
        assert res["inserted"] == 120
        mock_sync.assert_awaited_once_with(db, force=True)
