from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.worker import Worker


def test_localized_fields_persist_complete_text_for_both_faces():
    fields = Worker._localized_fields({
        "id": "dfc", "name": "Front // Back", "card_faces": [
            {"name": "Front", "oracle_text": "Texto frontal completo."},
            {"name": "Back", "oracle_text": "Texto trasero completo."},
        ],
    }, None)
    assert fields["oracleTextEs"] == "Texto frontal completo.\n//\nTexto trasero completo."
    assert fields["detailsEs"]["card_faces"][1]["oracle_text_es"] == "Texto trasero completo."


@pytest.mark.asyncio
async def test_retry_spanish_translation_keeps_english_then_retries_weekly_three_times():
    db = MagicMock()
    retry = SimpleNamespace(
        attempts=2,
        cardPrinting=SimpleNamespace(id="card-1", set=SimpleNamespace(code="abc"), collectorNumber="7"),
    )
    db.cardtranslationretry.find_many = AsyncMock(return_value=[retry])
    db.cardtranslationretry.delete = AsyncMock()
    scryfall = MagicMock(fetch_spanish_printing=AsyncMock(return_value=None))

    result = await Worker(db_client=db, scryfall_client=scryfall).retry_spanish_translations()

    assert result == {"attempted": 1, "translated": 0, "exhausted": 1, "errors": 0}
    db.cardtranslationretry.delete.assert_awaited_once_with(where={"cardPrintingId": "card-1"})


@pytest.mark.asyncio
async def test_retry_spanish_translation_updates_catalog_existing_intermediary():
    db = MagicMock()
    catalog = SimpleNamespace(
        normalizedName="test card", name="Test Card", manaCost=None, typeLine="Creature"
    )
    printing = SimpleNamespace(
        id="card-1", set=SimpleNamespace(code="abc"), collectorNumber="7", rarity="common", catalog=catalog
    )
    db.cardtranslationretry.find_many = AsyncMock(return_value=[SimpleNamespace(attempts=0, cardPrinting=printing)])
    db.cardcatalog.update_many = AsyncMock()
    db.cardtranslationretry.delete = AsyncMock()
    scryfall = MagicMock(fetch_spanish_printing=AsyncMock(return_value={"printed_name": "Carta de prueba", "printed_type_line": "Criatura", "printed_text": "Vuela"}))

    result = await Worker(db_client=db, scryfall_client=scryfall).retry_spanish_translations()

    assert result["translated"] == 1
    catalog_call = db.cardcatalog.update_many.await_args.kwargs
    assert catalog_call["where"] == {"normalizedName": "test card"}
    assert catalog_call["data"]["nameEs"] == "Carta de prueba"
    assert catalog_call["data"]["typeLineEs"] == "Criatura"
    assert catalog_call["data"]["oracleTextEs"] == "Vuela"
