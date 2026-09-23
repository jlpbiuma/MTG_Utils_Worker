import pytest
import respx
from unittest.mock import AsyncMock, MagicMock
from src.services.card_utils import (
    is_digital_or_arena_card,
    is_playable_card,
    is_catalog_record_playable,
    is_arena_or_digital_set_code,
)
from src.services.scryfall import ScryfallClient
from src.worker import Worker


def test_worker_is_arena_or_digital_set_code():
    assert is_arena_or_digital_set_code("ana") is True
    assert is_arena_or_digital_set_code("anb") is True
    assert is_arena_or_digital_set_code("hbg") is True
    assert is_arena_or_digital_set_code("j21") is True
    assert is_arena_or_digital_set_code("y22") is True
    assert is_arena_or_digital_set_code("ydmu") is True
    assert is_arena_or_digital_set_code("ha1") is True
    assert is_arena_or_digital_set_code("aa1") is True
    assert is_arena_or_digital_set_code("tpr") is True
    # Paper sets
    assert is_arena_or_digital_set_code("tmp") is False
    assert is_arena_or_digital_set_code("mrd") is False
    assert is_arena_or_digital_set_code("mh3") is False


def test_worker_is_digital_or_arena_card_checks():
    # A- prefix
    assert is_digital_or_arena_card({"name": "A-Vivi Ornitier", "set": "mh3"}) is True
    assert is_playable_card({"name": "A-Vivi Ornitier", "set": "mh3"}) is False

    # A- collector number
    assert is_digital_or_arena_card({"name": "The One Ring", "collector_number": "A-246", "set": "ltr"}) is True
    assert is_playable_card({"name": "The One Ring", "collector_number": "A-246", "set": "ltr"}) is False

    # Digital True
    assert is_digital_or_arena_card({"name": "Card", "digital": True, "set": "xyz"}) is True
    assert is_playable_card({"name": "Card", "digital": True, "set": "xyz"}) is False

    # Games without paper
    assert is_digital_or_arena_card({"name": "Card", "games": ["arena"], "set": "xyz"}) is True
    assert is_playable_card({"name": "Card", "games": ["arena"], "set": "xyz"}) is False

    # Arena stamp
    assert is_digital_or_arena_card({"name": "Card", "security_stamp": "arena", "set": "xyz"}) is True

    # Alchemy layout or set_type
    assert is_digital_or_arena_card({"name": "Card", "layout": "alchemy", "set": "xyz"}) is True
    assert is_digital_or_arena_card({"name": "Card", "set_type": "alchemy", "set": "xyz"}) is True

    # Real paper card
    real_card = {
        "name": "Vivi Ornitier",
        "set": "fin",
        "collector_number": "42",
        "layout": "normal",
        "games": ["paper", "arena"],
        "digital": False,
        "type_line": "Legendary Creature",
    }
    assert is_digital_or_arena_card(real_card) is False
    assert is_playable_card(real_card) is True


def test_worker_is_catalog_record_playable():
    class DummyCatalog:
        def __init__(self, name, set_code, collector_number, type_line="Creature"):
            self.name = name
            self.setCode = set_code
            self.collectorNumber = collector_number
            self.typeLine = type_line

    assert is_catalog_record_playable(DummyCatalog("A-The One Ring", "ltr", "A-246")) is False
    assert is_catalog_record_playable(DummyCatalog("Real Card", "y22", "10")) is False
    assert is_catalog_record_playable(DummyCatalog("Vivi Ornitier", "fin", "42")) is True


@pytest.mark.asyncio
async def test_worker_fetch_printings_by_name_discards_a_prefix():
    client = ScryfallClient(base_url="https://api.scryfall.test", rate_limit_delay=0.0)
    cards = await client.fetch_printings_by_name("A-Vivi Ornitier")
    assert cards == []


@pytest.mark.asyncio
async def test_worker_fetch_printings_by_name_filters_arena_prints():
    client = ScryfallClient(base_url="https://api.scryfall.test", rate_limit_delay=0.0)

    search_payload = {
        "object": "list",
        "total_cards": 2,
        "has_more": False,
        "data": [
            {
                "id": "paper-1",
                "name": "The One Ring",
                "set": "ltr",
                "collector_number": "246",
                "layout": "normal",
                "type_line": "Legendary Artifact",
                "games": ["paper"],
            },
            {
                "id": "arena-1",
                "name": "The One Ring",
                "set": "ltr",
                "collector_number": "A-246",
                "layout": "normal",
                "type_line": "Legendary Artifact",
                "games": ["arena"],
            },
        ],
    }

    with respx.mock(base_url="https://api.scryfall.test") as mock:
        mock.get("/cards/search").respond(status_code=200, json=search_payload)
        cards = await client.fetch_printings_by_name("The One Ring")

    assert len(cards) == 1
    assert cards[0]["id"] == "paper-1"
    assert cards[0]["collector_number"] == "246"


@pytest.mark.asyncio
async def test_worker_get_pending_sets_excludes_digital_and_alchemy():
    mock_db = MagicMock()
    mock_db.cardset.find_many = AsyncMock(return_value=[])

    worker = Worker(db_client=mock_db)
    await worker.get_pending_sets(limit=5)

    where_clause = mock_db.cardset.find_many.await_args.kwargs["where"]
    assert where_clause["isDigital"] is False
    assert "alchemy" in where_clause["setType"]["not_in"]


@pytest.mark.asyncio
async def test_worker_download_priority_cards_does_not_create_digital_printings():
    mock_db = MagicMock()
    mock_db.cardcatalog.upsert = AsyncMock()
    # Digital set
    mock_digital_set = MagicMock(id="set-aa1", code="aa1", isDigital=True, setType="box")
    mock_db.cardset.find_unique = AsyncMock(return_value=mock_digital_set)
    mock_db.cardprinting.upsert = AsyncMock()

    mock_scryfall = MagicMock()
    mock_images = MagicMock()
    mock_images.store_card_images = AsyncMock(return_value={"image_uri": "http://img.webp"})

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_images)

    # Card payload whose set is digital
    cards = [
        {
            "id": "p-1",
            "name": "Darksteel Plate",
            "set": "aa1",
            "collector_number": "1",
            "layout": "normal",
            "type_line": "Artifact",
        }
    ]

    await worker.download_priority_cards([], prepared_cards=cards)

    # CardPrinting.upsert must NOT be called for a digital set!
    mock_db.cardprinting.upsert.assert_not_called()
