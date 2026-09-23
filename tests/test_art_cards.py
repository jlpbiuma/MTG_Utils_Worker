import pytest
import respx
import httpx
from unittest.mock import AsyncMock, MagicMock
from src.services.card_utils import is_art_card, is_playable_card
from src.services.scryfall import ScryfallClient
from src.worker import Worker


def test_is_art_card_and_playable_card():
    # Art series card
    art_card = {
        "id": "18263a99-378d-45af-8bc1-7188ca2a83a5",
        "name": "Cloud, Ex-SOLDIER // Cloud, Ex-SOLDIER",
        "set": "afin",
        "set_type": "memorabilia",
        "layout": "art_series",
        "type_line": "Card // Card",
        "collector_number": "50",
    }
    assert is_art_card(art_card) is True
    assert is_playable_card(art_card) is False

    # Another art card: Humongous Fungus
    humongous_art = {
        "id": "0ee0cdcf-e6cb-4d24-a086-7009a403115f",
        "name": "Humongous Fungus // Humongous Fungus",
        "set": "atmt",
        "set_type": "memorabilia",
        "layout": "art_series",
        "type_line": "Card // Card",
        "collector_number": "45",
    }
    assert is_art_card(humongous_art) is True
    assert is_playable_card(humongous_art) is False

    # Token card
    token_card = {
        "id": "token-1",
        "name": "Soldier",
        "layout": "token",
        "type_line": "Token Creature — Soldier",
    }
    assert is_playable_card(token_card) is False

    # Front card
    front_card = {
        "id": "front-1",
        "name": "Aang",
        "set": "jtla",
        "set_type": "memorabilia",
        "layout": "front_card",
        "type_line": "Card",
    }
    assert is_art_card(front_card) is True
    assert is_playable_card(front_card) is False

    # Playable card: Cloud, Ex-SOLDIER (fic #2)
    playable_cloud = {
        "id": "07b4e4f8-6a31-4533-be51-668ce3ddc84f",
        "name": "Cloud, Ex-SOLDIER",
        "set": "fic",
        "set_type": "commander",
        "layout": "normal",
        "type_line": "Legendary Creature — Human Soldier Mercenary",
        "mana_cost": "{1}{R}{W}{B}",
        "collector_number": "2",
    }
    assert is_art_card(playable_cloud) is False
    assert is_playable_card(playable_cloud) is True

    # Playable card: Corpsejack Menace (tmc #56) - flavor name Humongous Fungus
    corpsejack = {
        "id": "85caf05a-035b-47c0-975c-dec76799ffcf",
        "name": "Corpsejack Menace",
        "flavor_name": "Humongous Fungus",
        "set": "tmc",
        "set_type": "eternal",
        "layout": "normal",
        "type_line": "Creature — Fungus",
        "mana_cost": "{2}{B}{G}",
        "collector_number": "56",
    }
    assert is_art_card(corpsejack) is False
    assert is_playable_card(corpsejack) is True

    # Playable card: Terra, Magical Adept (transform)
    terra = {
        "id": "fbd447aa-588d-4c4d-925e-a7d3bdf6a65c",
        "name": "Terra, Magical Adept // Esper Terra",
        "set": "fin",
        "set_type": "expansion",
        "layout": "transform",
        "type_line": "Legendary Creature — Human Wizard Warrior // Legendary Enchantment Creature — Saga Wizard",
        "collector_number": "245",
    }
    assert is_art_card(terra) is False
    assert is_playable_card(terra) is True


@pytest.mark.asyncio
async def test_fetch_printings_by_name_never_returns_art_series():
    client = ScryfallClient(base_url="https://api.scryfall.test", rate_limit_delay=0.0)

    # Scryfall mock response with both playable card and art series card
    search_payload = {
        "object": "list",
        "total_cards": 2,
        "has_more": False,
        "data": [
            {
                "id": "07b4e4f8-6a31-4533-be51-668ce3ddc84f",
                "name": "Cloud, Ex-SOLDIER",
                "set": "fic",
                "set_type": "commander",
                "collector_number": "2",
                "layout": "normal",
                "type_line": "Legendary Creature — Human Soldier Mercenary",
            },
            {
                "id": "18263a99-378d-45af-8bc1-7188ca2a83a5",
                "name": "Cloud, Ex-SOLDIER // Cloud, Ex-SOLDIER",
                "set": "afin",
                "set_type": "memorabilia",
                "collector_number": "50",
                "layout": "art_series",
                "type_line": "Card // Card",
            },
        ],
    }

    with respx.mock(base_url="https://api.scryfall.test") as mock:
        mock.get("/cards/search").respond(status_code=200, json=search_payload)
        cards = await client.fetch_printings_by_name("Cloud, Ex-SOLDIER")

    # MUST contain only the playable card and NEVER the art series card
    assert len(cards) == 1
    assert cards[0]["id"] == "07b4e4f8-6a31-4533-be51-668ce3ddc84f"
    assert cards[0]["set"] == "fic"
    assert cards[0]["collector_number"] == "2"
    assert cards[0]["layout"] == "normal"
    assert not any(c.get("layout") == "art_series" for c in cards)


@pytest.mark.asyncio
async def test_fetch_printings_by_name_spanish_resolution():
    client = ScryfallClient(base_url="https://api.scryfall.test", rate_limit_delay=0.0)

    # 1. Exact query fails (404)
    # 2. lang:any query succeeds with Spanish printed name and returns canonical name
    # 3. Canonical query returns printings
    canonical_payload = {
        "object": "list",
        "total_cards": 1,
        "has_more": False,
        "data": [
            {
                "id": "42006bd4-f6f0-4638-a50e-46d9d7a54b6e",
                "name": "Kimahri, Valiant Guardian",
                "printed_name": "Kimahri, guardián valiente",
                "lang": "es",
                "set": "fic",
                "set_type": "commander",
                "collector_number": "85",
                "layout": "normal",
                "type_line": "Legendary Creature — Cat Warrior",
            }
        ],
    }

    with respx.mock(base_url="https://api.scryfall.test") as mock:
        # First call with exact English fails
        mock.get(
            "/cards/search",
            params={"q": '!"Kimahri, guardián valiente" game:paper -is:digital -set_type:alchemy -layout:art_series -set_type:memorabilia', "unique": "prints", "order": "released", "dir": "asc", "include_extras": "false"}
        ).respond(status_code=404)

        # Second call with lang:any finds the localized card
        mock.get(
            "/cards/search",
            params={"q": 'lang:any !"Kimahri, guardián valiente" game:paper -is:digital -set_type:alchemy -layout:art_series -set_type:memorabilia', "unique": "prints", "order": "released", "dir": "asc", "include_extras": "false"}
        ).respond(status_code=200, json=canonical_payload)

        # Third call fetches printings for canonical name
        mock.get(
            "/cards/search",
            params={"q": '!"Kimahri, Valiant Guardian" game:paper -is:digital -set_type:alchemy -layout:art_series -set_type:memorabilia', "unique": "prints", "order": "released", "dir": "asc", "include_extras": "false"}
        ).respond(status_code=200, json=canonical_payload)

        cards = await client.fetch_printings_by_name("Kimahri, guardián valiente")

    assert len(cards) == 1
    assert cards[0]["id"] == "42006bd4-f6f0-4638-a50e-46d9d7a54b6e"
    assert cards[0]["name"] == "Kimahri, Valiant Guardian"
    assert cards[0]["set"] == "fic"
    assert cards[0]["collector_number"] == "85"


@pytest.mark.asyncio
async def test_worker_download_priority_cards_discards_art_cards():
    mock_db = MagicMock()
    mock_db.cardcatalog.upsert = AsyncMock()
    mock_db.cardset.find_unique = AsyncMock(return_value=MagicMock(id="set-fic", code="fic"))
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardtranslationretry.upsert = AsyncMock()
    mock_db.collectioncard.update_many = AsyncMock()
    mock_db.deckcard.update_many = AsyncMock()

    mock_scryfall = MagicMock()
    mock_scryfall.fetch_printing_language = AsyncMock(return_value=None)

    mock_images = MagicMock()
    mock_images.store_card_images = AsyncMock(return_value={"image_uri": "http://img/cloud.webp", "image_uri_small": "http://img/cloud_sm.webp", "image_uri_large": "http://img/cloud_lg.webp"})

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_images)

    # Prepared cards list with 1 playable card and 1 art card
    prepared = [
        {
            "id": "07b4e4f8-6a31-4533-be51-668ce3ddc84f",
            "name": "Cloud, Ex-SOLDIER",
            "set": "fic",
            "set_type": "commander",
            "collector_number": "2",
            "layout": "normal",
            "type_line": "Legendary Creature — Human Soldier Mercenary",
            "mana_cost": "{1}{R}{W}{B}",
            "prices": {"usd": "5.00"},
        },
        {
            "id": "18263a99-378d-45af-8bc1-7188ca2a83a5",
            "name": "Cloud, Ex-SOLDIER // Cloud, Ex-SOLDIER",
            "set": "afin",
            "set_type": "memorabilia",
            "collector_number": "50",
            "layout": "art_series",
            "type_line": "Card // Card",
        },
    ]

    result = await worker.download_priority_cards([], prepared_cards=prepared, update_linked_cards=True)

    assert result["downloaded"] == 1
    assert result["errors"] == 0
    # Exactly 1 card catalog upsert (for the playable card, NOT the art card)
    assert mock_db.cardcatalog.upsert.await_count == 1
    catalog_call = mock_db.cardcatalog.upsert.await_args.kwargs["data"]["create"]
    assert catalog_call["id"] == "07b4e4f8-6a31-4533-be51-668ce3ddc84f"
    assert catalog_call["setCode"] == "fic"
    assert catalog_call["collectorNumber"] == "2"
    assert catalog_call["typeLine"] == "Legendary Creature — Human Soldier Mercenary"


@pytest.mark.asyncio
async def test_worker_download_set_skips_memorabilia_sets():
    mock_db = MagicMock()
    mock_db.cardset.update = AsyncMock()
    mock_scryfall = MagicMock()

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall)

    set_obj = MagicMock()
    set_obj.code = "afin"
    set_obj.name = "Final Fantasy Art Series"
    set_obj.setType = "memorabilia"
    set_obj.searchUri = "https://api.scryfall.test/cards/search?q=e:afin"

    result = await worker.download_set(set_obj)

    # Must skip immediately
    assert result["cards_processed"] == 0
    assert result["catalog_added"] == 0
    mock_db.cardset.update.assert_awaited_once()
    mock_scryfall.fetch_cards_for_set.assert_not_called()
