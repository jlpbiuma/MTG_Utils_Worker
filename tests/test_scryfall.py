import pytest
import respx
import httpx
from datetime import datetime
from src.services.scryfall import ScryfallClient

@pytest.mark.asyncio
async def test_fetch_all_sets():
    client = ScryfallClient(base_url="https://api.scryfall.test")
    mock_data = {
        "object": "list",
        "has_more": False,
        "data": [
            {
                "id": "set-uuid-1",
                "code": "lea",
                "name": "Limited Edition Alpha",
                "set_type": "core",
                "card_count": 295,
                "released_at": "1993-08-05",
                "digital": False,
                "search_uri": "https://api.scryfall.test/cards/search?q=e:lea",
            },
            {
                "id": "set-uuid-2",
                "code": "vma",
                "name": "Vintage Masters",
                "set_type": "masters",
                "card_count": 325,
                "released_at": "2014-06-16",
                "digital": True,
                "search_uri": "https://api.scryfall.test/cards/search?q=e:vma",
            },
        ],
    }

    with respx.mock(base_url="https://api.scryfall.test") as mock:
        mock.get("/sets").respond(status_code=200, json=mock_data)
        sets = await client.fetch_all_sets()

    assert len(sets) == 2
    assert sets[0]["code"] == "lea"
    assert sets[1]["digital"] is True

@pytest.mark.asyncio
async def test_fetch_cards_for_set_pagination():
    client = ScryfallClient(rate_limit_delay=0.0)
    page1 = {
        "object": "list",
        "has_more": True,
        "next_page": "https://api.scryfall.test/cards/page2",
        "data": [{"id": "card-1", "name": "Card One"}],
    }
    page2 = {
        "object": "list",
        "has_more": False,
        "data": [{"id": "card-2", "name": "Card Two"}],
    }

    with respx.mock() as mock:
        mock.get("https://api.scryfall.test/cards/page1").respond(status_code=200, json=page1)
        mock.get("https://api.scryfall.test/cards/page2").respond(status_code=200, json=page2)
        cards = await client.fetch_cards_for_set("https://api.scryfall.test/cards/page1", delay_seconds=0.0)

    assert len(cards) == 2
    assert cards[0]["id"] == "card-1"
    assert cards[1]["id"] == "card-2"

def test_extract_image_uris():
    # Single-faced card
    card_single = {
        "image_uris": {
            "small": "https://cards.scryfall.io/small/1.jpg",
            "normal": "https://cards.scryfall.io/normal/1.jpg",
        }
    }
    images = ScryfallClient.extract_image_uris(card_single)
    assert images["normal"] == "https://cards.scryfall.io/normal/1.jpg"
    assert images["small"] == "https://cards.scryfall.io/small/1.jpg"

    # Double-faced card
    card_dfc = {
        "card_faces": [
            {
                "name": "Delver of Secrets",
                "image_uris": {
                    "small": "https://cards.scryfall.io/small/front.jpg",
                    "normal": "https://cards.scryfall.io/normal/front.jpg",
                },
            },
            {
                "name": "Insectile Aberration",
                "image_uris": {
                    "normal": "https://cards.scryfall.io/normal/back.jpg",
                },
            },
        ]
    }
    images_dfc = ScryfallClient.extract_image_uris(card_dfc)
    assert images_dfc["normal"] == "https://cards.scryfall.io/normal/front.jpg"
    assert images_dfc["small"] == "https://cards.scryfall.io/small/front.jpg"

    # Card without images
    assert ScryfallClient.extract_image_uris({}) == {
        "normal": None,
        "small": None,
        "large": None,
        "png": None,
    }


def test_extract_spanish_rules_text_only_uses_official_spanish_printing():
    assert ScryfallClient.extract_spanish_rules_text({
        "lang": "es", "printed_text": "El Relámpago hace 3 puntos de daño a cualquier objetivo."
    }) == "El Relámpago hace 3 puntos de daño a cualquier objetivo."
    assert ScryfallClient.extract_spanish_rules_text({
        "lang": "en", "printed_text": "Lightning Bolt deals 3 damage to any target."
    }) is None
    assert ScryfallClient.extract_spanish_rules_text({
        "lang": "es",
        "card_faces": [{"printed_text": "Cara uno."}, {"printed_text": "Cara dos."}],
    }) == "Cara uno.\n//\nCara dos."


def test_extract_full_rules_text_includes_every_card_face():
    card = {"card_faces": [
        {"oracle_text": "Elige uno —\n• Roba una carta."},
        {"oracle_text": "Añade {G}."},
    ]}
    assert ScryfallClient.extract_full_rules_text(card) == "Elige uno —\n• Roba una carta.\n//\nAñade {G}."


def test_spanish_search_uri_preserves_set_query_and_filters_spanish():
    uri = ScryfallClient.spanish_search_uri(
        "https://api.scryfall.test/cards/search?include_extras=true&order=set&q=e%3Asos&unique=prints"
    )

    assert "include_extras=true" in uri
    assert "q=e%3Asos+lang%3Aes" in uri

def test_parse_float_price():
    assert ScryfallClient.parse_float_price("1.25") == 1.25
    assert ScryfallClient.parse_float_price(2.5) == 2.5
    assert ScryfallClient.parse_float_price(None) is None
    assert ScryfallClient.parse_float_price("") is None
    assert ScryfallClient.parse_float_price("invalid") is None

def test_parse_date():
    dt = ScryfallClient.parse_date("2024-06-14")
    assert dt == datetime(2024, 6, 14)
    assert ScryfallClient.parse_date(None) is None
    assert ScryfallClient.parse_date("not-a-date") is None
