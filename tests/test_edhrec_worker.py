import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport, Response
from prisma import Prisma

from src.main import app
from src.services.edhrec_worker import (
    EdhrecWorker,
    to_edhrec_slug,
    is_commander_candidate_type,
    is_basic_land_name,
)


def test_to_edhrec_slug():
    assert to_edhrec_slug("Atraxa, Praetors' Voice") == "atraxa-praetors-voice"
    assert to_edhrec_slug("Urza, Lord High Artificer") == "urza-lord-high-artificer"
    assert to_edhrec_slug("Y'shtola, Night's Blessed") == "yshtola-nights-blessed"
    assert to_edhrec_slug("Lathril, Blade of the Elves") == "lathril-blade-of-the-elves"
    assert to_edhrec_slug("Fire // Ice") == "fire-ice"
    assert to_edhrec_slug("") == ""


def test_is_commander_candidate_type():
    # Legendary creatures: yes
    assert is_commander_candidate_type("Legendary Creature — Human Wizard") is True
    assert is_commander_candidate_type("Legendary Artifact Creature — Golem") is True

    # Non-legendary creatures: no
    assert is_commander_candidate_type("Creature — Elf Druid") is False

    # Legendary vehicles: yes
    assert is_commander_candidate_type("Legendary Artifact — Vehicle") is True
    assert is_commander_candidate_type("Artifact — Vehicle") is False

    # Legendary planeswalker with clause: yes
    assert is_commander_candidate_type(
        "Legendary Planeswalker — Rowan",
        "Rowan, Fearless Sparkmage can be your commander."
    ) is True

    # Legendary planeswalker without clause: no
    assert is_commander_candidate_type(
        "Legendary Planeswalker — Jace",
        "+1: Draw a card. -2: Target creature can't be blocked."
    ) is False


def test_is_basic_land_name():
    assert is_basic_land_name("Plains") is True
    assert is_basic_land_name("Snow-Covered Island") is True
    assert is_basic_land_name("Command Tower") is False
    assert is_basic_land_name("Sol Ring") is False


@pytest.mark.asyncio
async def test_fetch_top_100():
    mock_db = MagicMock(spec=Prisma)
    mock_client = AsyncMock()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "container": {
            "json_dict": {
                "cardlists": [
                    {
                        "cardviews": [
                            {"name": "Atraxa, Praetors' Voice", "rank": 1, "num_decks": 25000, "id": "card-1"},
                            {"name": "Urza, Lord High Artificer", "rank": 2, "num_decks": 21000, "id": "card-2"},
                        ]
                    }
                ]
            }
        }
    }
    mock_client.get.return_value = mock_resp

    worker = EdhrecWorker(mock_db, http_client=mock_client)
    items = await worker.fetch_top_100()

    assert len(items) == 2
    assert items[0]["name"] == "Atraxa, Praetors' Voice"
    assert items[0]["rank"] == 1
    assert items[1]["name"] == "Urza, Lord High Artificer"


@pytest.mark.asyncio
async def test_sync_top_100_commanders():
    mock_db = MagicMock(spec=Prisma)
    mock_db.edhreccommander = AsyncMock()
    # First doesn't exist, second exists
    mock_db.edhreccommander.find_unique.side_effect = [None, MagicMock(id="existing-id")]

    worker = EdhrecWorker(mock_db)
    worker.fetch_top_100 = AsyncMock(return_value=[
        {"name": "Atraxa, Praetors' Voice", "rank": 1, "num_decks": 25000, "id": "card-1"},
        {"name": "Urza, Lord High Artificer", "rank": 2, "num_decks": 21000, "id": "card-2"},
    ])

    count = await worker.sync_top_100_commanders()
    assert count == 2

    # First created
    mock_db.edhreccommander.create.assert_called_once()
    create_args = mock_db.edhreccommander.create.call_args[1]["data"]
    assert create_args["name"] == "Atraxa, Praetors' Voice"
    assert create_args["isTop100"] is True
    assert create_args["rank"] == 1

    # Second updated
    mock_db.edhreccommander.update.assert_called_once()
    update_args = mock_db.edhreccommander.update.call_args[1]["data"]
    assert update_args["isTop100"] is True
    assert update_args["rank"] == 2


@pytest.mark.asyncio
async def test_discover_candidate_commanders():
    mock_db = MagicMock(spec=Prisma)
    mock_db.cardcatalog = AsyncMock()
    mock_db.edhreccommander = AsyncMock()

    card1 = MagicMock(id="c1", normalizedName="lathril, blade of the elves", typeLine="Legendary Creature — Elf Noble", oracleTextEs=None)
    card1.name = "Lathril, Blade of the Elves"
    card2 = MagicMock(id="c2", normalizedName="llanowar elves", typeLine="Creature — Elf Druid", oracleTextEs=None)
    card2.name = "Llanowar Elves"
    card3 = MagicMock(id="c3", normalizedName="shorikai, genesis engine", typeLine="Legendary Artifact — Vehicle", oracleTextEs=None)
    card3.name = "Shorikai, Genesis Engine"

    mock_db.cardcatalog.find_many.return_value = [card1, card2, card3]
    # card1 is new, card3 already exists
    mock_db.edhreccommander.find_unique.side_effect = [None, MagicMock(id="exists")]

    worker = EdhrecWorker(mock_db)
    added = await worker.discover_candidate_commanders()

    assert added == 1
    mock_db.edhreccommander.create.assert_called_once()
    data = mock_db.edhreccommander.create.call_args[1]["data"]
    assert data["name"] == "Lathril, Blade of the Elves"
    assert data["status"] == "pending"


def test_parse_commander_payload():
    raw = {
        "creature": 2,
        "instant": 2,
        "sorcery": 1,
        "artifact": 2,
        "enchantment": 1,
        "land": 35,
        "basic": 10,
        "nonbasic": 25,
        "container": {
            "json_dict": {
                "card": {"color_identity": ["G", "B"]},
                "cardlists": [
                    {
                        "tag": "creatures",
                        "cardviews": [
                            {"name": "Elvish Archdruid", "num_decks": 90, "potential_decks": 100},
                            {"name": "Priest of Titania", "num_decks": 85, "potential_decks": 100},
                            {"name": "Marwyn, the Nurturer", "num_decks": 80, "potential_decks": 100},
                        ],
                    },
                    {
                        "tag": "instants",
                        "cardviews": [
                            {"name": "Assassin's Trophy", "num_decks": 70, "potential_decks": 100},
                            {"name": "Heroic Intervention", "num_decks": 65, "potential_decks": 100},
                        ],
                    },
                    {
                        "tag": "sorceries",
                        "cardviews": [
                            {"name": "Demonic Tutor", "num_decks": 60, "potential_decks": 100},
                        ],
                    },
                    {
                        "tag": "utilityartifacts",
                        "cardviews": [
                            {"name": "Sol Ring", "num_decks": 99, "potential_decks": 100},
                            {"name": "Arcane Signet", "num_decks": 95, "potential_decks": 100},
                        ],
                    },
                    {
                        "tag": "enchantments",
                        "cardviews": [
                            {"name": "Sylvan Library", "num_decks": 50, "potential_decks": 100},
                        ],
                    },
                    {
                        "tag": "lands",
                        "cardviews": [
                            {"name": "Command Tower", "num_decks": 90, "potential_decks": 100},
                            {"name": "Overgrown Tomb", "num_decks": 80, "potential_decks": 100},
                            {"name": "Forest", "num_decks": 99, "potential_decks": 100},  # basic land should be excluded from canonical nonbasics
                        ],
                    },
                ],
            }
        },
    }

    parsed = EdhrecWorker.parse_commander_payload(raw)

    assert parsed["colorIdentity"] == "BG"
    assert parsed["creatureCount"] == 2
    assert parsed["instantCount"] == 2
    assert parsed["sorceryCount"] == 1
    assert parsed["artifactCount"] == 2
    assert parsed["enchantmentCount"] == 1

    canonical = parsed["canonicalCardNames"]
    # Check top 2 creatures picked
    assert "elvish archdruid" in canonical
    assert "priest of titania" in canonical
    assert "marwyn, the nurturer" not in canonical  # limited by creatureCount=2

    # Check instants, sorcery, artifacts, enchantments
    assert "assassin's trophy" in canonical
    assert "heroic intervention" in canonical
    assert "demonic tutor" in canonical
    assert "sol ring" in canonical
    assert "arcane signet" in canonical
    assert "sylvan library" in canonical

    # Check lands: Command Tower and Overgrown Tomb should be in canonical, Forest excluded
    assert "command tower" in canonical
    assert "overgrown tomb" in canonical
    assert "forest" not in canonical


@pytest.mark.asyncio
async def test_sync_next_pending():
    mock_db = MagicMock(spec=Prisma)
    mock_db.edhreccommander = AsyncMock()

    mock_commander = MagicMock()
    mock_commander.id = "c1"
    mock_commander.name = "Lathril, Blade of the Elves"
    mock_commander.slug = "lathril-blade-of-the-elves"
    mock_commander.colorIdentity = "BG"

    mock_db.edhreccommander.find_first.return_value = mock_commander

    worker = EdhrecWorker(mock_db)
    worker.fetch_commander_details = AsyncMock(return_value={
        "creature": 1,
        "instant": 1,
        "container": {
            "json_dict": {
                "card": {"color_identity": ["B", "G"]},
                "cardlists": [
                    {"tag": "creatures", "cardviews": [{"name": "Priest of Titania", "num_decks": 10, "potential_decks": 10}]},
                    {"tag": "instants", "cardviews": [{"name": "Assassin's Trophy", "num_decks": 10, "potential_decks": 10}]},
                ],
            }
        },
    })

    result = await worker.sync_next_pending()
    assert result["status"] == "synced"
    assert result["name"] == "Lathril, Blade of the Elves"

    mock_db.edhreccommander.update.assert_called_once()
    data = mock_db.edhreccommander.update.call_args[1]["data"]
    assert data["status"] == "synced"
    assert data["creatureCount"] == 1
    assert data["instantCount"] == 1


@pytest.mark.asyncio
async def test_sync_next_pending_handles_404():
    mock_db = MagicMock(spec=Prisma)
    mock_db.edhreccommander = AsyncMock()

    mock_commander = MagicMock()
    mock_commander.id = "c2"
    mock_commander.name = "Obscure Commander"
    mock_commander.slug = "obscure-commander"
    mock_db.edhreccommander.find_first.return_value = mock_commander

    worker = EdhrecWorker(mock_db)
    worker.fetch_commander_details = AsyncMock(return_value={"_status": "not_found"})

    result = await worker.sync_next_pending()
    assert result["status"] == "not_found"

    mock_db.edhreccommander.update.assert_called_once()
    data = mock_db.edhreccommander.update.call_args[1]["data"]
    assert data["status"] == "not_found"


@pytest.mark.asyncio
async def test_edhrec_status_endpoint():
    with patch("src.main.db.edhreccommander") as mock_model:
        mock_model.count = AsyncMock(side_effect=[150, 100, 50, 90, 5, 5])

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/edhrec/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "success"
            assert data["total"] == 150
            assert data["top100"] == 100
            assert data["synced"] == 50
            assert data["rate_limit_seconds"] == 30.0


def test_parser_preserves_all_recommendations_beyond_canonical_quota():
    raw = {
        "creature": 2,
        "container": {"json_dict": {"cardlists": [
            {"tag": "creatures", "cardviews": [
                {"name": f"Elf {i}", "num_decks": 100 - i, "potential_decks": 100}
                for i in range(60)
            ]},
            {"tag": "utilityartifacts", "cardviews": [{"name": "Sol Ring"}]},
            {"tag": "lands", "cardviews": [{"name": "Command Tower"}]},
        ]}},
    }
    parsed = EdhrecWorker.parse_commander_payload(raw)
    assert len(parsed["cardsJson"]["creatures"]) == 60
    assert parsed["cardsJson"]["creatures"][-1]["normalizedName"] == "elf 59"
    assert parsed["canonicalCardNames"] == ["elf 0", "elf 1"]


@pytest.mark.asyncio
async def test_network_failure_is_retried_after_cooldown():
    database = MagicMock(spec=Prisma)
    database.edhreccommander = AsyncMock()
    database.edhreccommander.find_first.return_value = None
    worker = EdhrecWorker(database)
    await worker.sync_next_pending()
    conditions = database.edhreccommander.find_first.call_args.kwargs["where"]["OR"]
    assert any(c.get("status") == "error" and "lt" in c["syncedAt"] for c in conditions)


@pytest.mark.asyncio
async def test_download_persists_every_recommendation_before_marking_synced():
    import httpx
    from types import SimpleNamespace

    raw = {"creature": 2, "container": {"json_dict": {"cardlists": [
        {"tag": "creatures", "cardviews": [{"name": f"Elf {i}"} for i in range(60)]},
        {"tag": "manaartifacts", "cardviews": [{"name": "Sol Ring"}]},
    ]}}}
    database = MagicMock(spec=Prisma)
    database.edhreccommander = AsyncMock()
    database.edhreccommander.find_first.return_value = SimpleNamespace(
        id="lathril", name="Lathril", slug="lathril", colorIdentity="BG"
    )
    def respond(request):
        assert request.url.path == "/pages/commanders/lathril.json"
        return httpx.Response(200, json=raw)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await EdhrecWorker(database, client).sync_next_pending()
    assert result["status"] == "synced"
    saved = database.edhreccommander.update.call_args.kwargs["data"]
    cards = saved["cardsJson"].data
    assert len(cards["creatures"]) == 60
    assert cards["creatures"][-1]["name"] == "Elf 59"
    assert cards["manaartifacts"][0]["name"] == "Sol Ring"
    assert saved["status"] == "synced"
