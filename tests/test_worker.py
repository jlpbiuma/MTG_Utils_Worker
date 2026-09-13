import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from src.worker import Worker

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
            "icon_svg_uri": "https://svgs.scryfall.test/sets/lea.svg",
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
            "icon_svg_uri": "https://svgs.scryfall.test/sets/mh3.svg",
            "digital": False,
        },
    ])
    storage = MagicMock()
    storage.store_set_icon = AsyncMock(side_effect=["s3://mtg-images/set-icons/lea.svg", "s3://mtg-images/set-icons/mh3.svg"])

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=storage)
    result = await worker.sync_sets_catalog()

    assert result["total_sets"] == 2
    assert result["new_sets_added"] == 1
    assert result["updated_sets"] == 1
    assert result["icons_downloaded"] == 2
    assert result["icon_errors"] == 0

    mock_db.cardset.create.assert_awaited_once()
    mock_db.cardset.update.assert_awaited_once()

    storage.store_set_icon.assert_awaited()
    assert storage.store_set_icon.await_count == 2

@pytest.mark.asyncio
async def test_download_set():
    mock_db = MagicMock()
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardcatalog.find_unique = AsyncMock(return_value=None)
    mock_db.cardcatalog.create = AsyncMock()
    mock_db.cardset.update = AsyncMock()
    mock_db.cardtranslationretry.upsert = AsyncMock()

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
    mock_scryfall.fetch_spanish_cards_for_set = AsyncMock(return_value=[])
    mock_storage = MagicMock()
    mock_storage.store_card_images = AsyncMock(return_value={
        "image_uri": "http://images.test/normal.webp",
        "image_uri_small": "http://images.test/small.webp",
    })

    set_mock = MagicMock()
    set_mock.id = "set-mh3"
    set_mock.code = "mh3"
    set_mock.name = "Modern Horizons 3"
    set_mock.searchUri = "https://api.scryfall.test/cards/search?q=e:mh3"
    set_mock.iconSvgUri = "https://svgs.scryfall.test/sets/mh3.svg"

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_storage)
    summary = await worker.download_set(set_mock)

    assert summary["code"] == "mh3"
    assert summary["cards_fetched"] == 1
    assert summary["printings_processed"] == 1
    assert summary["catalog_cards_added"] == 1
    assert summary["icon_downloaded"] is False
    assert summary["icon_errors"] == 0
    assert summary["errors"] == 0

    mock_db.cardprinting.upsert.assert_awaited_once()
    mock_db.cardcatalog.create.assert_awaited_once()
    mock_db.cardset.update.assert_awaited_once()
    mock_storage.store_card_images.assert_awaited_once()
    printing_data = mock_db.cardprinting.upsert.await_args.kwargs["data"]
    assert printing_data["create"]["set"]["connect"] == {"id": "set-mh3"}
    assert printing_data["update"]["setId"] == "set-mh3"

@pytest.mark.asyncio
async def test_worker_run_cycle():
    mock_db = MagicMock()
    mock_db.cardset.count = AsyncMock(return_value=42)

    worker = Worker(db_client=mock_db)
    worker.sync_sets_catalog = AsyncMock(return_value={"total_sets": 100, "new_sets_added": 2, "updated_sets": 98})
    worker.audit_incomplete_sets = AsyncMock(return_value=3)

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
    worker.backfill_scryfall_images = AsyncMock(return_value={"attempted": 0, "migrated": 0, "errors": 0})
    worker.retry_spanish_translations = AsyncMock(return_value={"attempted": 0, "translated": 0, "exhausted": 0, "errors": 0})

    result = await worker.run()

    assert result["status"] == "success"
    assert result["sets_downloaded_count"] == 1
    assert result["remaining_pending_sets"] == 42
    assert len(result["sets_downloaded"]) == 1
    assert result["image_backfill"]["migrated"] == 0
    assert result["incomplete_sets_requeued"] == 3


@pytest.mark.asyncio
async def test_audit_requeues_sets_with_missing_cards_art_or_prices():
    mock_db = MagicMock()
    mock_db.query_raw = AsyncMock(return_value=[{"id": "set-1", "code": "mh3"}])
    mock_db.cardset.update = AsyncMock()

    count = await Worker(db_client=mock_db).audit_incomplete_sets()

    assert count == 1
    mock_db.cardset.update.assert_awaited_once_with(
        where={"id": "set-1"},
        data={"isDownloaded": False, "downloadedAt": None},
    )


@pytest.mark.asyncio
async def test_download_set_stays_pending_when_any_art_variant_is_missing():
    mock_db = MagicMock()
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardcatalog.find_unique = AsyncMock(return_value=None)
    mock_db.cardcatalog.create = AsyncMock(return_value=MagicMock(id="catalog-1"))
    mock_db.cardset.update = AsyncMock()
    mock_db.cardtranslationretry.upsert = AsyncMock()
    card = {"id": "card-1", "name": "Card", "collector_number": "1", "prices": {"eur": "1.25"}}
    mock_scryfall = MagicMock(
        fetch_cards_for_set=AsyncMock(return_value=[card]),
        fetch_spanish_cards_for_set=AsyncMock(return_value=[]),
    )
    storage = MagicMock()
    storage.store_card_images = AsyncMock(return_value={
        "image_uri": "https://images.test/normal.webp",
        "image_uri_small": None,
        "image_uri_large": "https://images.test/large.webp",
    })
    set_mock = MagicMock(
        id="set-1", code="tst", name="Test", cardCount=1,
        searchUri="https://api.test/search",
    )

    summary = await Worker(mock_db, mock_scryfall, storage).download_set(set_mock)

    assert summary["is_complete"] is False
    assert summary["image_errors"] == 1
    update = mock_db.cardset.update.await_args.kwargs["data"]
    assert update["isDownloaded"] is False
    printing = mock_db.cardprinting.upsert.await_args.kwargs["data"]["create"]
    assert printing["pricesUpdatedAt"] is not None
    assert printing["priceCardmarketTrend"] == 1.25
    assert printing["priceCardtraderTrend"] == 1.23

@pytest.mark.asyncio
async def test_download_set_does_not_write_icons_locally():
    mock_db = MagicMock()
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardcatalog.find_unique = AsyncMock(return_value=None)
    mock_db.cardcatalog.create = AsyncMock()
    mock_db.cardset.update = AsyncMock()
    mock_db.cardtranslationretry.upsert = AsyncMock()

    mock_scryfall = MagicMock()
    mock_scryfall.fetch_cards_for_set = AsyncMock(return_value=[
        {
            "id": "card-1",
            "name": "Test Card",
            "collector_number": "1",
            "image_uris": {"normal": "https://cards.scryfall.test/normal.jpg", "small": "https://cards.scryfall.test/small.jpg"},
            "prices": {},
            "released_at": "2024-06-14",
        }
    ])
    mock_scryfall.fetch_spanish_cards_for_set = AsyncMock(return_value=[])
    mock_storage = MagicMock()
    mock_storage.store_card_images = AsyncMock(return_value={
        "image_uri": "http://images.test/normal.webp",
        "image_uri_small": "http://images.test/small.webp",
    })

    set_mock = MagicMock()
    set_mock.code = "mh3"
    set_mock.name = "Modern Horizons 3"
    set_mock.searchUri = "https://api.scryfall.test/cards/search?q=e:mh3"
    set_mock.iconSvgUri = "https://svgs.scryfall.test/sets/mh3.svg"

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_storage)
    summary = await worker.download_set(set_mock)

    assert summary["icon_downloaded"] is False
    assert summary["icon_errors"] == 0


@pytest.mark.asyncio
async def test_download_set_replaces_legacy_scryfall_catalog_url():
    mock_db = MagicMock()
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardcatalog.find_unique = AsyncMock(
        return_value=MagicMock(imageUri="https://cards.scryfall.io/normal/legacy.jpg")
    )
    mock_db.cardcatalog.update = AsyncMock()
    mock_db.cardset.update = AsyncMock()
    mock_db.cardtranslationretry.upsert = AsyncMock()
    mock_scryfall = MagicMock()
    mock_scryfall.fetch_cards_for_set = AsyncMock(return_value=[{
        "id": "card-legacy", "name": "Legacy Card", "collector_number": "1", "prices": {},
    }])
    mock_scryfall.fetch_spanish_cards_for_set = AsyncMock(return_value=[])
    mock_storage = MagicMock()
    mock_storage.store_card_images = AsyncMock(return_value={
        "image_uri": "http://localhost:8080/images/new.webp",
        "image_uri_small": "http://localhost:8080/images/new-small.webp",
    })
    set_mock = MagicMock(code="tst", name="Test", searchUri="https://api.test/cards", iconSvgUri=None)

    await Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_storage).download_set(set_mock)

    assert mock_db.cardcatalog.update.await_args.kwargs["data"]["imageUri"].endswith("new.webp")


@pytest.mark.asyncio
async def test_download_set_stores_official_spanish_rules_text():
    mock_db = MagicMock()
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardcatalog.find_unique = AsyncMock(return_value=MagicMock(
        imageUri="http://localhost:8080/images/card.webp", oracleTextEs=None
    ))
    mock_db.cardcatalog.update = AsyncMock()
    mock_db.cardset.update = AsyncMock()
    mock_db.cardtranslationretry.upsert = AsyncMock()
    mock_scryfall = MagicMock()
    mock_scryfall.fetch_cards_for_set = AsyncMock(return_value=[{
        "id": "card-es", "name": "Lightning Bolt", "collector_number": "1", "lang": "es",
        "printed_text": "El Relámpago hace 3 puntos de daño a cualquier objetivo.", "prices": {},
    }])
    mock_scryfall.fetch_spanish_cards_for_set = AsyncMock(return_value=[{
        "name": "Lightning Bolt", "lang": "es",
        "printed_text": "El Relámpago hace 3 puntos de daño a cualquier objetivo.",
    }])
    mock_storage = MagicMock()
    mock_storage.store_card_images = AsyncMock(return_value={"image_uri": None, "image_uri_small": None})
    set_mock = MagicMock(code="tst", name="Test", searchUri="https://api.test/cards", iconSvgUri=None)

    await Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_storage).download_set(set_mock)

    assert mock_db.cardcatalog.update.await_args.kwargs["data"]["oracleTextEs"] == "El Relámpago hace 3 puntos de daño a cualquier objetivo."


@pytest.mark.asyncio
async def test_backfill_scryfall_images_updates_printing_and_catalog():
    mock_db = MagicMock()
    printing = MagicMock(
        id="card-legacy",
        imageUri="https://cards.scryfall.io/normal/legacy.jpg",
        normalizedName="legacy card",
    )
    mock_db.cardprinting.find_many = AsyncMock(return_value=[printing])
    mock_db.cardprinting.update = AsyncMock()
    mock_db.cardcatalog.update_many = AsyncMock()
    mock_scryfall = MagicMock()
    mock_scryfall.fetch_cards_by_ids = AsyncMock(return_value={})
    mock_storage = MagicMock()
    mock_storage.store_card_images = AsyncMock(return_value={
        "image_uri": "http://localhost:8080/images/new.webp",
        "image_uri_small": "http://localhost:8080/images/new-small.webp",
    })

    result = await Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_storage).backfill_scryfall_images(limit=1)

    assert result == {"attempted": 1, "migrated": 1, "errors": 0}
    assert mock_db.cardprinting.update.await_args.kwargs["data"]["imageUri"].endswith("new.webp")
    assert mock_db.cardcatalog.update_many.await_args.kwargs["data"]["imageUri"].endswith("new.webp")


@pytest.mark.asyncio
async def test_backfill_resolves_missing_source_by_printing_id():
    mock_db = MagicMock()
    printing = MagicMock(id="card-missing", imageUri=None, catalogId="catalog-1")
    mock_db.cardprinting.find_many = AsyncMock(return_value=[printing])
    mock_db.cardprinting.update = AsyncMock()
    mock_db.cardcatalog.update_many = AsyncMock()
    mock_scryfall = MagicMock()
    mock_scryfall.fetch_cards_by_ids = AsyncMock(return_value={
        "card-missing": {
            "id": "card-missing",
            "image_uris": {
                "normal": "https://cards.scryfall.test/normal.jpg",
                "small": "https://cards.scryfall.test/small.jpg",
            },
        }
    })
    mock_storage = MagicMock()
    mock_storage.store_card_images = AsyncMock(return_value={
        "image_uri": "http://localhost:8080/images/normal.webp",
        "image_uri_small": "http://localhost:8080/images/small.webp",
        "image_uri_large": "http://localhost:8080/images/large.webp",
    })

    result = await Worker(
        db_client=mock_db,
        scryfall_client=mock_scryfall,
        image_storage=mock_storage,
    ).backfill_scryfall_images(limit=1)

    assert result == {"attempted": 1, "migrated": 1, "errors": 0}
    mock_scryfall.fetch_cards_by_ids.assert_awaited_once_with(["card-missing"])
    source = mock_storage.store_card_images.await_args.args[1]
    assert source["normal"] == "https://cards.scryfall.test/normal.jpg"
    update = mock_db.cardprinting.update.await_args.kwargs["data"]
    assert update["imageUriSmall"].endswith("small.webp")
    assert update["imageUriLarge"].endswith("large.webp")

@pytest.mark.asyncio
async def test_download_set_links_printing_to_catalog_id():
    mock_db = MagicMock()
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardcatalog.find_unique = AsyncMock(return_value=None)
    catalog_created = MagicMock()
    catalog_created.id = "catalog-1"
    mock_db.cardcatalog.create = AsyncMock(return_value=catalog_created)
    mock_db.cardset.update = AsyncMock()
    mock_db.cardtranslationretry.upsert = AsyncMock()

    mock_scryfall = MagicMock()
    mock_scryfall.fetch_cards_for_set = AsyncMock(return_value=[
        {
            "id": "card-123",
            "name": "Sol Ring",
            "collector_number": "1",
            "mana_cost": "{1}",
            "type_line": "Artifact",
            "rarity": "uncommon",
            "image_uris": {"normal": "https://cards.scryfall.test/normal.jpg"},
            "prices": {"eur": "1.50"},
            "released_at": "2024-06-14",
        }
    ])
    mock_scryfall.fetch_spanish_cards_for_set = AsyncMock(return_value=[])
    mock_storage = MagicMock()
    mock_storage.store_card_images = AsyncMock(return_value={
        "image_uri": "http://images.test/normal.webp",
        "image_uri_small": "http://images.test/small.webp",
        "image_uri_large": "http://images.test/large.webp",
    })

    set_mock = MagicMock(id="set-mh3", code="mh3", name="Modern Horizons 3", searchUri="https://api.test/search", iconSvgUri=None)

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_storage)
    await worker.download_set(set_mock)

    upsert_call = mock_db.cardprinting.upsert.await_args.kwargs["data"]
    assert upsert_call["create"]["catalog"]["connect"]["id"] == "catalog-1"
    assert upsert_call["create"]["set"]["connect"]["id"] == "set-mh3"
    assert upsert_call["create"]["imageUriLarge"] == "http://images.test/large.webp"
    assert "cardName" not in upsert_call["create"]
    assert "normalizedName" not in upsert_call["create"]


@pytest.mark.asyncio
async def test_download_priority_cards_fetches_and_stores_rulings():
    mock_db = MagicMock()
    catalog_entry = MagicMock()
    catalog_entry.id = "catalog-sol-ring"
    mock_db.cardcatalog.upsert = AsyncMock(return_value=catalog_entry)
    mock_db.cardset.find_unique = AsyncMock(return_value=MagicMock(id="set-c21", code="c21"))
    mock_db.cardprinting.upsert = AsyncMock()
    mock_db.cardruling.upsert = AsyncMock()
    mock_db.collectioncard.update_many = AsyncMock()
    mock_db.deckcard.update_many = AsyncMock()
    mock_db.cardtranslationretry.upsert = AsyncMock()

    mock_scryfall = MagicMock()
    mock_scryfall.fetch_cards_by_names = AsyncMock(return_value=[
        {
            "id": "printing-sol-ring",
            "name": "Sol Ring",
            "oracle_id": "oracle-sol-ring",
            "set": "C21",
            "collector_number": "263",
            "mana_cost": "{1}",
            "type_line": "Artifact",
            "rarity": "uncommon",
            "image_uris": {"normal": "https://cards.scryfall.test/normal.jpg"},
            "prices": {"eur": "2.00"},
            "released_at": "2021-04-23",
        }
    ])
    mock_scryfall.fetch_printing_language = AsyncMock(return_value={
        "id": "printing-sol-ring-es",
        "name": "Sol Ring",
        "printed_name": "Anillo solar",
        "printed_type_line": "Artefacto",
        "printed_text": "{T}: Agrega {C}{C}.",
    })
    mock_scryfall.fetch_rulings = AsyncMock(return_value=[
        {
            "oracle_id": "oracle-sol-ring",
            "source": "wotc",
            "published_at": "2020-09-25",
            "comment": "If tapped for mana, {1} is added to your mana pool.",
        }
    ])
    mock_storage = MagicMock()
    mock_storage.store_card_images = AsyncMock(return_value={
        "image_uri": "http://images.test/normal.webp",
        "image_uri_small": "http://images.test/small.webp",
        "image_uri_large": "http://images.test/large.webp",
    })

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=mock_storage)
    result = await worker.download_priority_cards(["Sol Ring"])

    assert result["downloaded"] == 1
    catalog_payload = mock_db.cardcatalog.upsert.await_args.kwargs["data"]["create"]
    assert catalog_payload["nameEs"] == "Anillo solar"
    assert catalog_payload["detailsEs"].data["oracle_text_es"] == "{T}: Agrega {C}{C}."
    assert catalog_payload["detailsEs"].data["cache_version"] == 2
    assert result["errors"] == 0
    printing_call = mock_db.cardprinting.upsert.await_args.kwargs["data"]
    assert printing_call["create"]["catalog"]["connect"]["id"] == "catalog-sol-ring"
    assert printing_call["create"]["set"]["connect"]["id"] == "set-c21"
    assert printing_call["update"]["setId"] == "set-c21"
    assert printing_call["create"]["imageUriLarge"] == "http://images.test/large.webp"
    assert "cardName" not in printing_call["create"]
    mock_scryfall.fetch_rulings.assert_awaited_once_with("printing-sol-ring")
    ruling_call = mock_db.cardruling.upsert.await_args.kwargs["data"]["create"]
    assert ruling_call["oracleId"] == "oracle-sol-ring"
    assert ruling_call["scryfallCardId"] == "printing-sol-ring"
    assert "tapped" in ruling_call["text"]
    mock_db = MagicMock()
    mock_db.cardset.find_unique = AsyncMock(return_value=None)
    mock_db.cardset.create = AsyncMock()
    mock_db.cardset.update = AsyncMock()

    mock_scryfall = MagicMock()
    mock_scryfall.fetch_all_sets = AsyncMock(return_value=[
        {
            "id": "uuid-1",
            "code": "okset",
            "name": "OK Set",
            "set_type": "expansion",
            "card_count": 100,
            "released_at": "2024-01-01",
            "icon_svg_uri": "https://svgs.scryfall.test/sets/okset.svg",
        },
        {
            "id": "uuid-2",
            "code": "badsvg",
            "name": "Bad SVG",
            "set_type": "expansion",
            "card_count": 50,
            "released_at": "2024-01-02",
            "icon_svg_uri": "https://svgs.scryfall.test/sets/badsvg.svg",
        },
    ])
    storage = MagicMock()
    storage.store_set_icon = AsyncMock(side_effect=["s3://mtg-images/set-icons/okset.svg", Exception("boom")])

    worker = Worker(db_client=mock_db, scryfall_client=mock_scryfall, image_storage=storage)
    result = await worker.sync_sets_catalog()

    assert result["icons_downloaded"] == 1
    assert result["icon_errors"] == 1
