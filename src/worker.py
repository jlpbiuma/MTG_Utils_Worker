import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from prisma import Json, Prisma

from src.config import settings
from src.db import db as default_db
from src.services.card_utils import normalize_card_name
from src.services.image_storage import CardImageStorage
from src.services.scryfall import ScryfallClient
from src.price_history import PrintingPrices

logger = logging.getLogger("mtg_worker.worker")


def is_scryfall_image_uri(value: Optional[str]) -> bool:
    """True only for legacy Scryfall card-image URLs that must be migrated."""
    if not value:
        return False
    return urlparse(value).hostname == "cards.scryfall.io"

class Worker:
    """
    Background worker that discovers MTG sets from Scryfall
    and downloads printings and card catalog entries in batches.
    """

    def __init__(
        self,
        db_client: Optional[Prisma] = None,
        scryfall_client: Optional[ScryfallClient] = None,
        image_storage: Optional[CardImageStorage] = None,
    ):
        self.db = db_client or default_db
        self.scryfall = scryfall_client or ScryfallClient()
        self.image_storage = image_storage or CardImageStorage()

    @staticmethod
    def _localized_fields(card: Dict[str, Any], spanish_card: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Spanish values when published; otherwise the complete English source."""
        translated = spanish_card or card
        translated_faces = translated.get("card_faces") or []
        base_faces = card.get("card_faces") or []
        localized_faces = []
        for index, base_face in enumerate(base_faces):
            translated_face = translated_faces[index] if index < len(translated_faces) else base_face
            localized_faces.append({
                "name": base_face.get("name"),
                "name_es": translated_face.get("printed_name") or translated_face.get("name") or base_face.get("name"),
                "mana_cost": base_face.get("mana_cost"),
                "type_line": base_face.get("type_line"),
                "type_line_es": translated_face.get("printed_type_line") or translated_face.get("type_line") or base_face.get("type_line"),
                "oracle_text": ScryfallClient.extract_full_rules_text(base_face),
                "oracle_text_es": ScryfallClient.extract_full_rules_text(translated_face, prefer_printed=True),
                "flavor_text": base_face.get("flavor_text"),
                "flavor_text_es": translated_face.get("flavor_text") or base_face.get("flavor_text"),
            })
        full_text = ScryfallClient.extract_full_rules_text(card)
        full_text_localized = ScryfallClient.extract_full_rules_text(translated, prefer_printed=True) or full_text
        return {
            "nameEs": translated.get("printed_name") or translated.get("name") or card.get("name"),
            "typeLineEs": translated.get("printed_type_line") or translated.get("type_line") or card.get("type_line"),
            "oracleTextEs": full_text_localized,
            "flavorTextEs": translated.get("flavor_text") or card.get("flavor_text"),
            "detailsEs": {
                "cache_version": 2,
                "id": card.get("id"),
                "name": card.get("name"),
                "name_es": translated.get("printed_name") or translated.get("name") or card.get("name"),
                "mana_cost": card.get("mana_cost"),
                "type_line": card.get("type_line"),
                "type_line_es": translated.get("printed_type_line") or translated.get("type_line") or card.get("type_line"),
                "oracle_text": full_text,
                "oracle_text_es": full_text_localized,
                "flavor_text": card.get("flavor_text"),
                "flavor_text_es": translated.get("flavor_text") or card.get("flavor_text"),
                "rarity": card.get("rarity"), "set": card.get("set"),
                "set_name": card.get("set_name"), "collector_number": card.get("collector_number"),
                "artist": card.get("artist"), "image_uris": ScryfallClient.extract_image_uris(card),
                "cmc": card.get("cmc"),
                "power": card.get("power"),
                "toughness": card.get("toughness"),
                "loyalty": card.get("loyalty"),
                "defense": card.get("defense"),
                "rarity_es": card.get("rarity"),
                "has_spanish_print": spanish_card is not None,
                "prices": card.get("prices") or {},
                "card_faces": localized_faces, "legalities": card.get("legalities") or {},
            },
        }

    @staticmethod
    def _spanish_key(card: Dict[str, Any]) -> tuple[str, str]:
        return (normalize_card_name(card.get("name") or ""), str(card.get("collector_number") or ""))

    async def _schedule_spanish_retry(self, card_id: str) -> None:
        await self.db.cardtranslationretry.upsert(
            where={"cardPrintingId": card_id},
            data={"create": {"cardPrintingId": card_id, "nextAttemptAt": datetime.now(timezone.utc) + timedelta(days=7)}, "update": {}},
        )

    async def sync_sets_catalog(self) -> Dict[str, int]:
        """
        Discovers all MTG sets from Scryfall and ensures they exist in `card_sets`.
        """
        logger.info("Syncing MTG sets catalog from Scryfall...")
        raw_sets = await self.scryfall.fetch_all_sets()

        new_sets_added = 0
        updated_sets = 0
        icons_downloaded = 0
        icon_errors = 0

        for s in raw_sets:
            set_id = s.get("id")
            code = (s.get("code") or "").lower()
            name = s.get("name")
            if not set_id or not code or not name:
                continue

            set_type = s.get("set_type", "unknown")
            card_count = int(s.get("card_count", 0))
            released_at = ScryfallClient.parse_date(s.get("released_at"))
            icon_svg_uri = s.get("icon_svg_uri")
            search_uri = s.get("search_uri") or f"https://api.scryfall.com/cards/search?order=set&q=e%3A{code}&unique=prints"
            is_digital = bool(s.get("digital", False))

            stored_icon_uri = None
            if icon_svg_uri:
                try:
                    stored_icon_uri = await self.image_storage.store_set_icon(code, icon_svg_uri)
                    icons_downloaded += 1
                except Exception as e:
                    icon_errors += 1
                    logger.warning("Could not download icon for set %s: %s", code, e)

            existing = await self.db.cardset.find_unique(where={"code": code})
            if not existing:
                await self.db.cardset.create(
                    data={
                        "id": set_id,
                        "code": code,
                        "name": name,
                        "setType": set_type,
                        "cardCount": card_count,
                        "releasedAt": released_at,
                        "iconSvgUri": stored_icon_uri,
                        "searchUri": search_uri,
                        "isDigital": is_digital,
                        "isDownloaded": False,
                    }
                )
                new_sets_added += 1
            else:
                # Update metadata if changed
                update_data = {
                    "name": name,
                    "setType": set_type,
                    "cardCount": card_count,
                    "searchUri": search_uri,
                    "isDigital": is_digital,
                }
                # Do not replace a valid MinIO URI with a transient download
                # failure; a later catalogue cycle will retry the SVG upload.
                if stored_icon_uri:
                    update_data["iconSvgUri"] = stored_icon_uri
                await self.db.cardset.update(
                    where={"code": code},
                    data=update_data,
                )
                updated_sets += 1

        logger.info(
            f"Set catalog sync finished: {len(raw_sets)} total sets, "
            f"{new_sets_added} newly registered, {updated_sets} updated."
        )
        return {
            "total_sets": len(raw_sets),
            "new_sets_added": new_sets_added,
            "updated_sets": updated_sets,
            "icons_downloaded": icons_downloaded,
            "icon_errors": icon_errors,
        }

    async def get_pending_sets(self, limit: Optional[int] = None) -> List[Any]:
        """
        Retrieves sets that have not yet been downloaded.
        Orders by release date descending to prioritize newer / standard sets.
        """
        fetch_limit = limit or settings.SETS_PER_CYCLE
        where_clause: Dict[str, Any] = {"isDownloaded": False}

        where_clause["isDigital"] = False
        where_clause["setType"] = {"not_in": ["memorabilia", "token", "alchemy"]}
        where_clause["releasedAt"] = {"lte": datetime.now(timezone.utc)}
        pending_sets = await self.db.cardset.find_many(
            where=where_clause,
            take=fetch_limit,
            order=[{"lastAttemptAt": "asc"}, {"releasedAt": "desc"}],
        )
        return pending_sets

    async def audit_incomplete_sets(self, limit: int = 100) -> int:
        """Requeue sets whose card rows, artwork, or provider-price refresh are incomplete."""
        rows = await self.db.query_raw(
            """
            SELECT cs.id, cs.code
            FROM card_sets cs
            LEFT JOIN card_printings cp ON cp.set_id = cs.id
            WHERE cs.is_downloaded = TRUE
            GROUP BY cs.id, cs.code, cs.card_count, cs.released_at
            HAVING COUNT(cp.id) < cs.card_count
                OR COUNT(cp.id) FILTER (WHERE cp.image_uri IS NULL) > 0
                OR COUNT(cp.id) FILTER (WHERE cp.prices_updated_at IS NULL) > 0
            ORDER BY cs.released_at DESC NULLS LAST
            LIMIT $1
            """,
            limit,
        )
        for row in rows:
            set_id = row.get("id") if isinstance(row, dict) else row.id
            await self.db.cardset.update(
                where={"id": set_id},
                data={"isDownloaded": False, "downloadedAt": None},
            )
        if rows:
            logger.info("Requeued %s incomplete set(s).", len(rows))
        return len(rows)

    async def _store_set_images(self, cards: List[Dict[str, Any]]) -> Dict[str, Dict[str, Optional[str]]]:
        """Mirror artwork concurrently with a small bound, while preserving per-card failures."""
        semaphore = asyncio.Semaphore(max(1, settings.IMAGE_DOWNLOAD_CONCURRENCY))

        async def store(card: Dict[str, Any]) -> tuple[str, Dict[str, Optional[str]]]:
            card_id = card.get("id") or ""
            if not card_id:
                return card_id, {"image_uri": None, "image_uri_small": None, "image_uri_large": None}
            async with semaphore:
                try:
                    images = await self.image_storage.store_card_images(
                        card_id, ScryfallClient.extract_image_uris(card)
                    )
                except Exception as error:
                    logger.warning("Could not store image for card %s: %s", card_id, error)
                    images = {"image_uri": None, "image_uri_small": None, "image_uri_large": None}
                return card_id, images

        return dict(await asyncio.gather(*(store(card) for card in cards)))

    async def download_set(self, set_obj: Any) -> Dict[str, Any]:
        """
        Downloads all card printings for a specific set and updates CardPrinting and CardCatalog.
        Printings reference their catalog entry through `catalogId`.
        """
        code = set_obj.code.lower()
        set_id = set_obj.id
        set_type = (getattr(set_obj, "setType", "") or "").lower()
        if set_type in ("memorabilia", "token") or (len(code) == 4 and code.startswith("a") and set_type == "memorabilia"):
            logger.info(f"Skipping non-playable/memorabilia set [{code}] '{set_obj.name}' (type={set_type})")
            await self.db.cardset.update(where={"code": code}, data={"isDownloaded": True, "downloadedAt": datetime.now(timezone.utc)})
            return {"cards_processed": 0, "catalog_added": 0, "errors": 0}

        logger.info(f"Downloading set [{code}] '{set_obj.name}' via {set_obj.searchUri}...")

        # Icons are copied to MinIO while synchronising the set catalogue. Do
        # not re-download them locally for every card-printing import.
        icon_downloaded = False
        icon_errors = 0

        from src.services.card_utils import is_playable_card
        raw_cards = await self.scryfall.fetch_cards_for_set(set_obj.searchUri)
        cards = [c for c in raw_cards if is_playable_card(c)]
        spanish_cards = await self.scryfall.fetch_spanish_cards_for_set(set_obj.searchUri)
        images_by_card = await self._store_set_images(cards)
        spanish_by_printing = {self._spanish_key(card): card for card in spanish_cards if card.get("name")}
        spanish_by_name = {normalize_card_name(card.get("name")): card for card in spanish_cards if card.get("name")}
        printings_processed = 0
        catalog_added = 0
        errors = 0
        image_errors = 0

        for card in cards:
            try:
                card_id = card.get("id")
                card_name = card.get("name")
                if not card_id or not card_name:
                    continue

                normalized = normalize_card_name(card_name)
                collector_num = str(card.get("collector_number", ""))
                mana_cost = card.get("mana_cost")
                type_line = card.get("type_line")
                rarity = card.get("rarity")
                spanish_card = spanish_by_printing.get(self._spanish_key(card)) or spanish_by_name.get(normalized)
                localized = self._localized_fields(card, spanish_card)
                images = images_by_card[card_id]
                if not all(images.get(key) for key in ("image_uri", "image_uri_small", "image_uri_large")):
                    image_errors += 1
                prices = card.get("prices") or {}

                price_eur = ScryfallClient.parse_float_price(prices.get("eur"))
                price_eur_foil = ScryfallClient.parse_float_price(prices.get("eur_foil"))
                price_usd = ScryfallClient.parse_float_price(prices.get("usd"))
                price_usd_foil = ScryfallClient.parse_float_price(prices.get("usd_foil"))
                provider_prices = PrintingPrices(
                    price_eur, price_eur_foil, price_usd, price_usd_foil
                ).current_provider_data()
                prices_updated_at = datetime.now(timezone.utc)
                released_at = ScryfallClient.parse_date(card.get("released_at"))

                # 1. Ensure CardCatalog entry exists first so every printing
                #    can reference the catalog entry through `catalogId`.
                catalog_entry = await self.db.cardcatalog.find_unique(
                    where={"normalizedName": normalized}
                )
                if not catalog_entry:
                    catalog_entry = await self.db.cardcatalog.create(
                        data={
                            "id": card_id,
                            "name": card_name,
                            "normalizedName": normalized,
                            "manaCost": mana_cost,
                            "typeLine": type_line,
                            "nameEs": localized["nameEs"],
                            "typeLineEs": localized["typeLineEs"],
                            "oracleTextEs": localized["oracleTextEs"],
                            "flavorTextEs": localized["flavorTextEs"],
                            "detailsEs": Json(localized["detailsEs"]),
                            "imageUri": images["image_uri"],
                            "setCode": code,
                            "collectorNumber": collector_num,
                        }
                    )
                    catalog_added += 1
                else:
                    catalog_update = {}
                    if images["image_uri"] and (
                        not catalog_entry.imageUri
                        or is_scryfall_image_uri(catalog_entry.imageUri)
                    ):
                        catalog_update["imageUri"] = images["image_uri"]
                    for field in ("nameEs", "typeLineEs", "oracleTextEs", "flavorTextEs", "detailsEs"):
                        if localized[field] is not None and getattr(catalog_entry, field, None) != localized[field]:
                            catalog_update[field] = Json(localized[field]) if field == "detailsEs" else localized[field]
                    if catalog_update:
                        await self.db.cardcatalog.update(
                            where={"normalizedName": normalized},
                            data=catalog_update,
                        )

                printing_update = {
                    "catalogId": catalog_entry.id,
                    "setId": set_id,
                    "collectorNumber": collector_num,
                    "rarity": rarity,
                    "priceEur": price_eur,
                    "priceEurFoil": price_eur_foil,
                    "priceUsd": price_usd,
                    "priceUsdFoil": price_usd_foil,
                    **provider_prices,
                    "pricesUpdatedAt": prices_updated_at,
                    "releasedAt": released_at,
                }
                # Do not erase a previously stored MinIO URL when a transient
                # upstream image request fails.
                if images.get("image_uri"):
                    printing_update["imageUri"] = images["image_uri"]
                if images.get("image_uri_small"):
                    printing_update["imageUriSmall"] = images["image_uri_small"]
                if images.get("image_uri_large"):
                    printing_update["imageUriLarge"] = images["image_uri_large"]

                # 2. Upsert CardPrinting referencing the catalog entry
                await self.db.cardprinting.upsert(
                    where={"id": card_id},
                    data={
                        "create": {
                            "id": card_id,
                            "catalog": {"connect": {"id": catalog_entry.id}},
                            "set": {"connect": {"id": set_id}},
                            "collectorNumber": collector_num,
                            "rarity": rarity,
                            "imageUri": images.get("image_uri"),
                            "imageUriSmall": images.get("image_uri_small"),
                            "imageUriLarge": images.get("image_uri_large"),
                            "priceEur": price_eur,
                            "priceEurFoil": price_eur_foil,
                            "priceUsd": price_usd,
                            "priceUsdFoil": price_usd_foil,
                            **provider_prices,
                            "pricesUpdatedAt": prices_updated_at,
                            "releasedAt": released_at,
                        },
                        "update": printing_update,
                    },
                )
                if spanish_card is None:
                    await self._schedule_spanish_retry(card_id)
                printings_processed += 1

            except Exception as e:
                logger.error(f"Error processing card {card.get('name')} in set {code}: {e}")
                errors += 1

        expected_count = getattr(set_obj, "cardCount", 0)
        if not isinstance(expected_count, int):
            expected_count = 0
        is_complete = (
            bool(cards)
            and printings_processed == len(cards)
            and len(cards) >= expected_count
            and errors == 0
            and image_errors == 0
        )
        now_utc = datetime.now(timezone.utc)
        await self.db.cardset.update(
            where={"code": code},
            data={
                "isDownloaded": is_complete,
                "downloadedAt": now_utc if is_complete else None,
                "lastAttemptAt": now_utc,
                "downloadAttempts": {"increment": 1},
            },
        )

        logger.info(
            f"Set [{code}] finished: {printings_processed} printings upserted, "
            f"{catalog_added} new catalog cards added, {errors} data errors, "
            f"{image_errors} image errors, complete={is_complete}."
        )

        return {
            "code": code,
            "name": set_obj.name,
            "cards_fetched": len(cards),
            "spanish_cards_fetched": len(spanish_cards),
            "printings_processed": printings_processed,
            "catalog_cards_added": catalog_added,
            "icon_downloaded": icon_downloaded,
            "icon_errors": icon_errors,
            "errors": errors,
            "image_errors": image_errors,
            "is_complete": is_complete,
        }

    async def backfill_scryfall_images(self, limit: Optional[int] = None) -> Dict[str, int]:
        """Gradually replace legacy Scryfall URLs with imgproxy URLs backed by MinIO."""
        batch_size = limit or settings.IMAGE_BACKFILL_PER_CYCLE
        printings = await self.db.cardprinting.find_many(
            where={"OR": [
                {"imageUri": {"startswith": "https://cards.scryfall.io/"}},
                {"imageUri": None},
                {"imageUriSmall": None},
                {"imageUriLarge": None},
            ]},
            take=batch_size,
            order={"updatedAt": "asc"},
        )
        missing_sources = [printing.id for printing in printings if not printing.imageUri]
        cards_by_id = (
            await self.scryfall.fetch_cards_by_ids(missing_sources)
            if missing_sources
            else {}
        )
        migrated = 0
        errors = 0
        for printing in printings:
            try:
                source_images = {"normal": printing.imageUri}
                if not printing.imageUri:
                    source_card = cards_by_id.get(printing.id)
                    if not source_card:
                        raise ValueError("Scryfall did not return artwork metadata")
                    source_images = ScryfallClient.extract_image_uris(source_card)
                images = await self.image_storage.store_card_images(printing.id, source_images)
                if not images["image_uri"]:
                    raise ValueError("image storage returned no public URL")
                data = {"imageUri": images["image_uri"]}
                if images.get("image_uri_small"):
                    data["imageUriSmall"] = images["image_uri_small"]
                if images.get("image_uri_large"):
                    data["imageUriLarge"] = images["image_uri_large"]
                await self.db.cardprinting.update(where={"id": printing.id}, data=data)
                if printing.catalogId:
                    await self.db.cardcatalog.update_many(
                        where={
                            "id": printing.catalogId,
                            "imageUri": {"startswith": "https://cards.scryfall.io/"},
                        },
                        data={"imageUri": images["image_uri"]},
                    )
                migrated += 1
            except Exception as error:
                errors += 1
                logger.warning("Could not backfill image for card %s: %s", printing.id, error)
        return {"attempted": len(printings), "migrated": migrated, "errors": errors}

    async def _store_rulings(self, card_id: str, oracle_id: Optional[str], *, strict: bool = False) -> int:
        """Fetch Scryfall rulings for a card and upsert them into card_rulings."""
        if not oracle_id:
            return 0
        try:
            rulings = None
            if strict:
                from src.services.bulk_catalog import cached_rulings
                rulings = await cached_rulings(self.db, oracle_id)
            if rulings is None:
                rulings = await self.scryfall.fetch_rulings(card_id)
        except Exception as error:
            if strict:
                raise
            logger.warning("Could not fetch rulings for %s: %s", card_id, error)
            return 0
        stored = 0
        now = datetime.now(timezone.utc)
        for item in rulings:
            oracle_id_c, source, date, text = (
                item.get("oracle_id"),
                item.get("source"),
                item.get("published_at"),
                item.get("comment"),
            )
            if not all((oracle_id_c, source, date, text)):
                continue
            digest = hashlib.sha256(f"{source}\x1f{date}\x1f{text}".encode()).hexdigest()
            try:
                await self.db.cardruling.upsert(
                    where={"oracleId_textHash": {"oracleId": oracle_id_c, "textHash": digest}},
                    data={
                        "create": {
                            "oracleId": oracle_id_c,
                            "scryfallCardId": card_id,
                            "source": source,
                            "rulingDate": datetime.fromisoformat(date),
                            "text": text,
                            "textHash": digest,
                        },
                        "update": {"lastSeenAt": now},
                    },
                )
                stored += 1
            except Exception as error:
                if strict:
                    raise
                logger.warning("Could not upsert ruling for %s: %s", card_id, error)
        return stored

    async def download_priority_cards(
        self,
        names: List[str],
        *,
        include_all_printings: bool = False,
        update_linked_cards: bool = True,
        prepared_cards: Optional[List[Dict[str, Any]]] = None,
        strict: bool = False,
    ) -> Dict[str, int]:
        """Immediately enrich cards imported by a user without waiting for sets."""
        if prepared_cards is not None:
            cards = prepared_cards
        elif include_all_printings:
            cards = []
            for name in names:
                cards.extend(await self.scryfall.fetch_printings_by_name(name))
        else:
            cards = await self.scryfall.fetch_cards_by_names(names)

        from src.services.card_utils import is_playable_card
        cards = [c for c in cards if is_playable_card(c)]

        downloaded = errors = 0
        processed_cards: List[Dict[str, Any]] = []
        ruling_oracles: set[str] = set()
        for card in cards:
            try:
                if not is_playable_card(card):
                    continue
                card_id, card_name = card.get("id"), card.get("name")
                code = (card.get("set") or "").lower()
                if not card_id or not card_name:
                    continue
                normalized = normalize_card_name(card_name)
                cached, spanish_card = False, None
                if strict:
                    from src.services.bulk_catalog import cached_spanish
                    cached, spanish_card = await cached_spanish(self.db, code, str(card.get("collector_number") or ""))
                if not cached:
                    spanish_card = await self.scryfall.fetch_printing_language(
                        code, str(card.get("collector_number") or ""), "es"
                    )
                localized = self._localized_fields(card, spanish_card)
                try:
                    images = await self.image_storage.store_card_images(card_id, ScryfallClient.extract_image_uris(card))
                except Exception as image_error:
                    if strict:
                        raise
                    logger.warning("Could not store priority image for %s: %s", card_id, image_error)
                    images = {
                        "image_uri": None,
                        "image_uri_small": None,
                        "image_uri_large": None,
                    }

                catalog_data = {
                    "id": card_id, "name": card_name, "normalizedName": normalized,
                    "manaCost": card.get("mana_cost"), "typeLine": card.get("type_line"),
                    "nameEs": localized["nameEs"], "typeLineEs": localized["typeLineEs"],
                    "oracleTextEs": localized["oracleTextEs"], "flavorTextEs": localized["flavorTextEs"],
                    "detailsEs": Json(localized["detailsEs"]), "imageUri": images.get("image_uri"),
                    "setCode": code or None, "collectorNumber": str(card.get("collector_number") or "") or None,
                }
                catalog_entry = await self.db.cardcatalog.upsert(
                    where={"normalizedName": normalized},
                    data={"create": catalog_data, "update": {key: value for key, value in catalog_data.items() if key not in {"id", "normalizedName"} and value is not None}},
                )

                # A set may not yet have been catalogued during the very first
                # bootstrap. The catalogue row is still useful immediately.
                card_set = await self.db.cardset.find_unique(where={"code": code}) if code else None
                from src.services.card_utils import is_arena_or_digital_set_code
                is_digital_set = (getattr(card_set, "isDigital", False) is True) or is_arena_or_digital_set_code(code)
                is_alchemy_set = getattr(card_set, "setType", None) == "alchemy"
                is_a_num = str(card.get("collector_number") or "").startswith(("A-", "a-"))
                if card_set and not is_digital_set and not is_alchemy_set and not is_a_num:
                    prices = card.get("prices") or {}
                    current_prices = PrintingPrices.from_scryfall(card)
                    price_data = {
                        **current_prices.as_db_data(),
                        **current_prices.current_provider_data(),
                        "pricesUpdatedAt": datetime.now(timezone.utc),
                    }
                    await self.db.cardprinting.upsert(
                        where={"id": card_id},
                        data={
                            "create": {
                                "id": card_id, "catalog": {"connect": {"id": catalog_entry.id}},
                                "set": {"connect": {"id": card_set.id}}, "collectorNumber": str(card.get("collector_number") or ""),
                                "rarity": card.get("rarity"),
                                "imageUri": images.get("image_uri"), "imageUriSmall": images.get("image_uri_small"),
                                "imageUriLarge": images.get("image_uri_large"),
                                **price_data,
                                "releasedAt": ScryfallClient.parse_date(card.get("released_at")),
                            },
                            "update": {
                                "catalogId": catalog_entry.id,
                                "setId": card_set.id,
                                "imageUri": images.get("image_uri"),
                                "imageUriSmall": images.get("image_uri_small"),
                                "imageUriLarge": images.get("image_uri_large"),
                                **price_data,
                            },
                        },
                    )
                    await self._schedule_spanish_retry(card_id)

                # Fetch and store rulings immediately for the requested card.
                oracle_id = card.get("oracle_id")
                if oracle_id and oracle_id not in ruling_oracles:
                    if strict:
                        await self._store_rulings(card_id, oracle_id, strict=True)
                    else:
                        await self._store_rulings(card_id, oracle_id)
                    ruling_oracles.add(oracle_id)

                if update_linked_cards:
                    metadata = {"cardScryfallId": card_id, "manaCost": card.get("mana_cost"), "typeLine": card.get("type_line"), "imageUri": images.get("image_uri"), "setCode": code or None}
                    await self.db.collectioncard.update_many(where={"cardName": {"equals": card_name, "mode": "insensitive"}, "enrichmentKey": None}, data=metadata)
                    await self.db.deckcard.update_many(where={"cardName": {"equals": card_name, "mode": "insensitive"}}, data=metadata)
                processed_cards.append(card)
                downloaded += 1
            except Exception as error:
                if strict:
                    raise
                errors += 1
                logger.warning("Could not prioritize imported card %s: %s", card.get("name"), error)
        return {"requested": len(names), "downloaded": downloaded, "errors": errors, "cards": processed_cards}

    async def retry_spanish_translations(self, limit: int = 100) -> Dict[str, int]:
        """Try Spanish again weekly, at most three times, without exposing a card flag."""
        retries = await self.db.cardtranslationretry.find_many(
            where={"nextAttemptAt": {"lte": datetime.now(timezone.utc)}},
            take=limit,
            include={"cardPrinting": {"include": {"catalog": True, "set": True}}},
            order={"nextAttemptAt": "asc"},
        )
        translated = exhausted = errors = 0
        for retry in retries:
            printing = retry.cardPrinting
            catalog = getattr(printing, "catalog", None)
            set_code = printing.set.code
            try:
                spanish_card = await self.scryfall.fetch_spanish_printing(set_code, printing.collectorNumber)
                if not spanish_card and catalog is None:
                    # Without a catalog link there is no localized base to fall
                    # back on; keep the row for a later retry.
                    if retry.attempts >= 2:
                        await self.db.cardtranslationretry.delete(where={"cardPrintingId": printing.id})
                        exhausted += 1
                    continue
                if spanish_card:
                    base = {
                        "id": printing.id,
                        "name": catalog.name if catalog else printing.cardName if hasattr(printing, "cardName") else "",
                        "mana_cost": catalog.manaCost if catalog else None,
                        "type_line": catalog.typeLine if catalog else None,
                        "rarity": printing.rarity,
                        "set": set_code,
                        "collector_number": printing.collectorNumber,
                    }
                    base = {k: v for k, v in base.items() if v is not None}
                    localized = self._localized_fields(base, spanish_card)
                    if catalog is not None:
                        await self.db.cardcatalog.update_many(
                            where={"normalizedName": catalog.normalizedName},
                            data={key: localized[key] for key in ("nameEs", "typeLineEs", "oracleTextEs", "flavorTextEs", "detailsEs")},
                        )
                    await self.db.cardtranslationretry.delete(where={"cardPrintingId": printing.id})
                    translated += 1
                elif retry.attempts >= 2:
                    await self.db.cardtranslationretry.delete(where={"cardPrintingId": printing.id})
                    exhausted += 1
                else:
                    await self.db.cardtranslationretry.update(
                        where={"cardPrintingId": printing.id},
                        data={"attempts": {"increment": 1}, "nextAttemptAt": datetime.now(timezone.utc) + timedelta(days=7)},
                    )
            except Exception as error:
                errors += 1
                logger.warning("Could not retry Spanish translation for %s: %s", printing.id, error)
        return {"attempted": len(retries), "translated": translated, "exhausted": exhausted, "errors": errors}

    async def close(self) -> None:
        """Release network resources created for this worker cycle."""
        await self.image_storage.close()

    async def run(self) -> Dict[str, Any]:
        """
        Executes one worker cycle:
        1. Sync set definitions from Scryfall
        2. Pick next N pending sets
        3. Download each set's cards
        """
        cycle_start = datetime.now(timezone.utc)
        logger.info(f"Starting worker synchronization cycle at {cycle_start.isoformat()}...")

        # 1. Sync set catalog
        sets_meta = await self.sync_sets_catalog()

        incomplete_requeued = await self.audit_incomplete_sets()

        # 2. Find pending sets
        pending = await self.get_pending_sets(limit=settings.SETS_PER_CYCLE)
        logger.info(f"Found {len(pending)} pending set(s) to process in this cycle.")

        # 3. Download pending sets
        processed_details = []
        for s in pending:
            # A blocked or malformed individual set must not prevent the price
            # and rules jobs from running in the same unified cycle.
            try:
                detail = await self.download_set(s)
            except Exception as error:
                logger.warning("Could not download set %s: %s", s.code, error)
                detail = {"code": s.code, "name": s.name, "errors": 1, "error": str(error)}
            processed_details.append(detail)
            # Short pause between sets
            if settings.RATE_LIMIT_DELAY_SECONDS > 0:
                await asyncio.sleep(settings.RATE_LIMIT_DELAY_SECONDS)

        image_backfill = await self.backfill_scryfall_images()
        spanish_retries = await self.retry_spanish_translations()

        # Discover new candidate commanders if new sets were registered or downloaded
        if sets_meta.get("new_sets_added", 0) > 0 or len(processed_details) > 0:
            try:
                from src.services.edhrec_worker import EdhrecWorker
                ew = EdhrecWorker(self.db)
                await ew.discover_candidate_commanders()
                if sets_meta.get("new_sets_added", 0) > 0:
                    await ew.sync_top_100_commanders()
            except Exception as e:
                logger.warning("Could not check new commanders for EDHREC: %s", e)

        # Count total remaining pending
        total_remaining = await self.db.cardset.count(where={"isDownloaded": False})

        cycle_end = datetime.now(timezone.utc)
        duration_s = (cycle_end - cycle_start).total_seconds()

        result = {
            "status": "success",
            "timestamp": cycle_end.isoformat(),
            "duration_seconds": round(duration_s, 2),
            "sets_catalog": sets_meta,
            "incomplete_sets_requeued": incomplete_requeued,
            "sets_downloaded_count": len(processed_details),
            "remaining_pending_sets": total_remaining,
            "sets_downloaded": processed_details,
            "image_backfill": image_backfill,
            "spanish_retries": spanish_retries,
        }

        logger.info(
            f"Completed worker cycle in {duration_s:.1f}s. "
            f"Downloaded {len(processed_details)} sets. Remaining: {total_remaining} sets."
        )
        return result
