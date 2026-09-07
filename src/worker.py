import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from prisma import Prisma

from src.config import settings
from src.db import db as default_db
from src.services.card_utils import normalize_card_name
from src.services.scryfall import ScryfallClient

logger = logging.getLogger("mtg_set_worker.worker")

class SetWorker:
    """
    Background worker that discovers MTG sets from Scryfall
    and downloads printings and card catalog entries in batches.
    """

    def __init__(
        self,
        db_client: Optional[Prisma] = None,
        scryfall_client: Optional[ScryfallClient] = None,
    ):
        self.db = db_client or default_db
        self.scryfall = scryfall_client or ScryfallClient()

    async def sync_sets_catalog(self) -> Dict[str, int]:
        """
        Discovers all MTG sets from Scryfall and ensures they exist in `card_sets`.
        """
        logger.info("Syncing MTG sets catalog from Scryfall...")
        raw_sets = await self.scryfall.fetch_all_sets()

        new_sets_added = 0
        updated_sets = 0

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
                        "iconSvgUri": icon_svg_uri,
                        "searchUri": search_uri,
                        "isDigital": is_digital,
                        "isDownloaded": False,
                    }
                )
                new_sets_added += 1
            else:
                # Update metadata if changed
                await self.db.cardset.update(
                    where={"code": code},
                    data={
                        "name": name,
                        "setType": set_type,
                        "cardCount": card_count,
                        "iconSvgUri": icon_svg_uri,
                        "searchUri": search_uri,
                        "isDigital": is_digital,
                    }
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
        }

    async def get_pending_sets(self, limit: Optional[int] = None) -> List[Any]:
        """
        Retrieves sets that have not yet been downloaded.
        Orders by release date descending to prioritize newer / standard sets.
        """
        fetch_limit = limit or settings.SETS_PER_CYCLE
        where_clause: Dict[str, Any] = {"isDownloaded": False}

        if not settings.DOWNLOAD_DIGITAL_SETS:
            where_clause["isDigital"] = False

        pending_sets = await self.db.cardset.find_many(
            where=where_clause,
            take=fetch_limit,
            order={"releasedAt": "desc"},
        )
        return pending_sets

    async def download_set(self, set_obj: Any) -> Dict[str, Any]:
        """
        Downloads all card printings for a specific set and updates CardPrinting and CardCatalog.
        """
        code = set_obj.code.lower()
        logger.info(f"Downloading set [{code}] '{set_obj.name}' via {set_obj.searchUri}...")

        cards = await self.scryfall.fetch_cards_for_set(set_obj.searchUri)
        printings_processed = 0
        catalog_added = 0
        errors = 0

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
                images = ScryfallClient.extract_image_uris(card)
                prices = card.get("prices") or {}

                price_eur = ScryfallClient.parse_float_price(prices.get("eur"))
                price_eur_foil = ScryfallClient.parse_float_price(prices.get("eur_foil"))
                price_usd = ScryfallClient.parse_float_price(prices.get("usd"))
                price_usd_foil = ScryfallClient.parse_float_price(prices.get("usd_foil"))
                released_at = ScryfallClient.parse_date(card.get("released_at"))

                # 1. Upsert CardPrinting
                await self.db.cardprinting.upsert(
                    where={"id": card_id},
                    data={
                        "create": {
                            "id": card_id,
                            "cardName": card_name,
                            "normalizedName": normalized,
                            "setCode": code,
                            "collectorNumber": collector_num,
                            "manaCost": mana_cost,
                            "typeLine": type_line,
                            "rarity": rarity,
                            "imageUri": images["image_uri"],
                            "imageUriSmall": images["image_uri_small"],
                            "priceEur": price_eur,
                            "priceEurFoil": price_eur_foil,
                            "priceUsd": price_usd,
                            "priceUsdFoil": price_usd_foil,
                            "releasedAt": released_at,
                        },
                        "update": {
                            "cardName": card_name,
                            "normalizedName": normalized,
                            "setCode": code,
                            "collectorNumber": collector_num,
                            "manaCost": mana_cost,
                            "typeLine": type_line,
                            "rarity": rarity,
                            "imageUri": images["image_uri"],
                            "imageUriSmall": images["image_uri_small"],
                            "priceEur": price_eur,
                            "priceEurFoil": price_eur_foil,
                            "priceUsd": price_usd,
                            "priceUsdFoil": price_usd_foil,
                            "releasedAt": released_at,
                        },
                    },
                )
                printings_processed += 1

                # 2. Enrich CardCatalog if card does not exist yet
                catalog_entry = await self.db.cardcatalog.find_unique(
                    where={"normalizedName": normalized}
                )
                if not catalog_entry:
                    await self.db.cardcatalog.create(
                        data={
                            "id": card_id,
                            "name": card_name,
                            "normalizedName": normalized,
                            "manaCost": mana_cost,
                            "typeLine": type_line,
                            "imageUri": images["image_uri"],
                            "setCode": code,
                            "collectorNumber": collector_num,
                        }
                    )
                    catalog_added += 1
                elif not catalog_entry.imageUri and images["image_uri"]:
                    await self.db.cardcatalog.update(
                        where={"normalizedName": normalized},
                        data={"imageUri": images["image_uri"]},
                    )

            except Exception as e:
                logger.error(f"Error processing card {card.get('name')} in set {code}: {e}")
                errors += 1

        # Mark set as downloaded
        now_utc = datetime.now(timezone.utc)
        await self.db.cardset.update(
            where={"code": code},
            data={
                "isDownloaded": True,
                "downloadedAt": now_utc,
                "cardCount": len(cards),
            },
        )

        logger.info(
            f"Set [{code}] finished: {printings_processed} printings upserted, "
            f"{catalog_added} new catalog cards added, {errors} errors."
        )

        return {
            "code": code,
            "name": set_obj.name,
            "cards_fetched": len(cards),
            "printings_processed": printings_processed,
            "catalog_cards_added": catalog_added,
            "errors": errors,
        }

    async def run(self) -> Dict[str, Any]:
        """
        Executes one worker cycle:
        1. Sync set definitions from Scryfall
        2. Pick next N pending sets
        3. Download each set's cards
        """
        cycle_start = datetime.now(timezone.utc)
        logger.info(f"Starting Set Worker synchronization cycle at {cycle_start.isoformat()}...")

        # 1. Sync set catalog
        sets_meta = await self.sync_sets_catalog()

        # 2. Find pending sets
        pending = await self.get_pending_sets(limit=settings.SETS_PER_CYCLE)
        logger.info(f"Found {len(pending)} pending set(s) to process in this cycle.")

        # 3. Download pending sets
        processed_details = []
        for s in pending:
            detail = await self.download_set(s)
            processed_details.append(detail)
            # Short pause between sets
            if settings.RATE_LIMIT_DELAY_SECONDS > 0:
                await asyncio.sleep(settings.RATE_LIMIT_DELAY_SECONDS)

        # Count total remaining pending
        total_remaining = await self.db.cardset.count(where={"isDownloaded": False})

        cycle_end = datetime.now(timezone.utc)
        duration_s = (cycle_end - cycle_start).total_seconds()

        result = {
            "status": "success",
            "timestamp": cycle_end.isoformat(),
            "duration_seconds": round(duration_s, 2),
            "sets_catalog": sets_meta,
            "sets_downloaded_count": len(processed_details),
            "remaining_pending_sets": total_remaining,
            "sets_downloaded": processed_details,
        }

        logger.info(
            f"Completed Set Worker cycle in {duration_s:.1f}s. "
            f"Downloaded {len(processed_details)} sets. Remaining: {total_remaining} sets."
        )
        return result
