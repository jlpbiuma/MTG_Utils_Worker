"""
Backfill script to find and fix any cards in the database that are Art Series / memorabilia
instead of the real playable MTG cards.

Specifically handles:
- Cloud, Ex-SOLDIER (resolving to fic #2: https://scryfall.com/card/fic/2/cloud-ex-soldier)
- Terra, Magical Adept (resolving to fin #245)
- Kimahri, guardián valiente / Kimahri, Valiant Guardian (resolving to fic #85)
- Humongous Fungus (resolving to Corpsejack Menace tmc #56)
- Any card in card_catalog, user_collections, or deck_cards where layout is art_series or type_line is Card // Card
"""

import asyncio
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db import connect_db, disconnect_db, db
from src.services.card_utils import is_art_card, is_playable_card, normalize_card_name
from src.services.scryfall import ScryfallClient
from src.services.image_storage import CardImageStorage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backfill_art_cards")


async def resolve_playable_card(client: ScryfallClient, name: str, set_code: Optional[str] = None, collector_number: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Resolves the official playable card for a given name and optional set/collector number."""
    # 1. If set and collector number provided, check that exact printing first
    if set_code and collector_number and not (len(set_code) == 4 and set_code.startswith("a")):
        try:
            res = await client._request("GET", f"{client.base_url}/cards/{set_code.lower()}/{collector_number}")
            if res.status_code == 200:
                card = res.json()
                if is_playable_card(card):
                    return card
        except Exception as e:
            logger.debug(f"Direct set/collector lookup failed for {set_code} #{collector_number}: {e}")

    # 2. Query fetch_printings_by_name (which already excludes art series and memorabilia)
    printings = await client.fetch_printings_by_name(name)
    if printings:
        # If set_code matched one of the printings, prefer it
        if set_code:
            for p in printings:
                if (p.get("set") or "").lower() == set_code.lower():
                    return p
        # Otherwise return the primary/standard playable printing (first or lowest collector number in main set)
        return printings[0]

    # 3. Try fuzzy named resolution
    try:
        res = await client._request("GET", f"{client.base_url}/cards/named", headers=client.headers, params={"fuzzy": name})
        if res.status_code == 200:
            card = res.json()
            if is_playable_card(card):
                return card
    except Exception as e:
        logger.debug(f"Named fuzzy lookup failed for {name}: {e}")

    return None


async def run_backfill():
    logger.info("Starting Art Card Backfill...")
    await connect_db()
    client = ScryfallClient(rate_limit_delay=0.1)
    image_storage = CardImageStorage()

    stats = {
        "catalog_checked": 0,
        "catalog_fixed": 0,
        "collections_checked": 0,
        "collections_fixed": 0,
        "decks_checked": 0,
        "decks_fixed": 0,
        "specific_targets_fixed": 0,
    }

    # -------------------------------------------------------------
    # 1. Fix Specific Target Cards Mentioned by User
    # -------------------------------------------------------------
    targets = [
        {"name": "Cloud, Ex-SOLDIER", "set": "fic", "collector_number": "2"},
        {"name": "Terra, Magical Adept // Esper Terra", "set": "fin", "collector_number": "245"},
        {"name": "Terra, Herald of Hope", "set": "fic", "collector_number": "4"},
        {"name": "Kimahri, Valiant Guardian", "set": "fic", "collector_number": "85"},
        {"name": "Kimahri, guardián valiente", "set": "fic", "collector_number": "85"},
        {"name": "Humongous Fungus", "set": "tmc", "collector_number": "56"},
    ]

    logger.info("--- Phase 1: Resolving and fixing specific target cards ---")
    for target in targets:
        name = target["name"]
        playable = await resolve_playable_card(client, name, target.get("set"), target.get("collector_number"))
        if not playable:
            logger.warning(f"Could not resolve playable card for target: {name}")
            continue

        cid = playable["id"]
        cname = playable["name"]
        norm = normalize_card_name(cname)
        cset = playable.get("set")
        cnum = playable.get("collector_number")
        cmana = playable.get("mana_cost")
        ctype = playable.get("type_line")
        images = ScryfallClient.extract_image_uris(playable)
        image_uri = images.get("normal") or images.get("large") or images.get("small")

        # Attempt to mirror image to MinIO if running
        try:
            stored_images = await image_storage.store_card_images(cid, images)
            image_uri = stored_images.get("image_uri") or image_uri
        except Exception:
            pass

        logger.info(f"Target '{name}' resolved to: {cname} [{cset} #{cnum}] (id={cid})")

        # Update or upsert CardCatalog
        catalog_exists = await db.query_raw("SELECT id FROM card_catalog WHERE normalized_name = $1", norm)
        if catalog_exists:
            await db.execute_raw(
                """UPDATE card_catalog 
                   SET id=$1, name=$2, mana_cost=$3, type_line=$4, image_uri=$5, set_code=$6, collector_number=$7, updated_at=NOW()
                   WHERE normalized_name=$8""",
                cid, cname, cmana, ctype, image_uri, cset, cnum, norm
            )
        else:
            await db.execute_raw(
                """INSERT INTO card_catalog(id, name, normalized_name, mana_cost, type_line, image_uri, set_code, collector_number, updated_at)
                   VALUES($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                   ON CONFLICT(normalized_name) DO UPDATE SET
                   id=EXCLUDED.id, name=EXCLUDED.name, mana_cost=EXCLUDED.mana_cost, type_line=EXCLUDED.type_line,
                   image_uri=EXCLUDED.image_uri, set_code=EXCLUDED.set_code, collector_number=EXCLUDED.collector_number, updated_at=NOW()""",
                cid, cname, norm, cmana, ctype, image_uri, cset, cnum
            )

        # Update user_collections matching this card
        clean_target_name = name.split(" // ")[0].strip()
        updated_colls = await db.execute_raw(
            """UPDATE user_collections
               SET card_scryfall_id=$1, card_name=$2, set_code=$3, collector_number=$4, mana_cost=$5, type_line=$6, image_uri=COALESCE($7, image_uri), updated_at=NOW()
               WHERE lower(btrim(card_name)) = lower($8) 
                  OR lower(btrim(card_name)) = lower($2)
                  OR card_scryfall_id LIKE 'custom-' || lower(replace(replace($8, ' ', '-'), ',', '')) || '%'
                  OR card_scryfall_id = 'pending:' || lower($8)""",
            cid, cname, cset, cnum, cmana, ctype, image_uri, clean_target_name
        )
        if updated_colls > 0:
            logger.info(f"Updated {updated_colls} user_collections rows for {cname}")
            stats["collections_fixed"] += updated_colls

        # Update deck_cards matching this card
        updated_decks = await db.execute_raw(
            """UPDATE deck_cards
               SET card_scryfall_id=$1, card_name=$2, mana_cost=$3, type_line=$4, image_uri=COALESCE($5, image_uri)
               WHERE lower(btrim(card_name)) = lower($6) 
                  OR lower(btrim(card_name)) = lower($2)
                  OR card_scryfall_id = 'pending:' || lower($6)""",
            cid, cname, cmana, ctype, image_uri, clean_target_name
        )
        if updated_decks > 0:
            logger.info(f"Updated {updated_decks} deck_cards rows for {cname}")
            stats["decks_fixed"] += updated_decks

        stats["specific_targets_fixed"] += 1

    # -------------------------------------------------------------
    # 2. Audit and Fix CardCatalog
    # -------------------------------------------------------------
    logger.info("--- Phase 2: Auditing CardCatalog for Art Cards ---")
    catalog_rows = await db.query_raw(
        """SELECT id, name, normalized_name, set_code, collector_number, type_line, image_uri
           FROM card_catalog"""
    )
    stats["catalog_checked"] = len(catalog_rows)

    art_catalog_entries = []
    for row in catalog_rows:
        tl = (row.get("type_line") or "").strip()
        sc = (row.get("set_code") or "").strip().lower()
        if tl in ("Card", "Card // Card") or tl.startswith("Card // Card") or (len(sc) == 4 and sc.startswith("a")):
            art_catalog_entries.append(row)

    logger.info(f"Found {len(art_catalog_entries)} suspicious art cards in CardCatalog")
    for entry in art_catalog_entries:
        name = entry["name"]
        logger.info(f"Fixing art card in catalog: '{name}' [{entry.get('set_code')} #{entry.get('collector_number')}]")
        playable = await resolve_playable_card(client, name)
        if playable:
            cid = playable["id"]
            cname = playable["name"]
            norm = normalize_card_name(cname)
            cset = playable.get("set")
            cnum = playable.get("collector_number")
            cmana = playable.get("mana_cost")
            ctype = playable.get("type_line")
            images = ScryfallClient.extract_image_uris(playable)
            image_uri = images.get("normal") or images.get("large") or images.get("small")

            await db.execute_raw(
                """UPDATE card_catalog
                   SET id=$1, name=$2, mana_cost=$3, type_line=$4, image_uri=$5, set_code=$6, collector_number=$7, updated_at=NOW()
                   WHERE normalized_name=$8""",
                cid, cname, cmana, ctype, image_uri, cset, cnum, norm
            )
            stats["catalog_fixed"] += 1

    # -------------------------------------------------------------
    # 3. Audit and Fix user_collections and deck_cards
    # -------------------------------------------------------------
    logger.info("--- Phase 3: Auditing user_collections for Art Series IDs/Sets ---")
    art_colls = await db.query_raw(
        """SELECT id, card_name, card_scryfall_id, set_code, collector_number, type_line
           FROM user_collections
           WHERE type_line IN ('Card', 'Card // Card')
              OR type_line LIKE 'Card // Card%'
              OR (length(set_code) = 4 AND set_code LIKE 'a%')"""
    )
    logger.info(f"Found {len(art_colls)} suspicious user_collections rows")
    for c in art_colls:
        playable = await resolve_playable_card(client, c["card_name"])
        if playable:
            cid = playable["id"]
            cname = playable["name"]
            cset = playable.get("set")
            cnum = playable.get("collector_number")
            cmana = playable.get("mana_cost")
            ctype = playable.get("type_line")
            images = ScryfallClient.extract_image_uris(playable)
            image_uri = images.get("normal") or images.get("large") or images.get("small")

            await db.execute_raw(
                """UPDATE user_collections
                   SET card_scryfall_id=$1, card_name=$2, set_code=$3, collector_number=$4, mana_cost=$5, type_line=$6, image_uri=COALESCE($7, image_uri), updated_at=NOW()
                   WHERE id=$8""",
                cid, cname, cset, cnum, cmana, ctype, image_uri, c["id"]
            )
            stats["collections_fixed"] += 1

    logger.info("--- Phase 4: Auditing deck_cards for Art Series IDs/Sets ---")
    art_decks = await db.query_raw(
        """SELECT id, card_name, card_scryfall_id, type_line
           FROM deck_cards
           WHERE type_line IN ('Card', 'Card // Card')
              OR type_line LIKE 'Card // Card%'"""
    )
    logger.info(f"Found {len(art_decks)} suspicious deck_cards rows")
    for d in art_decks:
        playable = await resolve_playable_card(client, d["card_name"])
        if playable:
            cid = playable["id"]
            cname = playable["name"]
            cmana = playable.get("mana_cost")
            ctype = playable.get("type_line")
            images = ScryfallClient.extract_image_uris(playable)
            image_uri = images.get("normal") or images.get("large") or images.get("small")

            await db.execute_raw(
                """UPDATE deck_cards
                   SET card_scryfall_id=$1, card_name=$2, mana_cost=$3, type_line=$4, image_uri=COALESCE($5, image_uri)
                   WHERE id=$6""",
                cid, cname, cmana, ctype, image_uri, d["id"]
            )
            stats["decks_fixed"] += 1

    await disconnect_db()
    logger.info("Art Card Backfill Completed Successfully.")
    logger.info(f"Summary: {json.dumps(stats, indent=2)}")
    return stats


if __name__ == "__main__":
    asyncio.run(run_backfill())
