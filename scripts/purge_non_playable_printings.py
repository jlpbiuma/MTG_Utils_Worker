"""
Purge non-playable printings (Art Series / memorabilia / Arena / Alchemy / digital)
from the local catalog and remap user rows to a playable paper printing.

Complements backfill_art_cards.py: that script remaps known art-series catalog
rows; this one deletes residual printings that should never have been stored and
rewrites deck_cards / user_collections / wants that still point at them.

Run from worker/:
  uv run python -m scripts.purge_non_playable_printings
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db import connect_db, disconnect_db, db
from src.services.card_utils import (
    ARENA_AND_DIGITAL_SET_CODES,
    is_non_playable_catalog_fields,
    is_playable_card,
    normalize_card_name,
)
from src.services.scryfall import ScryfallClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("purge_non_playable")


async def resolve_playable(client: ScryfallClient, name: str) -> Optional[Dict[str, Any]]:
    printings = await client.fetch_printings_by_name(name)
    for p in printings or []:
        if is_playable_card(p):
            return p
    try:
        res = await client._request(
            "GET",
            f"{client.base_url}/cards/named",
            headers=client.headers,
            params={"fuzzy": name},
        )
        if res.status_code == 200:
            card = res.json()
            if is_playable_card(card):
                return card
    except Exception as exc:
        logger.debug("fuzzy named failed for %s: %s", name, exc)
    return None


async def remap_user_rows(bad_ids: Set[str], client: ScryfallClient, stats: Dict[str, int]) -> None:
    if not bad_ids:
        return
    id_list = list(bad_ids)

    for table, name_col, extra_set in (
        ("deck_cards", "card_name", False),
        ("user_collections", "card_name", True),
        ("user_wants", "card_name", True),
    ):
        rows = await db.query_raw(
            f"""SELECT id, {name_col} AS card_name, card_scryfall_id
                FROM {table}
                WHERE card_scryfall_id = ANY($1::text[])""",
            id_list,
        )
        logger.info("Remapping %s rows in %s pointing at non-playable printings", len(rows or []), table)
        for row in rows or []:
            playable = await resolve_playable(client, row["card_name"])
            if not playable:
                logger.warning("Could not resolve playable for %s row %s (%s)", table, row["id"], row["card_name"])
                continue
            cid = playable["id"]
            cname = playable["name"]
            images = ScryfallClient.extract_image_uris(playable)
            image_uri = images.get("normal") or images.get("large") or images.get("small")
            cmana = playable.get("mana_cost")
            ctype = playable.get("type_line")
            cset = playable.get("set")
            cnum = playable.get("collector_number")

            if extra_set:
                await db.execute_raw(
                    f"""UPDATE {table}
                       SET card_scryfall_id=$1, card_name=$2, set_code=$3, collector_number=$4,
                           mana_cost=$5, type_line=$6, image_uri=COALESCE($7, image_uri), updated_at=NOW()
                       WHERE id=$8""",
                    cid, cname, cset, cnum, cmana, ctype, image_uri, row["id"],
                )
            else:
                await db.execute_raw(
                    f"""UPDATE {table}
                       SET card_scryfall_id=$1, card_name=$2, mana_cost=$3, type_line=$4,
                           image_uri=COALESCE($5, image_uri)
                       WHERE id=$6""",
                    cid, cname, cmana, ctype, image_uri, row["id"],
                )
            stats["user_rows_remapped"] += 1


async def run_purge() -> Dict[str, int]:
    logger.info("Starting non-playable printings purge...")
    await connect_db()
    client = ScryfallClient(rate_limit_delay=0.1)

    stats = {
        "printings_deleted": 0,
        "catalog_fixed": 0,
        "user_rows_remapped": 0,
        "bad_printings_found": 0,
    }

    arena_codes = sorted(ARENA_AND_DIGITAL_SET_CODES)

    # 1) Find printings on excluded sets / collectors
    bad_printings = await db.query_raw(
        """
        SELECT cp.id, cp.collector_number, cs.code AS set_code, cs.set_type, cs.is_digital,
               cc.name AS catalog_name, cc.type_line, cc.normalized_name
        FROM card_printings cp
        LEFT JOIN card_sets cs ON cs.id = cp.set_id
        LEFT JOIN card_catalog cc ON cc.id = cp.catalog_id
        WHERE coalesce(cp.collector_number, '') LIKE 'A-%'
           OR coalesce(cp.collector_number, '') LIKE 'a-%'
           OR coalesce(cs.is_digital, false) = true
           OR lower(coalesce(cs.set_type, '')) IN ('alchemy', 'memorabilia', 'token')
           OR (
             length(lower(coalesce(cs.code, ''))) = 4
             AND lower(coalesce(cs.code, '')) LIKE 'a%'
           )
           OR lower(coalesce(cs.code, '')) = ANY($1::text[])
           OR lower(coalesce(cs.code, '')) ~ '^y[0-9a-z]{2,3}$'
           OR lower(coalesce(cc.type_line, '')) IN ('card', 'card // card')
           OR lower(coalesce(cc.type_line, '')) LIKE 'card // card%'
           OR coalesce(cc.name, '') LIKE 'A-%'
           OR coalesce(cc.name, '') LIKE 'a-%'
        """,
        arena_codes,
    )
    bad_ids = {row["id"] for row in (bad_printings or []) if row.get("id")}
    stats["bad_printings_found"] = len(bad_ids)
    logger.info("Found %s non-playable printings", len(bad_ids))

    await remap_user_rows(bad_ids, client, stats)

    if bad_ids:
        # Delete price history first if table references printings
        try:
            await db.execute_raw(
                "DELETE FROM card_price_history WHERE card_printing_id = ANY($1::text[])",
                list(bad_ids),
            )
        except Exception:
            logger.debug("card_price_history cleanup skipped", exc_info=True)

        deleted = await db.execute_raw(
            "DELETE FROM card_printings WHERE id = ANY($1::text[])",
            list(bad_ids),
        )
        stats["printings_deleted"] = deleted if isinstance(deleted, int) else len(bad_ids)
        logger.info("Deleted %s printings", stats["printings_deleted"])

    # 2) Fix catalog rows that still look like art/digital
    catalog_rows = await db.query_raw(
        """SELECT id, name, normalized_name, set_code, collector_number, type_line
           FROM card_catalog"""
    )
    for row in catalog_rows or []:
        if not is_non_playable_catalog_fields(
            row.get("name"),
            row.get("type_line"),
            row.get("set_code"),
            row.get("collector_number"),
        ):
            continue
        playable = await resolve_playable(client, row["name"])
        if not playable:
            logger.warning("Could not fix catalog row %s (%s)", row["id"], row["name"])
            continue
        cid = playable["id"]
        cname = playable["name"]
        norm = normalize_card_name(cname)
        images = ScryfallClient.extract_image_uris(playable)
        image_uri = images.get("normal") or images.get("large") or images.get("small")
        await db.execute_raw(
            """UPDATE card_catalog
               SET id=$1, name=$2, mana_cost=$3, type_line=$4, image_uri=$5,
                   set_code=$6, collector_number=$7, updated_at=NOW()
               WHERE normalized_name=$8""",
            cid,
            cname,
            playable.get("mana_cost"),
            playable.get("type_line"),
            image_uri,
            playable.get("set"),
            playable.get("collector_number"),
            row.get("normalized_name") or norm,
        )
        stats["catalog_fixed"] += 1

    await disconnect_db()
    logger.info("Purge completed: %s", json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    asyncio.run(run_purge())
