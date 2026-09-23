"""
EDHREC Worker Service.
Synchronizes Top 100 commanders and all candidate commanders from EDHREC,
respecting a 30-second rate limit between downloads.
"""

import asyncio
import logging
import re
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set
import httpx
from prisma import Prisma, Json

from src.services.card_utils import normalize_card_name

logger = logging.getLogger("mtg_worker.edhrec")

EDHREC_BASE_URL = "https://json.edhrec.com"
USER_AGENT = "MTGUtils/2.0 (Worker-Daemon; +https://github.com/mtg-utils)"


def to_edhrec_slug(name: str) -> str:
    """Converts a card name to the URL slug convention used by EDHREC."""
    if not name:
        return ""
    s = str(name).lower()
    # Normalize unicode accents
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s*//\s*", "-", s)
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    s = re.sub(r"-+", "-", s)
    return s


def is_commander_candidate_type(type_line: Optional[str], oracle_text: Optional[str] = None) -> bool:
    """
    Check if a card type is eligible as a commander:
    - Legendary Creature
    - Legendary Artifact Vehicle
    - Legendary Planeswalker with 'can be your commander' in oracle text
    """
    type_value = (type_line or "").lower()
    is_legendary = "legendary" in type_value
    is_legendary_creature = is_legendary and "creature" in type_value
    is_legendary_vehicle = (
        is_legendary
        and "artifact" in type_value
        and "vehicle" in type_value
    )
    is_legendary_planeswalker = is_legendary and "planeswalker" in type_value
    allows_commander = bool(oracle_text) and "can be your commander" in (oracle_text or "").lower()
    return (
        is_legendary_creature
        or is_legendary_vehicle
        or (is_legendary_planeswalker and allows_commander)
    )


BASIC_LAND_NAMES = {
    "plains", "island", "swamp", "mountain", "forest", "wastes",
    "snow-covered plains", "snow-covered island", "snow-covered swamp",
    "snow-covered mountain", "snow-covered forest"
}


def is_basic_land_name(name: str) -> bool:
    return normalize_card_name(name) in BASIC_LAND_NAMES


class EdhrecWorker:
    """Manages downloading, parsing and scheduling EDHREC commander syncs."""

    def __init__(self, db: Prisma, http_client: Optional[httpx.AsyncClient] = None):
        self.db = db
        self._external_client = http_client

    def _get_client(self) -> httpx.AsyncClient:
        if self._external_client:
            return self._external_client
        return httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=15.0,
            follow_redirects=True,
        )

    async def fetch_top_100(self, client: Optional[httpx.AsyncClient] = None) -> List[Dict[str, Any]]:
        """Fetch the top 100 commanders from EDHREC."""
        url = f"{EDHREC_BASE_URL}/pages/commanders/year.json"
        c = client or self._get_client()
        should_close = client is None and not self._external_client

        try:
            res = await c.get(url)
            if res.status_code != 200:
                logger.error(f"EDHREC top 100 returned HTTP {res.status_code}")
                return []
            data = res.json()
            container = data.get("container", {}).get("json_dict", {})
            cardlists = container.get("cardlists", [])
            if not cardlists:
                return []
            return cardlists[0].get("cardviews", [])[:100]
        except Exception as e:
            logger.error(f"Failed to fetch EDHREC top 100: {e}")
            return []
        finally:
            if should_close:
                await c.aclose()

    async def sync_top_100_commanders(self, client: Optional[httpx.AsyncClient] = None) -> int:
        """Download top 100 list and register or update entries in the database."""
        items = await self.fetch_top_100(client)
        if not items:
            return 0

        updated_count = 0
        for idx, item in enumerate(items):
            name = item.get("name")
            if not name:
                continue

            slug = item.get("slug") or item.get("sanitized") or to_edhrec_slug(name)
            norm = normalize_card_name(name)
            rank = item.get("rank") or (idx + 1)
            num_decks = item.get("num_decks", 0)
            card_id = item.get("id") or f"edhrec-{slug}"

            existing = await self.db.edhreccommander.find_unique(where={"normalizedName": norm})
            if existing:
                await self.db.edhreccommander.update(
                    where={"id": existing.id},
                    data={
                        "isTop100": True,
                        "rank": rank,
                        "numDecks": num_decks,
                        "slug": slug,
                    },
                )
            else:
                await self.db.edhreccommander.create(
                    data={
                        "id": card_id,
                        "name": name,
                        "normalizedName": norm,
                        "slug": slug,
                        "rank": rank,
                        "isTop100": True,
                        "numDecks": num_decks,
                        "status": "pending",
                    }
                )
            updated_count += 1

        logger.info(f"Registered/updated {updated_count} commanders in the EDHREC Top 100.")
        return updated_count

    async def discover_candidate_commanders(self) -> int:
        """
        Scan CardCatalog for candidate commanders (legendary creatures, vehicles,
        and planeswalkers with commander clause) and register new pending entries.
        """
        catalog_cards = await self.db.cardcatalog.find_many()
        added_count = 0

        for card in catalog_cards:
            if not is_commander_candidate_type(card.typeLine, card.oracleTextEs):
                continue

            norm = card.normalizedName or normalize_card_name(card.name)
            existing = await self.db.edhreccommander.find_unique(where={"normalizedName": norm})
            if existing:
                continue

            slug = to_edhrec_slug(card.name)
            await self.db.edhreccommander.create(
                data={
                    "id": card.id,
                    "name": card.name,
                    "normalizedName": norm,
                    "slug": slug,
                    "status": "pending",
                }
            )
            added_count += 1

        if added_count > 0:
            logger.info(f"Discovered and registered {added_count} new candidate commanders.")
        return added_count

    async def fetch_commander_details(
        self, slug: str, client: Optional[httpx.AsyncClient] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch full details and card lists for a single commander from EDHREC."""
        url = f"{EDHREC_BASE_URL}/pages/commanders/{slug}.json"
        c = client or self._get_client()
        should_close = client is None and not self._external_client

        try:
            res = await c.get(url)
            if res.status_code == 404:
                logger.info(f"EDHREC returned 404 for slug '{slug}' (no deck data).")
                return {"_status": "not_found"}
            if res.status_code == 429:
                logger.warning(f"EDHREC rate limited (429) for slug '{slug}'.")
                return {"_status": "rate_limited"}
            if res.status_code != 200:
                logger.warning(f"EDHREC returned status {res.status_code} for slug '{slug}'.")
                return None

            return res.json()
        except Exception as e:
            logger.error(f"Error fetching EDHREC details for slug '{slug}': {e}")
            return None
        finally:
            if should_close:
                await c.aclose()

    @staticmethod
    def parse_commander_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
        """Extract type counts, categorized cards, and build canonical card list."""
        creature_count = int(raw.get("creature", 0) or 0)
        instant_count = int(raw.get("instant", 0) or 0)
        sorcery_count = int(raw.get("sorcery", 0) or 0)
        artifact_count = int(raw.get("artifact", 0) or 0)
        enchantment_count = int(raw.get("enchantment", 0) or 0)
        battle_count = int(raw.get("battle", 0) or 0)
        planeswalker_count = int(raw.get("planeswalker", 0) or 0)
        land_count = int(raw.get("land", 0) or 0)
        basic_count = int(raw.get("basic", 0) or 0)
        nonbasic_count = int(raw.get("nonbasic", 0) or 0)

        # Parse card lists
        container = (raw.get("container") or {}).get("json_dict") or {}
        cardlists = container.get("cardlists") or raw.get("cardlist") or []

        category_cards: Dict[str, List[Dict[str, Any]]] = {}
        for group in cardlists:
            tag = (group.get("tag") or group.get("header") or "other").lower().replace(" ", "")
            views = group.get("cardviews", [])
            if tag not in category_cards:
                category_cards[tag] = []
            for cv in views:
                c_name = cv.get("name")
                if not c_name:
                    continue
                num_decks = cv.get("num_decks", 0)
                potential = cv.get("potential_decks", 0)
                pct = round((num_decks / potential) * 100, 1) if potential > 0 else 0.0
                category_cards[tag].append({
                    "name": c_name,
                    "normalizedName": normalize_card_name(c_name),
                    "sanitized": cv.get("sanitized", ""),
                    "id": cv.get("id", ""),
                    "inclusionPct": pct,
                    "synergy": round(cv.get("synergy", 0.0) * 100, 1) if "synergy" in cv else 0.0,
                    "numDecks": num_decks,
                })

        # Deduplicate and sort cards within each category by inclusion % desc
        for tag in category_cards:
            seen = set()
            unique_list = []
            for item in category_cards[tag]:
                norm = item["normalizedName"]
                if norm not in seen:
                    seen.add(norm)
                    unique_list.append(item)
            unique_list.sort(key=lambda x: x["inclusionPct"], reverse=True)
            category_cards[tag] = unique_list

        # If top level counts were missing/0, fallback to length of category cards
        if creature_count == 0 and "creatures" in category_cards:
            creature_count = min(30, len(category_cards["creatures"]))
        if instant_count == 0 and "instants" in category_cards:
            instant_count = min(15, len(category_cards["instants"]))
        if sorcery_count == 0 and "sorceries" in category_cards:
            sorcery_count = min(10, len(category_cards["sorceries"]))

        # Build canonical 99 cards based on type quota:
        canonical_names: List[str] = []

        def pick_top(category_tags: List[str], count: int):
            picked: List[str] = []
            if count <= 0:
                return picked
            for tag in category_tags:
                for c in category_cards.get(tag, []):
                    norm = c["normalizedName"]
                    if norm not in canonical_names and norm not in picked:
                        if "land" in tag and is_basic_land_name(norm):
                            continue
                        picked.append(norm)
                        if len(picked) >= count:
                            return picked
            return picked

        canonical_names.extend(pick_top(["creatures"], creature_count))
        canonical_names.extend(pick_top(["instants"], instant_count))
        canonical_names.extend(pick_top(["sorceries"], sorcery_count))
        canonical_names.extend(pick_top(["utilityartifacts", "manaartifacts", "artifacts"], artifact_count))
        canonical_names.extend(pick_top(["enchantments"], enchantment_count))
        canonical_names.extend(pick_top(["planeswalkers"], planeswalker_count))
        canonical_names.extend(pick_top(["utilitylands", "lands"], nonbasic_count))

        # Color identity from card header
        card_header = container.get("card", {})
        color_id = "".join(sorted(card_header.get("color_identity", [])))

        return {
            "colorIdentity": color_id,
            "creatureCount": creature_count,
            "instantCount": instant_count,
            "sorceryCount": sorcery_count,
            "artifactCount": artifact_count,
            "enchantmentCount": enchantment_count,
            "battleCount": battle_count,
            "planeswalkerCount": planeswalker_count,
            "landCount": land_count,
            "basicLandCount": basic_count,
            "nonbasicLandCount": nonbasic_count,
            "canonicalCardNames": canonical_names,
            "cardsJson": category_cards,
        }

    async def sync_next_pending(self, client: Optional[httpx.AsyncClient] = None) -> Optional[Dict[str, Any]]:
        """
        Process the next pending commander:
        1. Priority: Top 100 first, then by rank, then oldest synced.
        2. Fetch from EDHREC.
        3. Save parsed type counts and canonical cards.
        """
        now = datetime.now(timezone.utc)
        stale_threshold = now - timedelta(days=30)

        # Look for pending commanders or those needing refresh
        commander = await self.db.edhreccommander.find_first(
            where={
                "OR": [
                    {"status": "pending"},
                    {"status": "error", "syncedAt": {"lt": now - timedelta(hours=1)}},
                    {"status": "synced", "syncedAt": {"lt": stale_threshold}},
                ]
            },
            order=[
                {"isTop100": "desc"},
                {"rank": "asc"},
                {"createdAt": "asc"},
            ],
        )

        if not commander:
            return None

        slug = commander.slug or to_edhrec_slug(commander.name)
        logger.info(f"Syncing EDHREC commander '{commander.name}' (slug: {slug})...")

        raw = await self.fetch_commander_details(slug, client)
        if raw is None:
            await self.db.edhreccommander.update(
                where={"id": commander.id},
                data={
                    "status": "error",
                    "lastError": "HTTP or network error during fetch",
                    "syncedAt": now,
                },
            )
            return {"status": "error", "name": commander.name}

        if raw.get("_status") == "not_found":
            await self.db.edhreccommander.update(
                where={"id": commander.id},
                data={
                    "status": "not_found",
                    "lastError": "No recommendations found on EDHREC",
                    "syncedAt": now,
                },
            )
            return {"status": "not_found", "name": commander.name}

        if raw.get("_status") == "rate_limited":
            return {"status": "rate_limited", "name": commander.name}

        parsed = self.parse_commander_payload(raw)

        await self.db.edhreccommander.update(
            where={"id": commander.id},
            data={
                "colorIdentity": parsed["colorIdentity"] or commander.colorIdentity,
                "creatureCount": parsed["creatureCount"],
                "instantCount": parsed["instantCount"],
                "sorceryCount": parsed["sorceryCount"],
                "artifactCount": parsed["artifactCount"],
                "enchantmentCount": parsed["enchantmentCount"],
                "battleCount": parsed["battleCount"],
                "planeswalkerCount": parsed["planeswalkerCount"],
                "landCount": parsed["landCount"],
                "basicLandCount": parsed["basicLandCount"],
                "nonbasicLandCount": parsed["nonbasicLandCount"],
                "cardsJson": Json(parsed["cardsJson"]),
                "canonicalCardNames": Json(parsed["canonicalCardNames"]),
                "status": "synced",
                "lastError": None,
                "syncedAt": now,
            },
        )

        logger.info(
            f"Successfully synced '{commander.name}': "
            f"{parsed['creatureCount']} creatures, {parsed['instantCount']} instants, "
            f"{len(parsed['canonicalCardNames'])} canonical cards."
        )

        return {"status": "synced", "name": commander.name}


async def edhrec_scheduler(db: Prisma, rate_limit_seconds: float = 30.0):
    """
    Background worker loop:
    Runs continuously, downloading 1 commander every 30 seconds.
    """
    worker = EdhrecWorker(db)
    logger.info("Initializing EDHREC background sync daemon...")

    # Wait a few seconds for initial DB readiness
    await asyncio.sleep(5)

    try:
        # Initial discovery & Top 100 check
        await worker.sync_top_100_commanders()
        await worker.discover_candidate_commanders()
    except Exception as e:
        logger.error(f"Error during initial EDHREC commander discovery: {e}", exc_info=True)

    while True:
        try:
            result = await worker.sync_next_pending()
            if result:
                if result.get("status") == "rate_limited":
                    logger.warning(f"Rate limited by EDHREC. Backing off for 60s...")
                    await asyncio.sleep(60.0)
                    continue
                # Enforce the 30-second rate limit requested by user
                await asyncio.sleep(rate_limit_seconds)
            else:
                # No pending commanders, sleep for 5 minutes before checking again
                await asyncio.sleep(300.0)
        except asyncio.CancelledError:
            logger.info("EDHREC sync worker loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in EDHREC sync loop: {e}", exc_info=True)
            await asyncio.sleep(rate_limit_seconds)
