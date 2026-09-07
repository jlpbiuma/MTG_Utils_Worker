import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
import httpx

from src.config import settings

logger = logging.getLogger("mtg_set_worker.scryfall")

class ScryfallClient:
    """Client for interacting with the Scryfall API."""

    def __init__(
        self,
        base_url: str = settings.SCRYFALL_API_BASE,
        user_agent: str = settings.USER_AGENT,
        rate_limit_delay: float = settings.RATE_LIMIT_DELAY_SECONDS,
    ):
        self.base_url = base_url
        self.headers = {
            "User-Agent": user_agent,
            "Accept": "application/json;q=0.9,*/*;q=0.8",
        }
        self.rate_limit_delay = rate_limit_delay

    async def fetch_all_sets(self) -> List[Dict[str, Any]]:
        """
        Fetches the complete catalog of MTG sets from Scryfall.
        Endpoint: GET /sets
        """
        url = f"{self.base_url}/sets"
        logger.info(f"Fetching MTG sets from {url}...")

        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            sets = data.get("data", [])
            logger.info(f"Retrieved {len(sets)} sets from Scryfall.")
            return sets

    async def fetch_cards_for_set(
        self,
        search_uri: str,
        delay_seconds: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetches all card printings for a given set following pagination links.
        """
        cards: List[Dict[str, Any]] = []
        current_url: Optional[str] = search_uri
        delay = delay_seconds if delay_seconds is not None else self.rate_limit_delay
        page_num = 1

        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            while current_url:
                logger.debug(f"Fetching page {page_num} from {current_url}")
                try:
                    response = await client.get(current_url)
                    if response.status_code == 404:
                        # Some promo or token sets have search_uris that return 0 cards / 404 on Scryfall
                        logger.warning(f"Set search returned 404 (no cards or invalid query): {current_url}")
                        break
                    response.raise_for_status()
                    data = response.json()
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        logger.warning("Scryfall rate limit reached (429). Backing off for 1.0s...")
                        await asyncio.sleep(1.0)
                        continue
                    raise

                page_cards = data.get("data", [])
                cards.extend(page_cards)

                has_more = data.get("has_more", False)
                current_url = data.get("next_page") if has_more else None

                if current_url:
                    page_num += 1
                    if delay > 0:
                        await asyncio.sleep(delay)

        logger.info(f"Completed fetching {len(cards)} card printings across {page_num} page(s).")
        return cards

    @staticmethod
    def parse_float_price(val: Any) -> Optional[float]:
        """Safely parses a pricing string/number to float or None."""
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[datetime]:
        """Safely parses an ISO date string (YYYY-MM-DD) to a datetime object."""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str)
        except (ValueError, TypeError):
            return None

    @classmethod
    def extract_image_uris(cls, card_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Extracts normal and small image URLs, handling single-faced and double-faced cards.
        """
        image_uris = card_data.get("image_uris")
        if not image_uris and "card_faces" in card_data and card_data["card_faces"]:
            front_face = card_data["card_faces"][0]
            image_uris = front_face.get("image_uris", {})

        if not image_uris:
            return {"image_uri": None, "image_uri_small": None}

        return {
            "image_uri": image_uris.get("normal") or image_uris.get("large") or image_uris.get("small"),
            "image_uri_small": image_uris.get("small"),
        }
