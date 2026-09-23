import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
import httpx

from src.config import settings
from src.services.tor import TorClient
from src.services.scryfall_transport import request as scryfall_request

logger = logging.getLogger("mtg_worker.scryfall")

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
        self.tor = TorClient()
        self.max_429_retries = None

    async def _request(self, method, url, **kwargs):
        if self.max_429_retries is not None:
            kwargs["max_429_retries"] = self.max_429_retries
        return await scryfall_request(method, url, **kwargs)

    async def fetch_all_sets(self) -> List[Dict[str, Any]]:
        """
        Fetches the complete catalog of MTG sets from Scryfall.
        Endpoint: GET /sets
        """
        url = f"{self.base_url}/sets"
        logger.info(f"Fetching MTG sets from {url}...")

        await self.tor.request_new_identity()
        response = await self._request("GET", url, headers=self.headers)
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
        Excludes non-playable art series, tokens, and memorabilia.
        """
        from src.services.card_utils import is_playable_card

        cards: List[Dict[str, Any]] = []
        current_url: Optional[str] = search_uri
        delay = delay_seconds if delay_seconds is not None else self.rate_limit_delay
        page_num = 1

        await self.tor.request_new_identity()
        while current_url:
            logger.debug(f"Fetching page {page_num} from {current_url}")
            response = await self._request("GET", current_url, headers=self.headers)
            if response.status_code == 404:
                # Some promo or token sets have search_uris that return 0 cards / 404 on Scryfall
                logger.warning(f"Set search returned 404 (no cards or invalid query): {current_url}")
                break
            response.raise_for_status()
            data = response.json()

            page_cards = [c for c in data.get("data", []) if is_playable_card(c)]
            cards.extend(page_cards)

            has_more = data.get("has_more", False)
            current_url = data.get("next_page") if has_more else None

            if current_url:
                page_num += 1
                if delay > 0:
                    await asyncio.sleep(delay)

        logger.info(f"Completed fetching {len(cards)} card printings across {page_num} page(s).")
        return cards

    async def fetch_cards_by_ids(self, ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Resolve printings in Scryfall's documented collection batches."""
        from src.services.card_utils import is_playable_card

        result: Dict[str, Dict[str, Any]] = {}
        if not ids:
            return result
        await self.tor.request_new_identity()
        for start in range(0, len(ids), 75):
            response = await self._request("POST", f"{self.base_url}/cards/collection", headers=self.headers, json={"identifiers": [{"id": value} for value in ids[start:start + 75]]})
            response.raise_for_status()
            for card in response.json().get("data", []):
                if card.get("id") and is_playable_card(card):
                    result[card["id"]] = card
            if start + 75 < len(ids) and self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay)
        return result

    async def fetch_cards_by_names(self, names: List[str]) -> List[Dict[str, Any]]:
        """Resolve a collection import in safe Scryfall collection batches."""
        from src.services.card_utils import is_playable_card

        unique_names = list(dict.fromkeys(name.strip() for name in names if name and name.strip()))
        cards: List[Dict[str, Any]] = []
        if not unique_names:
            return cards
        await self.tor.request_new_identity()
        for start in range(0, len(unique_names), 75):
            response = await self._request(
                "POST", f"{self.base_url}/cards/collection", headers=self.headers,
                json={"identifiers": [{"name": value} for value in unique_names[start:start + 75]]},
            )
            response.raise_for_status()
            cards.extend([c for c in response.json().get("data", []) if is_playable_card(c)])
            if start + 75 < len(unique_names) and self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay)
        return cards

    async def fetch_printing_language(
        self, set_code: str, collector_number: str, language: str = "es"
    ) -> Optional[Dict[str, Any]]:
        """Fetch one exact localized printing when Scryfall publishes it."""
        if not set_code or not collector_number:
            return None
        url = f"{self.base_url}/cards/{set_code.lower()}/{collector_number}/{language.lower()}"
        response = await self._request("GET", url, headers=self.headers)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    async def fetch_printings_by_name(self, name: str) -> List[Dict[str, Any]]:
        """Fetch every playable paper printing for an exact card name, oldest to newest, NEVER art series."""
        from src.services.card_utils import is_playable_card

        clean_name = name.strip()
        if not clean_name or clean_name.startswith(("A-", "a-")):
            return []

        async def _query_search(query_str: str) -> List[Dict[str, Any]]:
            results: List[Dict[str, Any]] = []
            current_url: Optional[str] = f"{self.base_url}/cards/search"
            params: Optional[Dict[str, str]] = {
                "q": query_str,
                "unique": "prints",
                "order": "released",
                "dir": "asc",
                "include_extras": "false",
            }
            while current_url:
                response = await self._request(
                    "GET", current_url, headers=self.headers, params=params
                )
                if response.status_code == 404:
                    break
                response.raise_for_status()
                payload = response.json()
                results.extend(payload.get("data", []))
                current_url = payload.get("next_page") if payload.get("has_more") else None
                params = None
                if current_url and self.rate_limit_delay:
                    await asyncio.sleep(self.rate_limit_delay)
            return results

        # 1. Primary search: exact name with paper-only, non-digital, non-alchemy filters
        base_query = f'!"{clean_name}" game:paper -is:digital -set_type:alchemy -layout:art_series -set_type:memorabilia'
        cards = await _query_search(base_query)

        # 2. If no prints found, the name might be in Spanish (e.g. "Kimahri, guardián valiente")
        #    or a localized printed name. Try with lang:any to find the canonical English card.
        if not cards:
            lang_query = f'lang:any !"{clean_name}" game:paper -is:digital -set_type:alchemy -layout:art_series -set_type:memorabilia'
            localized_cards = await _query_search(lang_query)
            if localized_cards:
                canonical_name = localized_cards[0].get("name")
                if canonical_name and canonical_name.lower() != clean_name.lower():
                    # Now fetch all playable printings of the canonical name
                    cards = await _query_search(f'!"{canonical_name}" game:paper -is:digital -set_type:alchemy -layout:art_series -set_type:memorabilia')
                if not cards:
                    cards = localized_cards

        # 3. Fallback: try fuzzy / named resolution if exact quotes returned nothing
        if not cards:
            try:
                named_res = await self._request(
                    "GET", f"{self.base_url}/cards/named", headers=self.headers, params={"fuzzy": clean_name}
                )
                if named_res.status_code == 200:
                    named_data = named_res.json()
                    canonical_name = named_data.get("name")
                    if canonical_name and canonical_name.lower() != clean_name.lower():
                        cards = await _query_search(f'!"{canonical_name}" game:paper -is:digital -set_type:alchemy -layout:art_series -set_type:memorabilia')
                    elif is_playable_card(named_data):
                        cards = [named_data]
            except Exception as e:
                logger.warning("Fuzzy fallback named resolution failed for %s: %s", clean_name, e)

        # 4. Strict safeguard: filter out ANY card that is not a playable paper card
        return [c for c in cards if is_playable_card(c)]


    async def fetch_rulings(self, card_id: str) -> List[Dict[str, Any]]:
        response = await self._request("GET", f"{self.base_url}/cards/{card_id}/rulings", headers=self.headers)
        response.raise_for_status()
        return response.json().get("data", [])

    @staticmethod
    def spanish_search_uri(search_uri: str) -> str:
        """Adds Scryfall's language filter while retaining the set search options."""
        parsed = urlsplit(search_uri)
        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query = params.get("q", "").strip()
        if not query:
            raise ValueError("Scryfall set search URI is missing its q parameter")
        params["q"] = f"{query} lang:es"
        params["unique"] = "prints"
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(params), parsed.fragment))

    async def fetch_spanish_cards_for_set(self, search_uri: str) -> List[Dict[str, Any]]:
        """Fetches the official Spanish printings for a set."""
        return await self.fetch_cards_for_set(self.spanish_search_uri(search_uri))

    async def fetch_spanish_printing(self, set_code: str, collector_number: str) -> Optional[Dict[str, Any]]:
        """Returns the matching Spanish printing or None when Scryfall has none."""
        url = f"{self.base_url}/cards/{set_code}/{collector_number}/es"
        response = await self._request("GET", url, headers=self.headers)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

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
            return {"normal": None, "small": None, "large": None, "png": None}

        return {
            "normal": image_uris.get("normal"),
            "small": image_uris.get("small"),
            "large": image_uris.get("large"),
            "png": image_uris.get("png"),
        }

    @staticmethod
    def extract_spanish_rules_text(card_data: Dict[str, Any]) -> Optional[str]:
        """Returns official Spanish printed rules text, including both faces when needed."""
        if card_data.get("lang") != "es":
            return None

        printed_text = card_data.get("printed_text")
        if printed_text:
            return printed_text

        face_texts = [
            face.get("printed_text")
            for face in card_data.get("card_faces", [])
            if face.get("printed_text")
        ]
        return "\n//\n".join(face_texts) if face_texts else None

    @staticmethod
    def extract_full_rules_text(card_data: Dict[str, Any], *, prefer_printed: bool = False) -> Optional[str]:
        """Return untruncated rules text, joining every face of a modal card."""
        direct_fields = ("printed_text", "oracle_text") if prefer_printed else ("oracle_text", "printed_text")
        for field in direct_fields:
            if card_data.get(field):
                return card_data[field]
        face_texts = []
        for face in card_data.get("card_faces") or []:
            for field in direct_fields:
                if face.get(field):
                    face_texts.append(face[field])
                    break
        return "\n//\n".join(face_texts) if face_texts else None
