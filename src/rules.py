"""Rules and rulings tasks retained inside the single worker process."""
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

from src.config import settings
from src.services.scryfall import ScryfallClient
from src.services.tor import async_http_client


class RulesWorker:
    def __init__(self, db_client, scryfall: ScryfallClient | None = None):
        self.db = db_client
        self.scryfall = scryfall or ScryfallClient()

    async def sync_document(self) -> dict[str, Any]:
        state = await self.db.rulessyncstate.find_unique(where={"id": "rules"})
        now = datetime.now(timezone.utc)
        if state and state.lastRulesCheckAt and now - state.lastRulesCheckAt.replace(tzinfo=timezone.utc) < timedelta(seconds=settings.RULES_CHECK_INTERVAL_SECONDS):
            return {"status": "skipped"}
        if not state:
            state = await self.db.rulessyncstate.create(data={"id": "rules"})
        await self.db.rulessyncstate.update(where={"id": "rules"}, data={"lastRulesCheckAt": now})
        if not settings.WIZARDS_RULES_TEXT_URL:
            return {"status": "skipped", "reason": "WIZARDS_RULES_TEXT_URL is not configured"}
        previous = await self.db.ruledocument.find_first(where={"source": "wizards", "format": "txt"}, order={"fetchedAt": "desc"})
        headers = {"Accept": "text/plain"}
        if previous and previous.etag:
            headers["If-None-Match"] = previous.etag
        async with async_http_client(headers=headers, timeout=60.0, follow_redirects=True) as client:
            response = await client.get(settings.WIZARDS_RULES_TEXT_URL)
        if response.status_code == 304:
            return {"status": "unchanged"}
        response.raise_for_status()
        content = response.text
        sha256 = hashlib.sha256(content.encode()).hexdigest()
        if previous and previous.sha256 == sha256:
            return {"status": "unchanged"}
        document = await self.db.ruledocument.create(data={"source": "wizards", "format": "txt", "url": str(response.url), "etag": response.headers.get("etag"), "lastModified": response.headers.get("last-modified"), "sha256": sha256, "content": content})
        return {"status": "updated", "document_id": document.id}

    async def sync_rulings(self) -> dict[str, int]:
        cards = await self.db.cardcatalog.find_many(take=settings.RULINGS_PER_CYCLE, order={"updatedAt": "asc"})
        stored = errors = 0
        for card in cards:
            try:
                for item in await self.scryfall.fetch_rulings(card.id):
                    oracle_id, source, date, text = item.get("oracle_id"), item.get("source"), item.get("published_at"), item.get("comment")
                    if not all((oracle_id, source, date, text)):
                        continue
                    digest = hashlib.sha256(f"{source}\x1f{date}\x1f{text}".encode()).hexdigest()
                    await self.db.cardruling.upsert(where={"oracleId_textHash": {"oracleId": oracle_id, "textHash": digest}}, data={"create": {"oracleId": oracle_id, "scryfallCardId": card.id, "source": source, "rulingDate": datetime.fromisoformat(date), "text": text, "textHash": digest}, "update": {"lastSeenAt": datetime.now(timezone.utc)}})
                    stored += 1
            except Exception:
                errors += 1
        return {"cards_checked": len(cards), "rulings_upserted": stored, "errors": errors}

    async def run(self) -> dict[str, Any]:
        return {"rules": await self.sync_document(), "rulings": await self.sync_rulings()}
