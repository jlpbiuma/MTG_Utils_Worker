"""Cardmarket price history via MTGJSON and initial Scryfall price extraction."""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.services.mtgjson_cardmarket import (
    sync_today,
    backfill_history,
    sync_map_remote,
    get_report,
)


@dataclass(frozen=True)
class PrintingPrices:
    eur: float | None
    eur_foil: float | None
    usd: float | None = None
    usd_foil: float | None = None

    @classmethod
    def from_scryfall(cls, card: dict[str, Any]) -> "PrintingPrices":
        prices = card.get("prices") or {}

        def number(value: Any) -> float | None:
            try:
                return float(value) if value not in (None, "") else None
            except (TypeError, ValueError):
                return None

        return cls(
            number(prices.get("eur")),
            number(prices.get("eur_foil")),
            number(prices.get("usd")),
            number(prices.get("usd_foil")),
        )

    def as_db_data(self) -> dict[str, float | None]:
        return {
            "priceEur": self.eur,
            "priceEurFoil": self.eur_foil,
            "priceUsd": self.usd,
            "priceUsdFoil": self.usd_foil,
        }

    def current_provider_data(self) -> dict[str, float | None]:
        cardmarket_trend = self.eur if self.eur is not None else self.eur_foil
        return {
            "priceCardmarketTrend": cardmarket_trend,
            "priceCardmarketMin": self.eur,
            "priceCardmarketMax": self.eur_foil if self.eur_foil is not None else self.eur,
        }


class PriceHistoryWorker:
    def __init__(self, db_client=None, scryfall_client=None, *, now=lambda: datetime.now(timezone.utc)):
        self.db = db_client
        self.scryfall = scryfall_client
        self.now = now

    async def run(self, force: bool = False) -> dict[str, Any]:
        """Sincronización diaria de MTGJSON Cardmarket."""
        if not self.db:
            return {"status": "skipped", "reason": "No db client provided"}
        try:
            stats = await sync_today(self.db, force=force)
            return {
                "status": stats.status,
                "feed_date": stats.feed_date,
                "tracked": stats.tracked,
                "mapped": stats.mapped,
                "matched": stats.matched,
                "inserted": stats.inserted,
                "anomalies": stats.anomalies,
                "conflicts": stats.conflicts,
                "notes": stats.notes,
            }
        except Exception as exc:
            return {"status": "error", "error": str(exc)}
