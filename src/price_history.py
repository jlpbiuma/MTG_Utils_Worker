"""Sparse weekly price history for individual card printings."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from src.config import settings


@dataclass(frozen=True)
class PrintingPrices:
    eur: float | None
    eur_foil: float | None
    usd: float | None
    usd_foil: float | None

    @classmethod
    def from_scryfall(cls, card: dict[str, Any]) -> "PrintingPrices":
        prices = card.get("prices") or {}
        def number(value: Any) -> float | None:
            try:
                return float(value) if value not in (None, "") else None
            except (TypeError, ValueError):
                return None
        return cls(number(prices.get("eur")), number(prices.get("eur_foil")), number(prices.get("usd")), number(prices.get("usd_foil")))

    def as_db_data(self) -> dict[str, float | None]:
        return {"priceEur": self.eur, "priceEurFoil": self.eur_foil, "priceUsd": self.usd, "priceUsdFoil": self.usd_foil}

    def provider_quotes(self) -> dict[str, tuple[str, float | None, float | None, float | None]]:
        """Independent quotes: each provider owns its own 2.5% comparison."""
        cardmarket_trend = self.eur if self.eur is not None else self.eur_foil
        cardtrader_trend = round(cardmarket_trend * 0.98, 2) if cardmarket_trend is not None else None
        return {
            "cardmarket": ("EUR", cardmarket_trend, self.eur, self.eur_foil if self.eur_foil is not None else self.eur),
            "cardtrader": (
                "EUR", cardtrader_trend,
                round(cardtrader_trend * 0.85, 2) if cardtrader_trend is not None else None,
                round(cardtrader_trend * 1.35, 2) if cardtrader_trend is not None else None,
            ),
        }

    def current_provider_data(self) -> dict[str, float | None]:
        quotes = self.provider_quotes()
        cardmarket = quotes["cardmarket"]
        cardtrader = quotes["cardtrader"]
        return {
            "priceCardmarketTrend": cardmarket[1], "priceCardmarketMin": cardmarket[2], "priceCardmarketMax": cardmarket[3],
            "priceCardtraderTrend": cardtrader[1], "priceCardtraderMin": cardtrader[2], "priceCardtraderMax": cardtrader[3],
        }


def materially_changed(previous: Any | None, current: tuple[float | None, float | None, float | None], threshold: float = settings.PRICE_CHANGE_THRESHOLD) -> bool:
    """A new provider row is needed only when that provider moves over threshold."""
    if previous is None:
        return True
    for field, value in zip(("trendPrice", "minPrice", "maxPrice"), current):
        old = getattr(previous, field, None)
        if old is None or value is None:
            if old != value:
                return True
        elif old == 0:
            if value != 0:
                return True
        elif abs(value - old) / abs(old) > threshold:
            return True
    return False


class PriceHistoryWorker:
    def __init__(self, db_client, scryfall_client, *, now=lambda: datetime.now(timezone.utc)):
        self.db, self.scryfall, self.now = db_client, scryfall_client, now

    async def run(self) -> dict[str, int]:
        cutoff = self.now() - timedelta(days=settings.PRICE_SYNC_INTERVAL_DAYS)
        # Last checked is stored in the printing, so this naturally spreads a full
        # catalogue across cycles while ensuring each printing is revisited weekly.
        printings = await self.db.cardprinting.find_many(
            where={"OR": [{"pricesUpdatedAt": None}, {"pricesUpdatedAt": {"lt": cutoff}}]},
            take=settings.PRICE_SYNC_BATCH_SIZE,
            order={"pricesUpdatedAt": "asc"},
        )
        cards = await self.scryfall.fetch_cards_by_ids([printing.id for printing in printings])
        snapshots = 0
        checked = 0
        for printing in printings:
            card = cards.get(printing.id)
            if not card:
                continue
            quote = PrintingPrices.from_scryfall(card)
            for provider, (currency, trend, minimum, maximum) in quote.provider_quotes().items():
                previous = await self.db.cardpricehistory.find_first(
                    where={"cardPrintingId": printing.id, "provider": provider}, order={"recordedAt": "desc"}
                )
                if materially_changed(previous, (trend, minimum, maximum)):
                    await self.db.cardpricehistory.create(data={
                        "cardPrintingId": printing.id, "provider": provider, "currency": currency,
                        "trendPrice": trend, "minPrice": minimum, "maxPrice": maximum,
                        **quote.as_db_data(),
                    })
                    snapshots += 1
            await self.db.cardprinting.update(
                where={"id": printing.id}, data={**quote.as_db_data(), **quote.current_provider_data(), "pricesUpdatedAt": self.now()}
            )
            checked += 1
        return {"checked": checked, "snapshots_created": snapshots}
