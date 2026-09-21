#!/usr/bin/env python3
"""
Ingesta de precios de Cardmarket (EUR) desde MTGJSON para cartas identificadas por Scryfall ID.
Soporta PostgreSQL nativo via Prisma y proxy Tor.

Comandos:
  sync-map   Carga la tabla uuid(MTGJSON) -> scryfall_id desde AllPrintings.sqlite
  ingest     Descarga AllPricesToday e inserta SOLO precios retail de Cardmarket
  backfill   Igual que ingest pero con AllPrices (últimos ~90 días)
  report     Cobertura, cartas sin mapear y última ejecución
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import io
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import date as _date
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
import ijson

from src.config import settings

log = logging.getLogger("mtg_worker.mtgjson_cm")

PRICES_TODAY_URL = "https://mtgjson.com/api/v5/AllPricesToday.json.gz"
PRICES_90D_URL = "https://mtgjson.com/api/v5/AllPrices.json.gz"
PRINTINGS_SQLITE_URL = "https://mtgjson.com/api/v5/AllPrintings.sqlite.gz"
USER_AGENT = "mtg-price-worker/1.0 (https://github.com/icedeal/mtg-utils)"

FINISHES = {"normal": 0, "foil": 1, "etched": 2}

# Cuarentena de anomalías: salto >= 5x (arriba o abajo) Y diferencia >= 20 EUR.
ANOMALY_RATIO = 5
ANOMALY_MIN_DELTA_CENTS = 2000
# Una anomalía se confirma si al día siguiente (o después) el precio se mantiene (±10%).
CONFIRM_TOLERANCE = Decimal("0.10")
COVERAGE_DROP_WARN = 0.8
BATCH_SIZE = 5000
DEFAULT_CARDS_QUERY = "SELECT id FROM card_printings"


# --------------------------------------------------------------------------- utilidades puras

def price_to_cents(value: Any) -> int | None:
    """Convierte un precio de MTGJSON a céntimos enteros. None si no es un precio válido (>0)."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation:
        return None
    if not d.is_finite() or d <= 0:
        return None
    return int((d * 100).to_integral_value(rounding=ROUND_HALF_UP))


def valid_iso_date(s: Any) -> bool:
    if not isinstance(s, str):
        return False
    try:
        _date.fromisoformat(s)
        return True
    except ValueError:
        return False


def is_anomalous(prev_cents: int, new_cents: int) -> bool:
    if abs(new_cents - prev_cents) < ANOMALY_MIN_DELTA_CENTS:
        return False
    return new_cents >= prev_cents * ANOMALY_RATIO or new_cents * ANOMALY_RATIO <= prev_cents


def iter_cardmarket_points(entry: Any):
    """Rinde (finish_code, fecha, precio_crudo) del retail de Cardmarket de un uuid. Ignora todo lo demás."""
    if not isinstance(entry, dict):
        return
    paper = entry.get("paper")
    cm = paper.get("cardmarket") if isinstance(paper, dict) else None
    retail = cm.get("retail") if isinstance(cm, dict) else None
    if not isinstance(retail, dict):
        return
    for name, code in FINISHES.items():
        series = retail.get(name)
        if isinstance(series, dict):
            for d, p in series.items():
                yield code, d, p


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _open_maybe_gz(path: Path):
    with open(path, "rb") as f:
        magic = f.read(2)
    return gzip.open(path, "rb") if magic == b"\x1f\x8b" else open(path, "rb")


def read_feed_date(path: Path) -> str | None:
    """Fecha de `meta` (viene antes que `data`, así que se lee sin recorrer el fichero entero)."""
    with _open_maybe_gz(path) as f:
        for meta in ijson.items(f, "meta"):
            return meta.get("date") if isinstance(meta, dict) else None
    return None


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- transporte / descarga

def _get_http_client(**kwargs) -> httpx.Client:
    if settings.TOR_ENABLED and settings.TOR_SOCKS_PROXY:
        kwargs.setdefault("proxy", settings.TOR_SOCKS_PROXY)
    kwargs.setdefault("timeout", httpx.Timeout(connect=15.0, read=300.0, write=30.0, pool=30.0))
    kwargs.setdefault("follow_redirects", True)
    return httpx.Client(**kwargs)


def _expected_sha256(url: str) -> str | None:
    try:
        with _get_http_client() as client:
            r = client.get(url + ".sha256", headers={"User-Agent": USER_AGENT})
            if r.status_code == 200:
                token = r.text.split()[0].strip().lower()
                if len(token) == 64 and all(c in "0123456789abcdef" for c in token):
                    return token
    except Exception as exc:
        log.warning("No se pudo obtener SHA256 para %s: %s", url, exc)
    return None


def download(url: str, dest: Path, verify: bool = True) -> Path:
    log.info("Descargando %s -> %s...", url, dest)
    with _get_http_client() as client:
        with client.stream("GET", url, headers={"User-Agent": USER_AGENT}) as r:
            r.raise_for_status()
            with open(dest, "wb") as out:
                for chunk in r.iter_bytes(chunk_size=1 << 20):
                    out.write(chunk)
    if verify:
        expected = _expected_sha256(url)
        if expected is None:
            log.warning("No se pudo obtener %s.sha256; se omite verificación.", url)
        elif sha256_file(dest) != expected:
            raise RuntimeError(f"SHA256 no coincide para {url}: descarga corrupta o incompleta")
    log.info("Descarga completada con éxito: %s (%.1f MB)", dest, dest.stat().st_size / (1024 * 1024))
    return dest


# --------------------------------------------------------------------------- operaciones BD (Prisma PostgreSQL)

@dataclass
class Stats:
    status: str = "running"
    feed_date: str | None = None
    tracked: int = 0
    mapped: int = 0
    matched: int = 0
    inserted: int = 0
    invalid: int = 0
    anomalies: int = 0
    confirmed: int = 0
    conflicts: int = 0
    notes: list[str] = field(default_factory=list)


async def load_tracked(db, cards_query: str = DEFAULT_CARDS_QUERY) -> set[str]:
    rows = await db.query_raw(cards_query)
    tracked = set()
    for r in rows:
        if isinstance(r, dict):
            val = next(iter(r.values()), None)
        else:
            val = r[0] if len(r) > 0 else None
        if val:
            tracked.add(str(val).lower())
    return tracked


async def load_uuid_map(db, tracked: set[str]) -> Tuple[dict[str, str], set[str]]:
    rows = await db.query_raw("SELECT uuid, scryfall_id FROM mtgjson_uuid_map")
    uuid_map: dict[str, str] = {}
    count: dict[str, int] = {}
    for r in rows:
        uuid = r.get("uuid") if isinstance(r, dict) else r[0]
        sid = (r.get("scryfall_id") if isinstance(r, dict) else r[1]).lower()
        if sid in tracked:
            uuid_map[uuid] = sid
            count[sid] = count.get(sid, 0) + 1
    multi = {sid for sid, n in count.items() if n > 1}
    return uuid_map, multi


async def load_latest(db) -> dict[tuple[str, int], tuple[str, int]]:
    q = """
    SELECT h.scryfall_id, h.finish, h.date::text, h.price_cents
    FROM cm_price_history h
    JOIN (
        SELECT scryfall_id, finish, MAX(date) AS d
        FROM cm_price_history
        GROUP BY 1, 2
    ) m ON h.scryfall_id = m.scryfall_id AND h.finish = m.finish AND h.date = m.d
    """
    rows = await db.query_raw(q)
    res = {}
    for r in rows:
        sid = r.get("scryfall_id") if isinstance(r, dict) else r[0]
        finish = int(r.get("finish") if isinstance(r, dict) else r[1])
        date_str = str(r.get("date") if isinstance(r, dict) else r[2])[:10]
        cents = int(r.get("price_cents") if isinstance(r, dict) else r[3])
        res[(sid.lower(), finish)] = (date_str, cents)
    return res


async def load_pending(db) -> dict[tuple[str, int], tuple[str, int]]:
    q = """
    SELECT scryfall_id, finish, date::text, price_cents
    FROM price_anomalies
    WHERE resolved = false
    ORDER BY date
    """
    rows = await db.query_raw(q)
    res = {}
    for r in rows:
        sid = r.get("scryfall_id") if isinstance(r, dict) else r[0]
        finish = int(r.get("finish") if isinstance(r, dict) else r[1])
        date_str = str(r.get("date") if isinstance(r, dict) else r[2])[:10]
        cents = int(r.get("price_cents") if isinstance(r, dict) else r[3])
        res[(sid.lower(), finish)] = (date_str, cents)
    return res


async def sync_uuid_map(db, printings_sqlite: Path) -> int:
    """Extrae uuid -> scryfallId de AllPrintings.sqlite y vuelca a mtgjson_uuid_map en PostgreSQL."""
    src = sqlite3.connect(f"file:{printings_sqlite}?mode=ro", uri=True)
    try:
        has = src.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cardIdentifiers'").fetchone()
        if not has:
            raise RuntimeError("No existe la tabla cardIdentifiers en AllPrintings.sqlite (¿cambió el esquema?)")

        n = 0
        batch_uuids: list[str] = []
        batch_sids: list[str] = []

        for uuid_val, sid_val in src.execute("SELECT uuid, scryfallId FROM cardIdentifiers WHERE scryfallId IS NOT NULL"):
            batch_uuids.append(uuid_val)
            batch_sids.append(sid_val.lower())
            if len(batch_uuids) >= BATCH_SIZE:
                await db.execute_raw(
                    """
                    INSERT INTO mtgjson_uuid_map (uuid, scryfall_id)
                    SELECT * FROM UNNEST($1::text[], $2::text[])
                    ON CONFLICT (uuid) DO UPDATE SET scryfall_id = EXCLUDED.scryfall_id
                    """,
                    batch_uuids,
                    batch_sids,
                )
                n += len(batch_uuids)
                batch_uuids = []
                batch_sids = []

        if batch_uuids:
            await db.execute_raw(
                """
                INSERT INTO mtgjson_uuid_map (uuid, scryfall_id)
                SELECT * FROM UNNEST($1::text[], $2::text[])
                ON CONFLICT (uuid) DO UPDATE SET scryfall_id = EXCLUDED.scryfall_id
                """,
                batch_uuids,
                batch_sids,
            )
            n += len(batch_uuids)
            batch_uuids = []
            batch_sids = []

        return n
    finally:
        src.close()


async def _flush_batch(
    db,
    rows: list[tuple[str, int, str, int]],
    anomaly_rows: list[tuple[str, int, str, int, int, str]],
    resolved_keys: list[tuple[str, int]],
    current_prices: dict[str, dict[str, float]],
):
    if rows:
        sids = [r[0] for r in rows]
        finishes = [r[1] for r in rows]
        dates = [r[2] for r in rows]
        cents = [r[3] for r in rows]
        await db.execute_raw(
            """
            INSERT INTO cm_price_history (scryfall_id, finish, date, price_cents)
            SELECT * FROM UNNEST($1::text[], $2::smallint[], $3::date[], $4::int[])
            ON CONFLICT (scryfall_id, date, finish)
            DO UPDATE SET price_cents = EXCLUDED.price_cents
            """,
            sids,
            finishes,
            dates,
            cents,
        )

    if anomaly_rows:
        a_sids = [r[0] for r in anomaly_rows]
        a_finishes = [r[1] for r in anomaly_rows]
        a_dates = [r[2] for r in anomaly_rows]
        a_cents = [r[3] for r in anomaly_rows]
        a_prev = [r[4] for r in anomaly_rows]
        await db.execute_raw(
            """
            INSERT INTO price_anomalies (id, scryfall_id, finish, date, price_cents, prev_cents, detected_at, resolved)
            SELECT gen_random_uuid()::text, u.sid, u.finish, u.dt, u.cents, u.prev, NOW(), false
            FROM UNNEST($1::text[], $2::smallint[], $3::date[], $4::int[], $5::int[]) AS u(sid, finish, dt, cents, prev)
            ON CONFLICT (scryfall_id, finish, date)
            DO UPDATE SET price_cents = EXCLUDED.price_cents, prev_cents = EXCLUDED.prev_cents, detected_at = NOW(), resolved = false
            """,
            a_sids,
            a_finishes,
            a_dates,
            a_cents,
            a_prev,
        )

    if resolved_keys:
        for sid, finish in resolved_keys:
            await db.execute_raw(
                "UPDATE price_anomalies SET resolved = true WHERE scryfall_id = $1 AND finish = $2 AND resolved = false",
                sid,
                finish,
            )

    if current_prices:
        cp_ids = list(current_prices.keys())
        cp_trends = [current_prices[cid].get("trend") for cid in cp_ids]
        cp_eurs = [current_prices[cid].get("eur") for cid in cp_ids]
        cp_foils = [current_prices[cid].get("foil") for cid in cp_ids]
        await db.execute_raw(
            """
            UPDATE card_printings AS cp
            SET
                price_cardmarket_trend = v.trend,
                price_eur = COALESCE(v.eur, cp.price_eur),
                price_eur_foil = COALESCE(v.foil, cp.price_eur_foil),
                prices_updated_at = NOW(),
                updated_at = NOW()
            FROM (
                SELECT * FROM UNNEST($1::text[], $2::double precision[], $3::double precision[], $4::double precision[])
                AS t(id, trend, eur, foil)
            ) AS v
            WHERE cp.id = v.id
            """,
            cp_ids,
            cp_trends,
            cp_eurs,
            cp_foils,
        )

    rows.clear()
    anomaly_rows.clear()
    resolved_keys.clear()
    current_prices.clear()


async def ingest_file(
    db,
    path: Path,
    cards_query: str = DEFAULT_CARDS_QUERY,
    source_url: str = "",
    force: bool = False,
) -> Stats:
    st = Stats()
    try:
        st.feed_date = read_feed_date(path)
    except Exception as exc:
        log.warning("No se pudo leer meta.date (%s); se intentará la ingesta completa.", exc)

    if st.feed_date and not force:
        done_rows = await db.query_raw(
            "SELECT 1 FROM ingest_runs WHERE feed_date = $1 AND source_url = $2 AND status IN ('success','warning')",
            st.feed_date,
            source_url,
        )
        if done_rows:
            st.status = "skipped"
            log.info("Feed %s ya ingerido; nada que hacer (usa force=True para repetir).", st.feed_date)
            return st

    prev_rows = await db.query_raw(
        "SELECT matched FROM ingest_runs WHERE source_url = $1 AND status IN ('success','warning') "
        "ORDER BY id DESC LIMIT 1",
        source_url,
    )
    prev_matched = prev_rows[0].get("matched") if (prev_rows and isinstance(prev_rows[0], dict)) else (prev_rows[0][0] if prev_rows else None)

    run_rows = await db.query_raw(
        "INSERT INTO ingest_runs (source_url, feed_date, status, started_at) VALUES ($1, $2, 'running', NOW()) RETURNING id",
        source_url,
        st.feed_date,
    )
    run_id = run_rows[0]["id"] if isinstance(run_rows[0], dict) else run_rows[0][0]

    try:
        tracked = await load_tracked(db, cards_query)
        uuid_map, multi = await load_uuid_map(db, tracked)
        st.tracked = len(tracked)
        st.mapped = len(set(uuid_map.values()))
        prev = await load_latest(db)
        pending = await load_pending(db)

        rows: list[tuple[str, int, str, int]] = []
        anomaly_rows: list[tuple[str, int, str, int, int, str]] = []
        resolved_keys: list[tuple[str, int]] = []
        current_prices: dict[str, dict[str, float]] = {}
        matched: set[str] = set()
        seen_multi: dict[tuple, int] = {}

        with _open_maybe_gz(path) as f:
            for uuid_val, entry in ijson.kvitems(f, "data"):
                sid = uuid_map.get(uuid_val)
                if sid is None:
                    continue
                points = sorted(iter_cardmarket_points(entry), key=lambda p: (p[0], str(p[1])))
                for finish, d, raw in points:
                    cents = price_to_cents(raw)
                    if cents is None or not valid_iso_date(d):
                        st.invalid += 1
                        continue

                    if sid in multi:
                        k = (sid, finish, d)
                        if k in seen_multi:
                            if seen_multi[k] != cents:
                                st.conflicts += 1
                            continue
                        seen_multi[k] = cents

                    matched.add(sid)
                    key = (sid, finish)
                    p = prev.get(key)
                    if p and p[0] < d and is_anomalous(p[1], cents):
                        pend = pending.get(key)
                        confirmed = (
                            pend is not None
                            and pend[0] < d
                            and abs(Decimal(cents) - Decimal(pend[1])) <= Decimal(pend[1]) * CONFIRM_TOLERANCE
                        )
                        if not confirmed:
                            anomaly_rows.append((sid, finish, d, cents, p[1], _now()))
                            pending[key] = (d, cents)
                            st.anomalies += 1
                            continue
                        resolved_keys.append(key)
                        pending.pop(key, None)
                        st.confirmed += 1

                    rows.append((sid, finish, d, cents))
                    st.inserted += 1

                    # Track current price in EUR for card_printings update
                    cur = current_prices.setdefault(sid, {})
                    eur_val = round(cents / 100.0, 2)
                    if finish == 0:
                        cur["eur"] = eur_val
                        cur["trend"] = eur_val
                    elif finish == 1:
                        cur["foil"] = eur_val
                        if "eur" not in cur:
                            cur["trend"] = eur_val
                    elif finish == 2:
                        if "eur" not in cur and "foil" not in cur:
                            cur["trend"] = eur_val

                    if p is None or d > p[0]:
                        prev[key] = (d, cents)

                if len(rows) >= BATCH_SIZE:
                    await _flush_batch(db, rows, anomaly_rows, resolved_keys, current_prices)

        await _flush_batch(db, rows, anomaly_rows, resolved_keys, current_prices)
        st.matched = len(matched)

        if st.tracked and st.mapped and st.matched == 0:
            raise RuntimeError("Ninguna de tus cartas obtuvo precio: ¿cambió el formato del feed o falló el mapeo?")

        st.status = "success"
        if prev_matched and st.matched < prev_matched * COVERAGE_DROP_WARN:
            st.status = "warning"
            st.notes.append(f"Cobertura cayó: {st.matched} vs {prev_matched} en la ejecución anterior")
            log.warning(st.notes[-1])
        if st.tracked - st.mapped:
            st.notes.append(f"{st.tracked - st.mapped} cartas sin uuid de MTGJSON (ejecuta sync-map)")

    except Exception as exc:
        await db.execute_raw(
            "UPDATE ingest_runs SET status='failed', error=$1, finished_at=NOW() WHERE id=$2",
            f"{type(exc).__name__}: {exc}",
            run_id,
        )
        raise

    await db.execute_raw(
        """
        UPDATE ingest_runs SET
            status = $1, tracked = $2, mapped = $3, matched = $4, inserted = $5,
            invalid = $6, anomalies = $7, conflicts = $8, notes = $9, finished_at = NOW()
        WHERE id = $10
        """,
        st.status,
        st.tracked,
        st.mapped,
        st.matched,
        st.inserted,
        st.invalid,
        st.anomalies,
        st.conflicts,
        "; ".join(st.notes) or None,
        run_id,
    )
    return st


# --------------------------------------------------------------------------- workflows de alto nivel

async def sync_today(db, force: bool = False, no_verify: bool = False) -> Stats:
    """Descarga AllPricesToday.json.gz e ingesta los precios de hoy."""
    with tempfile.TemporaryDirectory(prefix="mtgjson-today-") as tmp:
        path = download(PRICES_TODAY_URL, Path(tmp) / "prices.json.gz", verify=not no_verify)
        return await ingest_file(db, path, source_url=PRICES_TODAY_URL, force=force)


async def backfill_history(db, force: bool = False, no_verify: bool = False) -> Stats:
    """Descarga AllPrices.json.gz e ingesta los últimos ~90 días de precios."""
    with tempfile.TemporaryDirectory(prefix="mtgjson-backfill-") as tmp:
        path = download(PRICES_90D_URL, Path(tmp) / "prices_90d.json.gz", verify=not no_verify)
        return await ingest_file(db, path, source_url=PRICES_90D_URL, force=force)


async def sync_map_remote(db, no_verify: bool = False) -> int:
    """Descarga AllPrintings.sqlite.gz y actualiza mtgjson_uuid_map."""
    with tempfile.TemporaryDirectory(prefix="mtgjson-map-") as tmp:
        gz = download(PRINTINGS_SQLITE_URL, Path(tmp) / "AllPrintings.sqlite.gz", verify=not no_verify)
        sqlite_path = Path(tmp) / "AllPrintings.sqlite"
        with gzip.open(gz, "rb") as fi, open(sqlite_path, "wb") as fo:
            shutil.copyfileobj(fi, fo)
        return await sync_uuid_map(db, sqlite_path)


async def get_report(db, cards_query: str = DEFAULT_CARDS_QUERY) -> dict[str, Any]:
    tracked = await load_tracked(db, cards_query)
    mapped_rows = await db.query_raw("SELECT DISTINCT scryfall_id FROM mtgjson_uuid_map")
    mapped = {r["scryfall_id"].lower() if isinstance(r, dict) else r[0].lower() for r in mapped_rows}
    unmapped = sorted(tracked - mapped)

    last_rows = await db.query_raw("SELECT MAX(date)::text, COUNT(*) FROM cm_price_history")
    last = last_rows[0] if last_rows else {}
    last_date = (last.get("max") if isinstance(last, dict) else last[0]) if last else None
    total_points = (last.get("count") if isinstance(last, dict) else last[1]) if last else 0

    run_rows = await db.query_raw(
        "SELECT feed_date, status, matched, inserted, anomalies, conflicts, notes, error, started_at, finished_at "
        "FROM ingest_runs ORDER BY id DESC LIMIT 1"
    )
    last_run = run_rows[0] if run_rows else None

    pend_rows = await db.query_raw("SELECT COUNT(*) FROM price_anomalies WHERE resolved = false")
    pend = (pend_rows[0].get("count") if isinstance(pend_rows[0], dict) else pend_rows[0][0]) if pend_rows else 0

    return {
        "tracked_cards": len(tracked),
        "unmapped_cards": len(unmapped),
        "unmapped_samples": unmapped[:20],
        "history_points": total_points,
        "history_last_date": str(last_date)[:10] if last_date else None,
        "last_run": last_run,
        "pending_anomalies": pend,
    }


# --------------------------------------------------------------------------- CLI

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cards-query", default=DEFAULT_CARDS_QUERY, help="SQL que devuelve los scryfall_id")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sync-map")
    s.add_argument("--printings-sqlite", help="Ruta a AllPrintings.sqlite local (si no, usa --download)")
    s.add_argument("--download", action="store_true")
    s.add_argument("--no-verify", action="store_true")

    for name in ("ingest", "backfill"):
        i = sub.add_parser(name)
        i.add_argument("--file", help="Usar un fichero local en vez de descargar")
        i.add_argument("--url")
        i.add_argument("--force", action="store_true")
        i.add_argument("--no-verify", action="store_true")

    sub.add_parser("report")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    from src.db import db, connect_db, disconnect_db

    async def _run():
        await connect_db()
        try:
            if args.cmd == "report":
                rep = await get_report(db, args.cards_query)
                print(f"Cartas seguidas: {rep['tracked_cards']} | sin uuid MTGJSON: {rep['unmapped_cards']}")
                print(f"Histórico: {rep['history_points']} filas, última fecha {rep['history_last_date']}")
                print(f"Última ejecución: {rep['last_run']}")
                print(f"Anomalías pendientes: {rep['pending_anomalies']}")
                for sid in rep["unmapped_samples"]:
                    print("  sin mapear:", sid)
                return 0

            if args.cmd == "sync-map":
                if args.printings_sqlite:
                    n = await sync_uuid_map(db, Path(args.printings_sqlite))
                elif args.download:
                    n = await sync_map_remote(db, no_verify=args.no_verify)
                else:
                    ap.error("indica --printings-sqlite o --download")
                log.info("Mapa actualizado: %d uuids", n)
                return 0

            url = args.url or (PRICES_90D_URL if args.cmd == "backfill" else PRICES_TODAY_URL)
            if args.file:
                st = await ingest_file(db, Path(args.file), args.cards_query, source_url=url, force=args.force)
            else:
                with tempfile.TemporaryDirectory() as tmp:
                    path = download(url, Path(tmp) / "prices.json.gz", verify=not args.no_verify)
                    st = await ingest_file(db, path, args.cards_query, source_url=url, force=args.force)
            log.info("Resultado: %s", st)
            return 0
        finally:
            await disconnect_db()

    try:
        return asyncio.run(_run())
    except Exception:
        log.exception("La ejecución falló")
        return 1


if __name__ == "__main__":
    sys.exit(main())
