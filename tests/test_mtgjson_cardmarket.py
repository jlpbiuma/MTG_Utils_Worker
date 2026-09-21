import gzip
import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.services.mtgjson_cardmarket as m

Q = "SELECT id FROM card_printings"

# Scryfall IDs y uuids de MTGJSON ficticios
S1, S2, S3, S_DFC, S_UNTRACKED = "s-1", "s-2", "s-3", "s-dfc", "s-untracked"
U1, U2, U3, U_DFC_A, U_DFC_B, U_UNTRACKED, U_UNMAPPED = "u-1", "u-2", "u-3", "u-dfc-a", "u-dfc-b", "u-x", "u-unmapped"


def cm(retail=None, **other):
    entry = {"paper": {"cardmarket": {"currency": "EUR", "retail": retail or {}}}}
    entry["paper"].update(other)
    return entry


def write_feed(path: Path, data: dict, date: str = "2026-09-19", gz: bool = True) -> Path:
    payload = json.dumps({"meta": {"date": date, "version": "5.x"}, "data": data}).encode()
    if gz:
        with gzip.open(path, "wb") as f:
            f.write(payload)
    else:
        path.write_bytes(payload)
    return path


# ------------------------------------------------------------------ unitarios

@pytest.mark.parametrize("value,expected", [
    (12.34, 1234), (Decimal("0.05"), 5), (Decimal("2.675"), 268), (1, 100), (0.1, 10),
    (0, None), (-1, None), (None, None), ("3.5", None), (True, None), (float("nan"), None), (float("inf"), None),
])
def test_price_to_cents(value, expected):
    assert m.price_to_cents(value) == expected


def test_valid_iso_date():
    assert m.valid_iso_date("2026-09-19")
    assert not m.valid_iso_date("2026-13-40")
    assert not m.valid_iso_date(20260919)


def test_is_anomalous():
    assert m.is_anomalous(500, 15000)        # 5€ -> 150€
    assert m.is_anomalous(15000, 500)        # caída brusca
    assert not m.is_anomalous(10, 60)        # x6 pero solo 0,50€ de diferencia
    assert not m.is_anomalous(1000, 2000)    # subida normal


def test_sha256_file(tmp_path):
    p = tmp_path / "x"
    p.write_bytes(b"hola")
    assert m.sha256_file(p) == "b221d9dbb083a7f33428d7c2a3c3198ae925614d70210e28716ccaa7cd4ddb79"


def test_read_feed_date(tmp_path):
    feed = write_feed(tmp_path / "f.json.gz", {}, date="2026-09-19")
    assert m.read_feed_date(feed) == "2026-09-19"


def test_iter_cardmarket_points_extracts_only_cardmarket():
    entry = {
        "paper": {
            "cardmarket": {
                "retail": {
                    "normal": {"2026-09-19": 1.50},
                    "foil": {"2026-09-19": 3.00},
                },
                "buylist": {"normal": {"2026-09-19": 0.50}},
            },
            "tcgplayer": {"retail": {"normal": {"2026-09-19": 99.0}}},
        }
    }
    points = list(m.iter_cardmarket_points(entry))
    assert points == [(0, "2026-09-19", 1.50), (1, "2026-09-19", 3.00)]


# ------------------------------------------------------------------ sync_uuid_map

@pytest.mark.asyncio
async def test_sync_uuid_map_from_allprintings_sqlite(tmp_path):
    src = tmp_path / "AllPrintings.sqlite"
    s = sqlite3.connect(src)
    s.execute("CREATE TABLE cardIdentifiers (uuid TEXT, scryfallId TEXT, mcmId TEXT)")
    s.executemany(
        "INSERT INTO cardIdentifiers VALUES (?,?,?)",
        [("u-a", "SCRY-A", "1"), ("u-b", None, "2"), ("u-c", "SCRY-C", None)],
    )
    s.commit()
    s.close()

    mock_db = MagicMock()
    mock_db.execute_raw = AsyncMock()

    count = await m.sync_uuid_map(mock_db, src)
    assert count == 2
    mock_db.execute_raw.assert_awaited_once()
    args = mock_db.execute_raw.await_args[0]
    uuids = args[1]
    sids = args[2]
    assert uuids == ["u-a", "u-c"]
    assert sids == ["scry-a", "scry-c"]


@pytest.mark.asyncio
async def test_sync_uuid_map_fails_clearly_if_schema_changed(tmp_path):
    src = tmp_path / "bad.sqlite"
    sqlite3.connect(src).close()
    mock_db = MagicMock()
    with pytest.raises(RuntimeError, match="No existe la tabla cardIdentifiers"):
        await m.sync_uuid_map(mock_db, src)


# ------------------------------------------------------------------ ingesta con mock db

@pytest.mark.asyncio
async def test_ingest_file_only_cardmarket_and_updates_card_printings(tmp_path):
    feed = write_feed(tmp_path / "f.json.gz", {
        U1: {
            "paper": {
                "cardmarket": {
                    "currency": "EUR",
                    "retail": {
                        "normal": {"2026-09-19": 1.5},
                        "foil": {"2026-09-19": 3.25},
                        "etched": {"2026-09-19": 9.99},
                    },
                },
                "tcgplayer": {"currency": "USD", "retail": {"normal": {"2026-09-19": 99.0}}},
            },
        }
    })

    mock_db = MagicMock()
    # 1. ingest_runs check
    # 2. prev_run
    # 3. insert ingest_runs
    # 4. load_tracked
    # 5. load_uuid_map
    # 6. load_latest
    # 7. load_pending
    mock_db.query_raw = AsyncMock(side_effect=[
        [],                              # not already done
        [{"matched": 1}],               # prev_run
        [{"id": 10}],                   # ingest_runs created
        [{"id": S1}],                   # tracked cards
        [{"uuid": U1, "scryfall_id": S1}], # uuid_map
        [],                             # load_latest
        [],                             # load_pending
    ])
    mock_db.execute_raw = AsyncMock()

    stats = await m.ingest_file(mock_db, feed, cards_query=Q, source_url="url", force=False)

    assert stats.status == "success"
    assert stats.matched == 1
    assert stats.inserted == 3

    # Verify cm_price_history insert and card_printings update
    assert mock_db.execute_raw.await_count >= 2
    executed_queries = [call[0][0] for call in mock_db.execute_raw.await_args_list]
    assert any("INSERT INTO cm_price_history" in q for q in executed_queries)
    assert any("UPDATE card_printings" in q for q in executed_queries)
    assert any("UPDATE ingest_runs" in q for q in executed_queries)


@pytest.mark.asyncio
async def test_same_feed_date_is_skipped_unless_forced(tmp_path):
    feed = write_feed(tmp_path / "f.json.gz", {U1: cm({"normal": {"2026-09-19": 1.0}})})

    mock_db = MagicMock()
    mock_db.query_raw = AsyncMock(return_value=[{"?column?": 1}])  # already done

    st = await m.ingest_file(mock_db, feed, cards_query=Q, source_url="url", force=False)
    assert st.status == "skipped"


@pytest.mark.asyncio
async def test_zero_matches_raises_runtime_error(tmp_path):
    feed = write_feed(tmp_path / "f.json.gz", {U1: {"paper": {"cardmarket_typo": {}}}})

    mock_db = MagicMock()
    mock_db.query_raw = AsyncMock(side_effect=[
        [],                              # not already done
        [],                              # prev_run
        [{"id": 10}],                   # ingest_runs created
        [{"id": S1}],                   # tracked
        [{"uuid": U1, "scryfall_id": S1}], # mapped
        [],                             # load_latest
        [],                             # load_pending
    ])
    mock_db.execute_raw = AsyncMock()

    with pytest.raises(RuntimeError, match="Ninguna de tus cartas obtuvo precio"):
        await m.ingest_file(mock_db, feed, cards_query=Q, source_url="url", force=False)

    # Status failed is recorded
    update_call = mock_db.execute_raw.await_args_list[-1]
    assert "status='failed'" in update_call[0][0]
