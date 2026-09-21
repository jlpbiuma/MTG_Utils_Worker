import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Set
from fastapi import BackgroundTasks, FastAPI
import uvicorn

from src.config import settings
from src.db import connect_db, disconnect_db, db
from src.worker import Worker
from src.price_history import PriceHistoryWorker
from src.services.mtgjson_cardmarket import sync_today, backfill_history, sync_map_remote, get_report
from src.services.scryfall import ScryfallClient
from src.services.card_utils import normalize_card_name
from src.rules import RulesWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("mtg_worker")

# Shared state for health and metrics
worker_state: Dict[str, Any] = {
    "is_running": False,
    "last_run": None,
    "last_result": None,
    "total_runs": 0,
}

_periodic_task: Optional[asyncio.Task] = None
_queue_task: Optional[asyncio.Task] = None
_bulk_task: Optional[asyncio.Task] = None
_shutdown_event = asyncio.Event()
_priority_tasks: Set[asyncio.Task] = set()

async def run_worker_task():
    """Runs the unified worker safely with state tracking."""
    if worker_state["is_running"]:
        logger.warning("Worker is already running. Skipping trigger.")
        return {"status": "busy", "message": "Worker is currently running"}

    worker_state["is_running"] = True
    worker = None
    try:
        worker = Worker()
        sets = await worker.run()
        prices = await PriceHistoryWorker(worker.db).run()
        rules = await RulesWorker(worker.db).run()
        result = {"status": "success", "timestamp": sets["timestamp"], "sets": sets, "prices": prices, "rules": rules}
        worker_state["last_run"] = result.get("timestamp")
        worker_state["last_result"] = result
        worker_state["total_runs"] += 1
        return result
    except Exception as e:
        logger.error(f"Error during worker execution: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}
    finally:
        if worker:
            try:
                await worker.close()
            except Exception as e:
                logger.warning("Could not close image storage session: %s", e)
        worker_state["is_running"] = False

async def periodic_scheduler():
    """Periodic loop activating the worker every UPDATE_INTERVAL_SECONDS."""
    logger.info(
        f"⏰ Scheduler initialized with interval of {settings.UPDATE_INTERVAL_SECONDS} seconds "
        f"({settings.UPDATE_INTERVAL_SECONDS / 60:.1f}m)."
    )

    if settings.RUN_ON_STARTUP:
        logger.info("⚡ RUN_ON_STARTUP is enabled. Executing initial sync in 5 seconds...")
        await asyncio.sleep(5)
        await run_worker_task()

    while not _shutdown_event.is_set():
        try:
            logger.info(f"⏳ Sleeping for {settings.UPDATE_INTERVAL_SECONDS}s until next scheduled sync...")
            await asyncio.wait_for(_shutdown_event.wait(), timeout=settings.UPDATE_INTERVAL_SECONDS)
            break
        except asyncio.TimeoutError:
            if not _shutdown_event.is_set():
                logger.info("⏰ Scheduled interval reached. Triggering worker...")
                await run_worker_task()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _periodic_task, _queue_task, _bulk_task
    logger.info("Initializing MTG worker service...")
    try:
        await connect_db()
    except Exception as e:
        logger.error(f"Could not connect to database on startup: {e}")

    # Start background scheduler
    _periodic_task = asyncio.create_task(periodic_scheduler())
    from src.priority_queue import supervisor
    from src.services.bulk_catalog import bulk_scheduler
    _queue_task = asyncio.create_task(supervisor(db))
    _bulk_task = asyncio.create_task(bulk_scheduler(db))
    yield

    # Shutdown
    logger.info("Stopping MTG worker service...")
    _shutdown_event.set()
    if _periodic_task:
        _periodic_task.cancel()
        try:
            await _periodic_task
        except asyncio.CancelledError:
            pass

    for task in (_queue_task, _bulk_task):
        if task:
            task.cancel()
    await asyncio.gather(*[t for t in (_queue_task,_bulk_task) if t], return_exceptions=True)
    await disconnect_db()
    logger.info("MTG worker stopped cleanly.")

app = FastAPI(
    title="MTG Utils Worker",
    version="1.0.0",
    description="Unified daemon for set cataloguing, MinIO images, and sparse weekly price history",
    lifespan=lifespan,
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "worker",
        "is_running": worker_state["is_running"],
        "interval_seconds": settings.UPDATE_INTERVAL_SECONDS,
        "last_run": worker_state["last_run"],
    }

@app.get("/status")
async def get_status():
    return {
        "service": "worker",
        "config": {
            "interval_seconds": settings.UPDATE_INTERVAL_SECONDS,
            "sets_per_cycle": settings.SETS_PER_CYCLE,
            "rate_limit_delay_seconds": settings.RATE_LIMIT_DELAY_SECONDS,
            "download_digital_sets": settings.DOWNLOAD_DIGITAL_SETS,
            "price_sync_interval_days": settings.PRICE_SYNC_INTERVAL_DAYS,
            "price_change_threshold": settings.PRICE_CHANGE_THRESHOLD,
            "tor_enabled": settings.TOR_ENABLED,
        },
        "state": worker_state,
    }

@app.post("/trigger")
async def trigger_run(background_tasks: BackgroundTasks):
    """Manually triggers an immediate worker sync in the background."""
    if worker_state["is_running"]:
        return {"status": "busy", "message": "Worker is already running"}

    background_tasks.add_task(run_worker_task)
    return {
        "status": "accepted",
        "message": "Unified worker sync triggered in background",
    }

@app.post("/prioritize")
async def prioritize_imported_cards(card_names: List[str]):
    """Download user-imported cards immediately, independently of the cycle."""
    names = list(dict.fromkeys(name.strip() for name in card_names if name and name.strip()))
    if not names:
        return {"status": "accepted", "prioritized": 0}

    from src.priority_queue import enqueue_names
    count = await enqueue_names(db, names)
    return {"status": "accepted", "prioritized": count}

@app.post("/enrich-card")
async def enrich_card(payload: Dict[str, Any]):
    """
    Synchronously enriches a single card (Scryfall data + images + prices +
    rulings) and returns its persisted catalog entry. Blocks until complete.
    """
    name = payload.get("name")
    card_id = payload.get("id")
    if not card_id and not name:
        return {"status": "error", "error": "Provide a card name or id"}

    worker = Worker()
    try:
        if not name and card_id:
            resolved = await worker.scryfall.fetch_cards_by_ids([card_id])
            for card_data in resolved.values():
                name = card_data.get("name")
                break
            if not name:
                return {"status": "not_found", "card": None}

        result = await worker.download_priority_cards(
            [name],
            include_all_printings=True,
            update_linked_cards=False,
        )
        if result.get("downloaded", 0) == 0:
            return {"status": "error", "error": result, "card": None}

        catalog = await worker.db.cardcatalog.find_unique(
            where={"normalizedName": normalize_card_name(name)}
        )
        if not catalog and result.get("cards"):
            canonical_name = result["cards"][0].get("name")
            if canonical_name:
                catalog = await worker.db.cardcatalog.find_unique(
                    where={"normalizedName": normalize_card_name(canonical_name)}
                )
        if not catalog:
            return {"status": "not_found", "card": None}

        card = dict(catalog.detailsEs or {})
        card["id"] = catalog.id
        card["name"] = catalog.name
        card["mana_cost"] = catalog.manaCost
        card["type_line"] = catalog.typeLine
        return {"status": "enriched", "card": card}
    except Exception as error:
        logger.exception("Synchronous card enrichment failed: %s", error)
        return {"status": "error", "error": str(error), "card": None}
    finally:
        await worker.close()

@app.post("/prices/sync")
async def trigger_price_sync(background_tasks: BackgroundTasks, force: bool = False):
    """Trigger daily MTGJSON Cardmarket price synchronization."""
    async def _do_sync():
        try:
            logger.info("Starting on-demand MTGJSON Cardmarket price sync (force=%s)...", force)
            res = await sync_today(db, force=force)
            logger.info("MTGJSON Cardmarket price sync finished: %s", res)
        except Exception as exc:
            logger.exception("MTGJSON price sync failed: %s", exc)

    background_tasks.add_task(_do_sync)
    return {"status": "accepted", "message": "MTGJSON Cardmarket price sync triggered in background"}

@app.post("/prices/backfill")
async def trigger_price_backfill(background_tasks: BackgroundTasks, force: bool = False):
    """Trigger ~90-day MTGJSON Cardmarket price backfill."""
    async def _do_backfill():
        try:
            logger.info("Starting on-demand MTGJSON Cardmarket price backfill (force=%s)...", force)
            res = await backfill_history(db, force=force)
            logger.info("MTGJSON Cardmarket price backfill finished: %s", res)
        except Exception as exc:
            logger.exception("MTGJSON price backfill failed: %s", exc)

    background_tasks.add_task(_do_backfill)
    return {"status": "accepted", "message": "MTGJSON Cardmarket price backfill triggered in background"}

@app.post("/prices/sync-map")
async def trigger_sync_map(background_tasks: BackgroundTasks):
    """Trigger Scryfall ID <-> MTGJSON UUID mapping sync from AllPrintings.sqlite."""
    async def _do_map_sync():
        try:
            logger.info("Starting on-demand MTGJSON UUID map sync...")
            count = await sync_map_remote(db)
            logger.info("MTGJSON UUID map sync finished: %s uuids mapped", count)
        except Exception as exc:
            logger.exception("MTGJSON UUID map sync failed: %s", exc)

    background_tasks.add_task(_do_map_sync)
    return {"status": "accepted", "message": "MTGJSON UUID map sync triggered in background"}

@app.get("/prices/report")
async def get_prices_report():
    """Returns coverage, unmapped cards, last ingest run, and pending anomalies."""
    try:
        report_data = await get_report(db)
        return {"status": "success", "report": report_data}
    except Exception as exc:
        logger.exception("Failed to generate price report: %s", exc)
        return {"status": "error", "error": str(exc)}

@app.get("/prices/anomalies")
async def get_price_anomalies(limit: int = 50):
    """List pending price anomalies quarantined by the system."""
    try:
        anomalies = await db.priceanomaly.find_many(
            where={"resolved": False},
            take=limit,
            order={"date": "desc"},
        )
        return {"status": "success", "count": len(anomalies), "anomalies": anomalies}
    except Exception as exc:
        logger.exception("Failed to query price anomalies: %s", exc)
        return {"status": "error", "error": str(exc)}

def handle_exit_signal(sig, frame):
    logger.info(f"Received exit signal {sig}, terminating gracefully...")
    _shutdown_event.set()
    sys.exit(0)

def start():
    """Main entrypoint when running the module directly."""
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    if settings.ENABLE_HTTP_SERVER:
        logger.info(f"Starting worker HTTP API on {settings.HTTP_HOST}:{settings.HTTP_PORT}...")
        uvicorn.run(
            app,
            host=settings.HTTP_HOST,
            port=settings.HTTP_PORT,
            log_level="info",
        )
    else:
        logger.info("HTTP Server disabled. Running standalone scheduler...")
        asyncio.run(periodic_scheduler())

if __name__ == "__main__":
    start()
