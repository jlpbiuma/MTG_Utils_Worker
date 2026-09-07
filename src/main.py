import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional
from fastapi import BackgroundTasks, FastAPI
import uvicorn

from src.config import settings
from src.db import connect_db, disconnect_db
from src.worker import SetWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("mtg_set_worker")

# Shared state for health and metrics
worker_state: Dict[str, Any] = {
    "is_running": False,
    "last_run": None,
    "last_result": None,
    "total_runs": 0,
}

_periodic_task: Optional[asyncio.Task] = None
_shutdown_event = asyncio.Event()

async def run_worker_task():
    """Runs the set sync worker safely with state tracking."""
    if worker_state["is_running"]:
        logger.warning("Set worker is already running. Skipping trigger.")
        return {"status": "busy", "message": "Worker is currently running"}

    worker_state["is_running"] = True
    try:
        worker = SetWorker()
        result = await worker.run()
        worker_state["last_run"] = result.get("timestamp")
        worker_state["last_result"] = result
        worker_state["total_runs"] += 1
        return result
    except Exception as e:
        logger.error(f"Error during set worker execution: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}
    finally:
        worker_state["is_running"] = False

async def periodic_scheduler():
    """Periodic loop activating the set worker every UPDATE_INTERVAL_SECONDS."""
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
                logger.info("⏰ Scheduled interval reached. Triggering set worker...")
                await run_worker_task()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _periodic_task
    logger.info("Initializing MTG Set Worker service...")
    try:
        await connect_db()
    except Exception as e:
        logger.error(f"Could not connect to database on startup: {e}")

    # Start background scheduler
    _periodic_task = asyncio.create_task(periodic_scheduler())
    yield

    # Shutdown
    logger.info("Stopping MTG Set Worker service...")
    _shutdown_event.set()
    if _periodic_task:
        _periodic_task.cancel()
        try:
            await _periodic_task
        except asyncio.CancelledError:
            pass

    await disconnect_db()
    logger.info("MTG Set Worker stopped cleanly.")

app = FastAPI(
    title="MTG Utils Set Worker",
    version="1.0.0",
    description="Background daemon for periodic Scryfall sets and card printings synchronization",
    lifespan=lifespan,
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "set-worker",
        "is_running": worker_state["is_running"],
        "interval_seconds": settings.UPDATE_INTERVAL_SECONDS,
        "last_run": worker_state["last_run"],
    }

@app.get("/status")
async def get_status():
    return {
        "service": "set-worker",
        "config": {
            "interval_seconds": settings.UPDATE_INTERVAL_SECONDS,
            "sets_per_cycle": settings.SETS_PER_CYCLE,
            "rate_limit_delay_seconds": settings.RATE_LIMIT_DELAY_SECONDS,
            "download_digital_sets": settings.DOWNLOAD_DIGITAL_SETS,
        },
        "state": worker_state,
    }

@app.post("/trigger")
async def trigger_run(background_tasks: BackgroundTasks):
    """Manually triggers an immediate set sync in the background."""
    if worker_state["is_running"]:
        return {"status": "busy", "message": "Worker is already running"}

    background_tasks.add_task(run_worker_task)
    return {
        "status": "accepted",
        "message": "Set worker sync triggered in background",
    }

def handle_exit_signal(sig, frame):
    logger.info(f"Received exit signal {sig}, terminating gracefully...")
    _shutdown_event.set()
    sys.exit(0)

def start():
    """Main entrypoint when running the module directly."""
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    if settings.ENABLE_HTTP_SERVER:
        logger.info(f"Starting Set Worker HTTP API on {settings.HTTP_HOST}:{settings.HTTP_PORT}...")
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
