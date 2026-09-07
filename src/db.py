import logging
from prisma import Prisma

logger = logging.getLogger("mtg_set_worker.db")

db = Prisma(auto_register=True)

async def connect_db():
    if not db.is_connected():
        logger.info("Connecting to PostgreSQL database via Prisma...")
        await db.connect()
        logger.info("Database connected successfully.")

async def disconnect_db():
    if db.is_connected():
        logger.info("Disconnecting from database...")
        await db.disconnect()
        logger.info("Database disconnected.")
