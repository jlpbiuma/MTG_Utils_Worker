import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/mtg_utils?schema=public"
    )
    UPDATE_INTERVAL_SECONDS: int = int(os.getenv("UPDATE_INTERVAL_SECONDS", "300"))  # 5 minutes default
    RUN_ON_STARTUP: bool = os.getenv("RUN_ON_STARTUP", "true").lower() in ("true", "1", "yes")
    SETS_PER_CYCLE: int = int(os.getenv("SETS_PER_CYCLE", "1"))  # One complete set is the atomic unit of work
    RATE_LIMIT_DELAY_SECONDS: float = float(os.getenv("RATE_LIMIT_DELAY_SECONDS", "0.1"))  # 100ms polite pause
    DOWNLOAD_DIGITAL_SETS: bool = os.getenv("DOWNLOAD_DIGITAL_SETS", "false").lower() in ("true", "1", "yes")
    HTTP_PORT: int = int(os.getenv("HTTP_PORT", "8001"))
    HTTP_HOST: str = os.getenv("HTTP_HOST", "0.0.0.0")
    ENABLE_HTTP_SERVER: bool = os.getenv("ENABLE_HTTP_SERVER", "true").lower() in ("true", "1", "yes")
    SCRYFALL_API_BASE: str = os.getenv("SCRYFALL_API_BASE", "https://api.scryfall.com")
    USER_AGENT: str = os.getenv("USER_AGENT", "MTGUtilsWorker/1.0")
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "mtg-utils")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "change-this-minio-password")
    MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "mtg-images")
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() in ("true", "1", "yes")
    PUBLIC_IMAGE_BASE_URL: str = os.getenv("PUBLIC_IMAGE_BASE_URL", "http://localhost:8080/images")
    IMGPROXY_KEY: str = os.getenv("IMGPROXY_KEY", "")
    IMGPROXY_SALT: str = os.getenv("IMGPROXY_SALT", "")
    IMAGE_BACKFILL_PER_CYCLE: int = int(os.getenv("IMAGE_BACKFILL_PER_CYCLE", "25"))
    IMAGE_DOWNLOAD_CONCURRENCY: int = int(os.getenv("IMAGE_DOWNLOAD_CONCURRENCY", "6"))
    PRICE_SYNC_INTERVAL_DAYS: int = int(os.getenv("PRICE_SYNC_INTERVAL_DAYS", "7"))
    PRICE_SYNC_BATCH_SIZE: int = int(os.getenv("PRICE_SYNC_BATCH_SIZE", "75"))
    PRICE_CHANGE_THRESHOLD: float = float(os.getenv("PRICE_CHANGE_THRESHOLD", "0.025"))
    TOR_ENABLED: bool = os.getenv("TOR_ENABLED", "true").lower() in ("true", "1", "yes")
    TOR_SOCKS_PROXY: str = os.getenv("TOR_SOCKS_PROXY", "socks5://tor:9050")
    TOR_CONTROL_HOST: str = os.getenv("TOR_CONTROL_HOST", "tor")
    TOR_CONTROL_PORT: int = int(os.getenv("TOR_CONTROL_PORT", "9051"))
    TOR_CONTROL_PASSWORD: str = os.getenv("TOR_CONTROL_PASSWORD", "")
    TOR_ROTATE_BEFORE_CYCLE: bool = os.getenv("TOR_ROTATE_BEFORE_CYCLE", "true").lower() in ("true", "1", "yes")
    RULES_CHECK_INTERVAL_SECONDS: int = int(os.getenv("RULES_CHECK_INTERVAL_SECONDS", "21600"))
    RULINGS_PER_CYCLE: int = int(os.getenv("RULINGS_PER_CYCLE", "50"))
    WIZARDS_RULES_TEXT_URL: str | None = os.getenv("WIZARDS_RULES_TEXT_URL") or None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
