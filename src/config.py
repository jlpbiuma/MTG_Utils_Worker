import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/mtg_utils?schema=public"
    )
    UPDATE_INTERVAL_SECONDS: int = int(os.getenv("UPDATE_INTERVAL_SECONDS", "1800"))  # 30 minutes default
    RUN_ON_STARTUP: bool = os.getenv("RUN_ON_STARTUP", "true").lower() in ("true", "1", "yes")
    SETS_PER_CYCLE: int = int(os.getenv("SETS_PER_CYCLE", "2"))  # Number of sets to download per cycle
    RATE_LIMIT_DELAY_SECONDS: float = float(os.getenv("RATE_LIMIT_DELAY_SECONDS", "0.1"))  # 100ms polite pause
    DOWNLOAD_DIGITAL_SETS: bool = os.getenv("DOWNLOAD_DIGITAL_SETS", "false").lower() in ("true", "1", "yes")
    HTTP_PORT: int = int(os.getenv("HTTP_PORT", "8002"))
    HTTP_HOST: str = os.getenv("HTTP_HOST", "0.0.0.0")
    ENABLE_HTTP_SERVER: bool = os.getenv("ENABLE_HTTP_SERVER", "true").lower() in ("true", "1", "yes")
    SCRYFALL_API_BASE: str = os.getenv("SCRYFALL_API_BASE", "https://api.scryfall.com")
    USER_AGENT: str = os.getenv("USER_AGENT", "MTGUtilsSetWorker/1.0")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
