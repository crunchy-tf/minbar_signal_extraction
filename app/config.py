# signal_extraction_service/app/config.py
from typing import List, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AnyHttpUrl # Ensure AnyHttpUrl is imported

class Settings(BaseSettings):
    SERVICE_NAME: str = "Minbar Signal Extraction Service"
    LOG_LEVEL: str = "INFO"

    TOP_N_AGGREGATED_KEYWORDS: int = 10
    HEALTHCARE_SENTIMENT_LABELS: List[str] = [
        "Satisfied", "Grateful", "Concerned", "Anxious", "Confused", "Angry", "Neutral"
    ]

    # --- Source DB (NLP Analyzer's Output) ---
    SOURCE_POSTGRES_USER: str
    SOURCE_POSTGRES_PASSWORD: str
    SOURCE_POSTGRES_HOST: str
    SOURCE_POSTGRES_PORT: int = Field(default=5432)
    SOURCE_POSTGRES_DB: str
    SOURCE_NLP_RESULTS_TABLE: str = Field(default="document_nlp_outputs")
    SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE: str = Field(default="signal_extraction_status")

    # --- Target DB (TimescaleDB for Aggregated Signals) ---
    TARGET_TIMESCALEDB_USER: str
    TARGET_TIMESCALEDB_PASSWORD: str
    TARGET_TIMESCALEDB_HOST: str
    TARGET_TIMESCALEDB_PORT: int = Field(default=5432)
    TARGET_TIMESCALEDB_DB: str
    TARGET_SIGNALS_TABLE_PREFIX: str = Field(default="agg_signals")

    # --- Service Logic & Scheduler ---
    SIGNAL_AGGREGATION_BATCH_SIZE: int = Field(default=200, gt=0)
    SCHEDULER_INTERVAL_MINUTES: int = Field(default=30, gt=0)
    MARK_AS_SIGNAL_EXTRACTED_IN_SOURCE_DB: bool = Field(default=True)
    TIME_AGGREGATION_WINDOW_HOURS: int = Field(default=1, gt=0)

    # --- Keyword Manager ---
    KEYWORD_MANAGER_URL: Optional[AnyHttpUrl] = Field(None, validation_alias='KEYWORD_MANAGER_URL_ENV') # Example if env var is different
    # If .env var is KEYWORD_MANAGER_URL, then just:
    # KEYWORD_MANAGER_URL: Optional[AnyHttpUrl] = None


    @property
    def source_postgres_dsn_asyncpg(self) -> str:
        return f"postgresql://{self.SOURCE_POSTGRES_USER}:{self.SOURCE_POSTGRES_PASSWORD}@{self.SOURCE_POSTGRES_HOST}:{self.SOURCE_POSTGRES_PORT}/{self.SOURCE_POSTGRES_DB}"

    @property
    def target_timescaledb_dsn_asyncpg(self) -> str:
        return f"postgresql://{self.TARGET_TIMESCALEDB_USER}:{self.TARGET_TIMESCALEDB_PASSWORD}@{self.TARGET_TIMESCALEDB_HOST}:{self.TARGET_TIMESCALEDB_PORT}/{self.TARGET_TIMESCALEDB_DB}"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()

# Globals only if necessary, prefer passing `settings` or importing `settings` directly
LOG_LEVEL = settings.LOG_LEVEL
TOP_N_AGGREGATED_KEYWORDS = settings.TOP_N_AGGREGATED_KEYWORDS
HEALTHCARE_SENTIMENT_LABELS = settings.HEALTHCARE_SENTIMENT_LABELS