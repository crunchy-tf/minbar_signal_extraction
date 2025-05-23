# signal_extraction_service/app/db_connectors/ts_target_connector.py
import asyncpg
from typing import List, Dict, Any, Optional
from loguru import logger
import json
from datetime import datetime

from app.config import settings
from app.models import ExtractedSignal # Assuming ExtractedSignal output model

_target_pool: Optional[asyncpg.Pool] = None
_hypertables_checked: Dict[str, bool] = {} # To track checked/created hypertables

async def connect_db(): # Renamed
    global _target_pool
    if _target_pool and not getattr(_target_pool, '_closed', True):
        logger.debug("Target TimescaleDB connection pool already established.")
        return

    logger.info(f"Connecting to Target TimescaleDB: {settings.target_timescaledb_dsn_asyncpg}")
    try:
        _target_pool = await asyncpg.create_pool(
            dsn=settings.target_timescaledb_dsn_asyncpg,
            min_size=2,
            max_size=10
        )
        # Table creation will be more dynamic based on signal type/granularity
        logger.success("Target TimescaleDB connection pool established.")
    except Exception as e:
        logger.critical(f"Failed to connect to Target TimescaleDB: {e}", exc_info=True)
        _target_pool = None
        raise ConnectionError("Could not connect to Target TimescaleDB") from e

async def close_db(): # Renamed
    global _target_pool, _hypertables_checked
    if _target_pool:
        logger.info("Closing Target TimescaleDB connection pool.")
        await _target_pool.close()
        _target_pool = None
        _hypertables_checked = {} # Reset checked tables
        logger.success("Target TimescaleDB connection pool closed.")

async def get_target_pool() -> asyncpg.Pool:
    if _target_pool is None or getattr(_target_pool, '_closed', True):
        await connect_db()
    if _target_pool is None:
        raise ConnectionError("Target TimescaleDB pool unavailable.")
    return _target_pool

async def create_signal_hypertable_if_not_exists(table_name: str, time_column_name: str = "signal_timestamp"):
    # This function needs to be robust. It creates ONE specific table structure.
    # You might need different structures for different signal types.
    global _hypertables_checked
    if _hypertables_checked.get(table_name):
        return

    pool = await get_target_pool()
    logger.info(f"Checking/Creating TimescaleDB hypertable '{table_name}' with time column '{time_column_name}'...")
    
    # Define a generic schema for aggregated topic signals. Adjust as needed.
    # This schema should match how you flatten/prepare ExtractedSignal for storage.
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        signal_timestamp TIMESTAMPTZ NOT NULL,
        topic_id TEXT NOT NULL, -- Using TEXT for flexibility with 'Any' from Pydantic
        topic_name TEXT,
        document_count INTEGER,
        -- Storing complex structures as JSONB
        aggregated_sentiment_avg_scores JSONB, -- Stores the average_scores dict
        dominant_sentiment_label TEXT,
        dominant_sentiment_score FLOAT,
        top_keywords JSONB, -- Stores List[AggregatedKeyword]
        timeframe_start TIMESTAMPTZ,
        timeframe_end TIMESTAMPTZ,
        -- Composite primary key is good for TimescaleDB hypertables
        PRIMARY KEY (topic_id, signal_timestamp) 
    );
    """
    # Attempt to convert to hypertable
    # Error will be raised if table doesn't exist or already a hypertable, or TimescaleDB extension not enabled
    create_hypertable_sql = f"SELECT create_hypertable('{table_name}', '{time_column_name}', if_not_exists => TRUE, migrate_data => TRUE);"

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(create_table_sql)
                logger.info(f"Standard table '{table_name}' ensured.")
                try:
                    await conn.execute(create_hypertable_sql)
                    logger.success(f"Hypertable '{table_name}' ensured/created successfully.")
                except asyncpg.PostgresError as ts_err: # Catch errors specific to hypertable creation
                    if "already a hypertable" in str(ts_err).lower():
                        logger.info(f"Table '{table_name}' is already a hypertable.")
                    elif "extension \"timescaledb\" does not exist" in str(ts_err).lower():
                         logger.error(f"TimescaleDB extension not enabled in database '{settings.TARGET_TIMESCALEDB_DB}'. Cannot create hypertable '{table_name}'.")
                         raise # Re-raise, this is a critical setup issue
                    else:
                        logger.error(f"Error converting '{table_name}' to hypertable: {ts_err}", exc_info=True)
                        # Decide if this is fatal or if a regular table is acceptable for now
                        # raise
        _hypertables_checked[table_name] = True
    except Exception as e:
        logger.error(f"Error during setup of table/hypertable '{table_name}': {e}", exc_info=True)
        _hypertables_checked[table_name] = False # Allow retry
        raise

async def store_extracted_signals(signals: List[ExtractedSignal], table_name: str) -> int:
    if not signals:
        return 0
    pool = await get_target_pool()
    
    # Ensure the target table is ready
    try:
        await create_signal_hypertable_if_not_exists(table_name)
    except Exception as table_setup_e:
        logger.error(f"Cannot store signals: Target table '{table_name}' setup failed: {table_setup_e}")
        return 0

    data_to_insert = []
    for signal in signals:
        # Map ExtractedSignal to the columns of your TimescaleDB table
        data_to_insert.append((
            signal.signal_timestamp,
            str(signal.topic_id), # Ensure topic_id is string for TEXT column
            signal.topic_name,
            signal.document_count,
            json.dumps(signal.aggregated_sentiment.average_scores) if signal.aggregated_sentiment else None,
            signal.aggregated_sentiment.dominant_sentiment_label if signal.aggregated_sentiment else None,
            signal.aggregated_sentiment.dominant_sentiment_score if signal.aggregated_sentiment else None,
            json.dumps([kw.model_dump() for kw in signal.top_aggregated_keywords]) if signal.top_aggregated_keywords else None,
            signal.timeframe_start,
            signal.timeframe_end
        ))
    
    # Column names must match the order in data_to_insert tuples
    cols = ("signal_timestamp", "topic_id", "topic_name", "document_count", 
            "aggregated_sentiment_avg_scores", "dominant_sentiment_label", 
            "dominant_sentiment_score", "top_keywords", 
            "timeframe_start", "timeframe_end")

    insert_query = f"""
        INSERT INTO {table_name} ({", ".join(cols)})
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (topic_id, signal_timestamp) DO UPDATE SET
            topic_name = EXCLUDED.topic_name,
            document_count = EXCLUDED.document_count,
            aggregated_sentiment_avg_scores = EXCLUDED.aggregated_sentiment_avg_scores,
            dominant_sentiment_label = EXCLUDED.dominant_sentiment_label,
            dominant_sentiment_score = EXCLUDED.dominant_sentiment_score,
            top_keywords = EXCLUDED.top_keywords,
            timeframe_start = EXCLUDED.timeframe_start,
            timeframe_end = EXCLUDED.timeframe_end;
    """
    try:
        async with pool.acquire() as conn:
            status_command = await conn.executemany(insert_query, data_to_insert)
            # Assume all attempted if no error, parse status_command for actual count if needed
            inserted_count = len(data_to_insert) 
            logger.info(f"Stored/Updated {inserted_count} signals in TimescaleDB table '{table_name}'. Status: {status_command}")
            return inserted_count
    except Exception as e:
        logger.error(f"Error storing signals in TimescaleDB table '{table_name}': {e}", exc_info=True)
        return 0