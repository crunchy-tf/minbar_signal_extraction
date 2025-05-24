# signal_extraction_service/app/db_connectors/pg_source_connector.py
import asyncpg
from typing import List, Dict, Any, Optional
from loguru import logger
from datetime import datetime, timezone

from app.config import settings

_source_pool: Optional[asyncpg.Pool] = None
_source_table_checked_for_status_field = False


async def connect_db():
    global _source_pool, _source_table_checked_for_status_field
    if _source_pool and not getattr(_source_pool, '_closed', True):
        logger.debug("Source PostgreSQL connection pool already established.")
        if not _source_table_checked_for_status_field: await ensure_status_field_in_source_table()
        return

    logger.info(f"Connecting to Source PostgreSQL database: {settings.source_postgres_dsn_asyncpg}")
    try:
        _source_pool = await asyncpg.create_pool(
            dsn=settings.source_postgres_dsn_asyncpg,
            min_size=1,
            max_size=5
        )
        await ensure_status_field_in_source_table()
        logger.success("Source PostgreSQL connection pool established and table checked.")
    except Exception as e:
        logger.critical(f"Failed to connect to Source PostgreSQL: {e}", exc_info=True)
        _source_pool = None
        _source_table_checked_for_status_field = False
        raise ConnectionError("Could not connect to Source PostgreSQL") from e

async def close_db():
    global _source_pool, _source_table_checked_for_status_field
    if _source_pool:
        logger.info("Closing Source PostgreSQL connection pool.")
        await _source_pool.close()
        _source_pool = None
        _source_table_checked_for_status_field = False
        logger.success("Source PostgreSQL connection pool closed.")

async def get_source_pool() -> asyncpg.Pool:
    if _source_pool is None or getattr(_source_pool, '_closed', True):
        await connect_db() # connect_db handles initialization and ensures pool is not None on success
    if _source_pool is None: # Check again after attempt
        raise ConnectionError("Source PostgreSQL pool unavailable after re-initialization attempt.")
    return _source_pool

async def ensure_status_field_in_source_table():
    global _source_table_checked_for_status_field
    if _source_table_checked_for_status_field: return

    pool = await get_source_pool()
    field_name = settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE
    table_name = settings.SOURCE_NLP_RESULTS_TABLE

    logger.info(f"Verifying status field '{field_name}' exists in source table '{table_name}' in DB '{settings.SOURCE_POSTGRES_DB}'...")
    try:
        async with pool.acquire() as conn:
            exists = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
                );
            """, table_name, field_name)
            if not exists:
                logger.error(f"CRITICAL: Status field '{field_name}' NOT FOUND in source table '{table_name}'. "
                             f"The NLP Analyzer service should create this field.")
                _source_table_checked_for_status_field = False
                raise LookupError(f"Required status field '{field_name}' missing in source table '{table_name}'.")
            else:
                logger.debug(f"Verified: Status field '{field_name}' exists in source table '{table_name}'.")
        _source_table_checked_for_status_field = True
    except Exception as e:
        logger.error(f"Error verifying status field in source table '{table_name}': {e}", exc_info=True)
        _source_table_checked_for_status_field = False
        raise


async def fetch_nlp_results_for_signal_extraction(limit: int) -> List[Dict[str, Any]]:
    pool = await get_source_pool()
    query = f"""
        SELECT id, raw_mongo_id, source, original_timestamp, retrieved_by_keyword,
               keyword_concept_id, original_keyword_language, -- <<< CRITICAL: Ensure these are selected
               detected_language, overall_sentiment, assigned_topics, extracted_keywords_frequency
               -- Add other fields if ProcessedDocumentInput requires them directly
        FROM {settings.SOURCE_NLP_RESULTS_TABLE}
        WHERE {settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE} = 'pending'
           OR {settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE} = 'failed_extraction' -- Optional: retry specific failed ones
        ORDER BY original_timestamp ASC
        LIMIT $1;
    """
    try:
        records = await pool.fetch(query, limit)
        logger.info(f"Fetched {len(records)} NLP result documents for signal extraction from '{settings.SOURCE_NLP_RESULTS_TABLE}'.")
        return [dict(record) for record in records] # Convert asyncpg.Record to dict
    except Exception as e:
        logger.error(f"Error fetching NLP results from '{settings.SOURCE_NLP_RESULTS_TABLE}': {e}", exc_info=True)
        return []

async def mark_nlp_results_as_signal_extracted(
    nlp_result_pg_ids: List[int],
    status: str = 'completed'
) -> int:
    if not settings.MARK_AS_SIGNAL_EXTRACTED_IN_SOURCE_DB or not nlp_result_pg_ids:
        if not nlp_result_pg_ids: logger.debug("No NLP result IDs provided to mark for signal extraction status.")
        return 0

    pool = await get_source_pool()
    status_field = settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE
    timestamp_field = f"{status_field}_timestamp" # Assumes NLP Analyzer creates this timestamp field too

    # Verify the timestamp field also exists, or handle its absence
    # For simplicity, assuming NLP Analyzer adds <status_field_name>_timestamp
    # If not, the SET clause for timestamp_field might fail if the column doesn't exist.
    # A robust solution would be another ensure_column_exists check, or make it conditional.

    update_query = f"""
        UPDATE {settings.SOURCE_NLP_RESULTS_TABLE}
        SET
            {status_field} = $1,
            {timestamp_field} = $2
        WHERE id = ANY($3::int[]);
    """
    try:
        valid_ids = [int(id_val) for id_val in nlp_result_pg_ids if id_val is not None]
        if not valid_ids:
            logger.warning("No valid integer document IDs provided for marking signal extraction status.")
            return 0

        result_status_str = await pool.execute(update_query, status, datetime.now(timezone.utc), valid_ids)
        updated_count = 0
        if result_status_str and result_status_str.startswith("UPDATE "):
            try:
                updated_count = int(result_status_str.split(" ")[1])
            except (IndexError, ValueError): # Handle cases where parsing count fails
                logger.warning(f"Could not parse update count from status: {result_status_str}. Assuming all attempts if no error.")
                updated_count = len(valid_ids)


        logger.info(f"Marked {updated_count} NLP results as signal extraction status '{status}' in '{settings.SOURCE_NLP_RESULTS_TABLE}'.")
        return updated_count
    except asyncpg.exceptions.UndefinedColumnError as col_err:
        logger.error(f"Column error marking NLP results: {col_err}. Ensure '{status_field}' and '{timestamp_field}' exist in '{settings.SOURCE_NLP_RESULTS_TABLE}'.")
        return 0 # Or re-raise if critical
    except Exception as e:
        logger.error(f"Error marking NLP results in '{settings.SOURCE_NLP_RESULTS_TABLE}': {e}", exc_info=True)
        return 0