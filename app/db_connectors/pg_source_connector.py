# signal_extraction_service/app/db_connectors/pg_source_connector.py
import asyncpg
from typing import List, Dict, Any, Optional
from loguru import logger
from datetime import datetime, timezone

from app.config import settings

_source_pool: Optional[asyncpg.Pool] = None
_source_table_checked_for_status_field = False # This flag will now cover both status and its specific timestamp

async def connect_db():
    global _source_pool, _source_table_checked_for_status_field
    if _source_pool and not getattr(_source_pool, '_closed', True):
        logger.debug("Source PostgreSQL connection pool already established.")
        # If pool exists but table/fields weren't checked (e.g., previous attempt failed before check)
        if not _source_table_checked_for_status_field:
            logger.info("Pool exists, but source table status field status unknown. Attempting check/creation.")
            try:
                await ensure_status_field_in_source_table()
            except Exception as table_err:
                logger.error(f"Failed to check/ensure fields in source table on existing pool: {table_err}")
                # Potentially close pool if check is critical and fails, or let it be handled by health checks
        return

    logger.info(f"Connecting to Source PostgreSQL database: {settings.source_postgres_dsn_asyncpg}")
    try:
        _source_pool = await asyncpg.create_pool(
            dsn=settings.source_postgres_dsn_asyncpg,
            min_size=1,
            max_size=5
        )
        await ensure_status_field_in_source_table() # This will now check for both fields
        logger.success("Source PostgreSQL connection pool established and table fields checked.")
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
        logger.warning("Source PostgreSQL pool is None or closed. Attempting to (re)initialize...")
        await connect_db() # connect_db handles initialization and ensures pool is not None on success
    if _source_pool is None: # Check again after attempt
        raise ConnectionError("Source PostgreSQL pool unavailable after re-initialization attempt.")
    return _source_pool

async def ensure_status_field_in_source_table():
    """
    Ensures the status field (from settings) AND the specific 'signal_extraction_timestamp'
    field exist in the source table. The NLP Analyzer is primarily responsible for creating these,
    this function acts as a verification and a robust fallback if the timestamp field was missed.
    """
    global _source_table_checked_for_status_field
    if _source_table_checked_for_status_field:
        logger.debug(f"Source table '{settings.SOURCE_NLP_RESULTS_TABLE}' fields already checked in this session.")
        return

    pool = await get_source_pool()
    status_field_to_check = settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE # This is 'signal_extraction_status'
    # THIS IS THE KEY CHANGE: We explicitly define the timestamp field name we expect.
    timestamp_field_to_check = "signal_extraction_timestamp"
    table_name = settings.SOURCE_NLP_RESULTS_TABLE

    logger.info(f"Ensuring status field '{status_field_to_check}' and timestamp field '{timestamp_field_to_check}' exist in source table '{table_name}'...")
    try:
        async with pool.acquire() as conn:
            # 1. Check for the main status field (e.g., 'signal_extraction_status')
            status_field_exists = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
                );
            """, table_name, status_field_to_check)

            if not status_field_exists:
                logger.error(f"CRITICAL: Main status field '{status_field_to_check}' NOT FOUND in source table '{table_name}'. "
                             f"The NLP Analyzer service is responsible for creating this field. "
                             f"This service will likely fail to query or update status correctly.")
                # Optionally, you could attempt to add it here as a very defensive measure,
                # but it's better if the upstream service (NLP Analyzer) creates its own output schema correctly.
                # For now, we log an error and proceed, relying on NLP Analyzer to have created it.
                # If it's absolutely critical to add it here if missing:
                # await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {status_field_to_check} VARCHAR(50) DEFAULT 'pending';")
                # logger.warning(f"Attempted to add missing main status field '{status_field_to_check}'. Review NLP Analyzer.")
            else:
                logger.debug(f"Verified: Main status field '{status_field_to_check}' exists in source table '{table_name}'.")

            # 2. Check for the specific timestamp field (e.g., 'signal_extraction_timestamp')
            timestamp_field_exists = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
                );
            """, table_name, timestamp_field_to_check)

            if not timestamp_field_exists:
                logger.warning(f"Timestamp field '{timestamp_field_to_check}' NOT FOUND in source table '{table_name}'. "
                               f"The NLP Analyzer service should have created this. This service will attempt to add it now for robustness.")
                try:
                    await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {timestamp_field_to_check} TIMESTAMPTZ;")
                    logger.success(f"Successfully added missing timestamp field '{timestamp_field_to_check}' to '{table_name}'.")
                except Exception as alter_e:
                    logger.error(f"Failed to add missing timestamp field '{timestamp_field_to_check}' to '{table_name}': {alter_e}. "
                                 f"Marking documents might fail.")
                    # If adding fails, subsequent updates will fail.
                    # _source_table_checked_for_status_field should remain False or this function should raise.
                    raise # Re-raise to indicate a critical setup problem.
            else:
                logger.debug(f"Verified: Timestamp field '{timestamp_field_to_check}' exists in source table '{table_name}'.")

        _source_table_checked_for_status_field = True # Both checks passed (or add attempted)
        logger.success(f"Source table '{table_name}' checks complete for necessary fields.")
    except Exception as e:
        logger.error(f"Error during field verification/addition in source table '{table_name}': {e}", exc_info=True)
        _source_table_checked_for_status_field = False # Allow retry on next startup/call
        raise # Re-raise so startup process knows there's an issue.


async def fetch_nlp_results_for_signal_extraction(limit: int) -> List[Dict[str, Any]]:
    pool = await get_source_pool()
    # Query remains the same as it uses settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE
    query = f"""
        SELECT id, raw_mongo_id, source, original_timestamp, retrieved_by_keyword,
               keyword_concept_id, original_keyword_language, 
               detected_language, overall_sentiment, assigned_topics, extracted_keywords_frequency
        FROM {settings.SOURCE_NLP_RESULTS_TABLE}
        WHERE {settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE} = 'pending'
           OR {settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE} = 'failed_extraction'
        ORDER BY original_timestamp ASC
        LIMIT $1;
    """
    try:
        records = await pool.fetch(query, limit)
        logger.info(f"Fetched {len(records)} NLP result documents for signal extraction from '{settings.SOURCE_NLP_RESULTS_TABLE}'.")
        return [dict(record) for record in records]
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
    status_field_to_update = settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE # e.g., 'signal_extraction_status'
    
    # --- THIS IS THE KEY CHANGE ---
    # Use the exact column name that exists in the database, as confirmed by your `SELECT *` query.
    timestamp_field_to_update = "signal_extraction_timestamp" 
    # --- END KEY CHANGE ---

    logger.debug(f"Attempting to update status to '{status}' using status field '{status_field_to_update}' and timestamp field '{timestamp_field_to_update}'.")

    update_query = f"""
        UPDATE {settings.SOURCE_NLP_RESULTS_TABLE}
        SET
            {status_field_to_update} = $1,
            {timestamp_field_to_update} = $2
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
            except (IndexError, ValueError):
                logger.warning(f"Could not parse update count from status: {result_status_str}. Assuming all attempts if no error.")
                updated_count = len(valid_ids)

        logger.info(f"Marked {updated_count} NLP results as signal extraction status '{status}' in '{settings.SOURCE_NLP_RESULTS_TABLE}'.")
        return updated_count
    except asyncpg.exceptions.UndefinedColumnError as col_err:
        # This error should now be less likely if ensure_status_field_in_source_table runs correctly
        # and if the timestamp_field_to_update is correctly "signal_extraction_timestamp"
        logger.error(f"Column error marking NLP results: {col_err}. This is unexpected if schema checks passed. "
                     f"Attempted to update status field '{status_field_to_update}' and timestamp field '{timestamp_field_to_update}'.")
        return 0
    except Exception as e:
        logger.error(f"Error marking NLP results in '{settings.SOURCE_NLP_RESULTS_TABLE}': {e}", exc_info=True)
        return 0