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
        if not _source_table_checked_for_status_field:
            logger.info("Pool exists, but source table status field status unknown. Attempting check/creation.")
            try:
                await ensure_status_field_in_source_table()
            except Exception as table_err:
                logger.error(f"Failed to check/ensure fields in source table on existing pool: {table_err}")
        return

    logger.info(f"Connecting to Source PostgreSQL database: {settings.source_postgres_dsn_asyncpg}")
    try:
        _source_pool = await asyncpg.create_pool(
            dsn=settings.source_postgres_dsn_asyncpg,
            min_size=1,
            max_size=5
        )
        await ensure_status_field_in_source_table()
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
        await connect_db()
    if _source_pool is None:
        raise ConnectionError("Source PostgreSQL pool unavailable after re-initialization attempt.")
    return _source_pool

async def ensure_status_field_in_source_table():
    global _source_table_checked_for_status_field
    if _source_table_checked_for_status_field:
        logger.debug(f"Source table '{settings.SOURCE_NLP_RESULTS_TABLE}' fields already checked in this session.")
        return

    pool = await get_source_pool()
    status_field_to_check = settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE
    timestamp_field_to_check = "signal_extraction_timestamp"
    table_name = settings.SOURCE_NLP_RESULTS_TABLE

    logger.info(f"Ensuring status field '{status_field_to_check}' and timestamp field '{timestamp_field_to_check}' exist in source table '{table_name}'...")
    try:
        async with pool.acquire() as conn:
            status_field_exists = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
                );
            """, table_name, status_field_to_check)

            if not status_field_exists:
                logger.error(f"CRITICAL: Main status field '{status_field_to_check}' NOT FOUND in source table '{table_name}'.")
            else:
                logger.debug(f"Verified: Main status field '{status_field_to_check}' exists in source table '{table_name}'.")

            timestamp_field_exists = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
                );
            """, table_name, timestamp_field_to_check)

            if not timestamp_field_exists:
                logger.warning(f"Timestamp field '{timestamp_field_to_check}' NOT FOUND in source table '{table_name}'. Attempting to add.")
                try:
                    await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {timestamp_field_to_check} TIMESTAMPTZ;")
                    logger.success(f"Successfully added missing timestamp field '{timestamp_field_to_check}' to '{table_name}'.")
                except Exception as alter_e:
                    logger.error(f"Failed to add missing timestamp field '{timestamp_field_to_check}' to '{table_name}': {alter_e}.")
                    raise
            else:
                logger.debug(f"Verified: Timestamp field '{timestamp_field_to_check}' exists in source table '{table_name}'.")

        _source_table_checked_for_status_field = True
        logger.success(f"Source table '{table_name}' checks complete for necessary fields.")
    except Exception as e:
        logger.error(f"Error during field verification/addition in source table '{table_name}': {e}", exc_info=True)
        _source_table_checked_for_status_field = False
        raise

async def fetch_nlp_results_for_signal_extraction(limit: int) -> List[Dict[str, Any]]:
    pool = await get_source_pool()
    
    # --- MODIFIED QUERY WITH STRICTER FILTERS ---
    # For JSONB fields, 'IS NOT NULL' checks if the field itself is NULL.
    # To check for empty JSONB arrays like '[]' or empty objects '{}',
    # you can use jsonb_array_length or compare to '[]'::jsonb / '{}'::jsonb.
    # For simplicity and common usage, IS NOT NULL often suffices if empty arrays/objects are treated as "not empty".
    # If an empty array `[]` should be skipped, you'd use `jsonb_array_length(overall_sentiment) > 0`.
    # The prompt said "cant be empty", which for JSONB could mean not NULL and not an empty structure.
    # Let's assume "cant be empty" means the JSONB field itself is not NULL and, if it's an array, it's not an empty array '[]'.
    # PostgreSQL's NULL is different from an empty JSON array '[]' or empty JSON object '{}'.
    # `overall_sentiment IS NOT NULL` is the first check.
    # `jsonb_typeof(overall_sentiment) = 'array' AND jsonb_array_length(overall_sentiment) > 0` for non-empty array.

    query = f"""
        SELECT 
            id, raw_mongo_id, source, original_timestamp, retrieved_by_keyword,
            keyword_concept_id, original_keyword_language,
            detected_language, overall_sentiment, assigned_topics, 
            extracted_keywords_frequency, sentiment_on_extracted_keywords_summary 
            -- Select sentiment_on_extracted_keywords_summary as it's now required
        FROM {settings.SOURCE_NLP_RESULTS_TABLE}
        WHERE 
            ({settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE} = 'pending' OR
             {settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE} = 'failed_extraction')
        AND raw_mongo_id IS NOT NULL AND raw_mongo_id != ''
        AND source IS NOT NULL AND source != ''
        AND detected_language IN ('en', 'fr', 'ar') -- Ensures detected_language is one of these
        -- Conditions for JSONB fields to be non-NULL and non-empty arrays
        -- (jsonb_typeof is useful to distinguish between null, array, object, string, number, boolean)
        AND overall_sentiment IS NOT NULL 
            AND jsonb_typeof(overall_sentiment) = 'array' AND jsonb_array_length(overall_sentiment) > 0
        AND assigned_topics IS NOT NULL
            AND jsonb_typeof(assigned_topics) = 'array' AND jsonb_array_length(assigned_topics) > 0
        AND extracted_keywords_frequency IS NOT NULL
            AND jsonb_typeof(extracted_keywords_frequency) = 'array' AND jsonb_array_length(extracted_keywords_frequency) > 0
        AND sentiment_on_extracted_keywords_summary IS NOT NULL -- Added this condition
            AND jsonb_typeof(sentiment_on_extracted_keywords_summary) = 'array' AND jsonb_array_length(sentiment_on_extracted_keywords_summary) > 0
            
        ORDER BY original_timestamp ASC
        LIMIT $1;
    """
    # --- END MODIFIED QUERY ---

    try:
        records = await pool.fetch(query, limit)
        logger.info(f"Fetched {len(records)} NLP result documents meeting strict criteria from '{settings.SOURCE_NLP_RESULTS_TABLE}'.")
        return [dict(record) for record in records]
    except Exception as e:
        logger.error(f"Error fetching strictly filtered NLP results from '{settings.SOURCE_NLP_RESULTS_TABLE}': {e}", exc_info=True)
        return []

async def mark_nlp_results_as_signal_extracted(
    nlp_result_pg_ids: List[int],
    status: str = 'completed'
) -> int:
    if not settings.MARK_AS_SIGNAL_EXTRACTED_IN_SOURCE_DB or not nlp_result_pg_ids:
        if not nlp_result_pg_ids: logger.debug("No NLP result IDs provided to mark for signal extraction status.")
        return 0

    pool = await get_source_pool()
    status_field_to_update = settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE
    timestamp_field_to_update = "signal_extraction_timestamp" 

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
        logger.error(f"Column error marking NLP results: {col_err}. This is unexpected if schema checks passed. "
                     f"Attempted to update status field '{status_field_to_update}' and timestamp field '{timestamp_field_to_update}'.")
        return 0
    except Exception as e:
        logger.error(f"Error marking NLP results in '{settings.SOURCE_NLP_RESULTS_TABLE}': {e}", exc_info=True)
        return 0