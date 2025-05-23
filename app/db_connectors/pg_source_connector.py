# signal_extraction_service/app/db_connectors/pg_source_connector.py
import asyncpg
from typing import List, Dict, Any, Optional
from loguru import logger
from datetime import datetime, timezone

from app.config import settings

_source_pool: Optional[asyncpg.Pool] = None
_source_table_checked_for_status_field = False


async def connect_db(): # Renamed to avoid clash if target also uses 'connect_db'
    global _source_pool, _source_table_checked_for_status_field
    if _source_pool and not getattr(_source_pool, '_closed', True):
        logger.debug("Source PostgreSQL connection pool already established.")
        if not _source_table_checked_for_status_field: await ensure_status_field_in_source_table()
        return

    logger.info(f"Connecting to Source PostgreSQL database: {settings.source_postgres_dsn_asyncpg}")
    try:
        _source_pool = await asyncpg.create_pool(
            dsn=settings.source_postgres_dsn_asyncpg,
            min_size=1, # Can be smaller if not heavily used
            max_size=5
        )
        await ensure_status_field_in_source_table()
        logger.success("Source PostgreSQL connection pool established and table checked.")
    except Exception as e:
        logger.critical(f"Failed to connect to Source PostgreSQL: {e}", exc_info=True)
        _source_pool = None
        _source_table_checked_for_status_field = False
        raise ConnectionError("Could not connect to Source PostgreSQL") from e

async def close_db(): # Renamed
    global _source_pool, _source_table_checked_for_status_field
    if _source_pool:
        logger.info("Closing Source PostgreSQL connection pool.")
        await _source_pool.close()
        _source_pool = None
        _source_table_checked_for_status_field = False
        logger.success("Source PostgreSQL connection pool closed.")

async def get_source_pool() -> asyncpg.Pool:
    if _source_pool is None or getattr(_source_pool, '_closed', True):
        await connect_db()
    if _source_pool is None:
        raise ConnectionError("Source PostgreSQL pool unavailable.")
    return _source_pool

async def ensure_status_field_in_source_table():
    """
    Ensures the status field (e.g., 'signal_extraction_status') exists in the source table
    (e.g., 'document_nlp_outputs'). This service DOES NOT CREATE THIS FIELD;
    it assumes the nlp_analyzer_service (the producer of document_nlp_outputs)
    has already created it. This function is more of a verification.
    """
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
                             f"The NLP Analyzer service should create this field (e.g., 'signal_extraction_status' in 'document_nlp_outputs').")
                # This service should not ALTER the table of another service.
                # It should fail or log a persistent error if the contract isn't met.
                _source_table_checked_for_status_field = False # Allow re-check
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
    # Fetches rows from nlp_analyzer's output table (e.g., document_nlp_outputs)
    # Assumes nlp_analyzer_service created 'signal_extraction_status' with DEFAULT 'pending'
    query = f"""
        SELECT id, raw_mongo_id, source, original_timestamp, retrieved_by_keyword,
               detected_language, overall_sentiment, assigned_topics, extracted_keywords_frequency
               -- Add other fields needed from nlp_outputs table for ProcessedDocumentInput
        FROM {settings.SOURCE_NLP_RESULTS_TABLE}
        WHERE {settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE} = 'pending'
           OR {settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE} = 'failed_extraction' -- Optional: retry failed ones
        ORDER BY original_timestamp ASC -- Process older documents first
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
    nlp_result_pg_ids: List[int], # Primary keys from SOURCE_NLP_RESULTS_TABLE
    status: str = 'completed'
) -> int:
    if not settings.MARK_AS_SIGNAL_EXTRACTED_IN_SOURCE_DB or not nlp_result_pg_ids:
        if not nlp_result_pg_ids: logger.debug("No NLP result IDs provided to mark for signal extraction status.")
        return 0
        
    pool = await get_source_pool()
    status_field = settings.SIGNAL_EXTRACTION_STATUS_FIELD_IN_SOURCE
    timestamp_field = f"{status_field}_timestamp"

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
            except: updated_count = len(valid_ids) # Fallback

        logger.info(f"Marked {updated_count} NLP results as signal extraction status '{status}' in '{settings.SOURCE_NLP_RESULTS_TABLE}'.")
        return updated_count
    except Exception as e:
        logger.error(f"Error marking NLP results in '{settings.SOURCE_NLP_RESULTS_TABLE}': {e}", exc_info=True)
        return 0