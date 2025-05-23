# signal_extraction_service/app/main_processor.py
import asyncio
from loguru import logger
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict
import json # For deserializing JSONB from PG if necessary

from app.config import settings # Use Pydantic settings
from app.db_connectors.pg_source_connector import (
    fetch_nlp_results_for_signal_extraction,
    mark_nlp_results_as_signal_extracted
)
from app.db_connectors.ts_target_connector import (
    store_extracted_signals,
    create_signal_hypertable_if_not_exists # Important for ensuring table exists
)
from app.models import ProcessedDocumentInput, SignalExtractionRequest, ExtractedSignal, SentimentScoreEntry
from app.logic.aggregation_logic import create_aggregated_signal
# Optional: If fetching topic names from Keyword Manager
# from app.services.keyword_manager_client import KeywordManagerClient # (You'd need to create this client)


def _parse_jsonb_field(jsonb_data: Optional[Any]) -> Optional[List[Dict[str, Any]]]:
    """Helper to parse JSONB data which might be a string or already a list/dict."""
    if isinstance(jsonb_data, str):
        try:
            return json.loads(jsonb_data)
        except json.JSONDecodeError:
            return None
    elif isinstance(jsonb_data, list): # Already parsed by asyncpg
        return jsonb_data
    return None

def group_documents_for_aggregation(
    nlp_results_dicts: List[Dict[str, Any]]
) -> Dict[tuple, List[ProcessedDocumentInput]]:
    """
    Groups NLP results by a topic identifier and a defined time window.
    The topic identifier will primarily be the first assigned BERTopic ID.
    """
    grouped_docs = defaultdict(list)
    window_seconds = settings.TIME_AGGREGATION_WINDOW_HOURS * 3600

    for result_dict in nlp_results_dicts:
        raw_mongo_id = result_dict.get("raw_mongo_id")
        pg_nlp_result_id = result_dict.get("id") # Primary key of the document_nlp_outputs row

        # 1. Determine Topic ID and Name for Aggregation:
        #    Using the first assigned BERTopic from `assigned_topics` (JSONB field)
        topic_id_to_aggregate_by: Any = None
        topic_name_for_aggregation: str = "Untopiced" # Default if no topic

        assigned_topics_data = _parse_jsonb_field(result_dict.get("assigned_topics"))
        
        if assigned_topics_data and isinstance(assigned_topics_data, list) and len(assigned_topics_data) > 0:
            first_topic_info = assigned_topics_data[0] # Expects list of dicts
            if isinstance(first_topic_info, dict):
                topic_id_to_aggregate_by = first_topic_info.get("id", -1) # Default to -1 (outlier) if ID missing
                topic_name_for_aggregation = first_topic_info.get("name", f"Topic {topic_id_to_aggregate_by}")
        else:
            topic_id_to_aggregate_by = -1 # Outlier topic if no topics assigned
            logger.trace(f"No topics assigned for raw_mongo_id {raw_mongo_id}, will use outlier topic ID -1.")


        # 2. Determine Time Window for Aggregation
        original_ts: Optional[datetime] = result_dict.get("original_timestamp")
        if not original_ts:
            logger.warning(f"Document (raw_mongo_id {raw_mongo_id}, pg_id {pg_nlp_result_id}) missing original_timestamp. Skipping for time-based aggregation.")
            continue
        
        if not isinstance(original_ts, datetime): # asyncpg might return datetime
            try:
                original_ts = datetime.fromisoformat(str(original_ts))
            except:
                logger.warning(f"Could not parse original_timestamp for {raw_mongo_id}. Skipping.")
                continue
        
        if original_ts.tzinfo is None:
            original_ts = original_ts.replace(tzinfo=timezone.utc)
        else:
            original_ts = original_ts.astimezone(timezone.utc) # Ensure UTC
        
        timestamp_seconds = original_ts.timestamp()
        timeframe_start_seconds = (timestamp_seconds // window_seconds) * window_seconds
        timeframe_start_dt = datetime.fromtimestamp(timeframe_start_seconds, tz=timezone.utc)

        # 3. Construct ProcessedDocumentInput for aggregation logic
        try:
            overall_sentiment_data = _parse_jsonb_field(result_dict.get("overall_sentiment"))
            keywords_freq_data = _parse_jsonb_field(result_dict.get("extracted_keywords_frequency"))

            doc_input = ProcessedDocumentInput(
                raw_mongo_id=str(raw_mongo_id),
                original_timestamp=original_ts,
                overall_sentiment=[SentimentScoreEntry(**s) for s in overall_sentiment_data] if overall_sentiment_data else [],
                extracted_keywords_frequency=keywords_freq_data if keywords_freq_data else []
                # Add pg_nlp_result_id if needed for later reference when marking
            )
            # Include original PG ID if useful for marking later, though not part of ProcessedDocumentInput
            doc_input._pg_nlp_result_id = pg_nlp_result_id


            group_key = (topic_id_to_aggregate_by, topic_name_for_aggregation, timeframe_start_dt)
            grouped_docs[group_key].append(doc_input)
        except Exception as pydantic_e:
            logger.error(f"Error creating ProcessedDocumentInput for (raw_mongo_id {raw_mongo_id}, pg_id {pg_nlp_result_id}): {pydantic_e}", exc_info=True)
            continue
            
    return grouped_docs


async def scheduled_signal_extraction_job():
    job_start_time = datetime.now(timezone.utc)
    logger.info(f"--- Starting scheduled Signal Extraction job (Window: {settings.TIME_AGGREGATION_WINDOW_HOURS}h) at {job_start_time.isoformat()} ---")
    
    nlp_results_fetched_dicts = await fetch_nlp_results_for_signal_extraction(settings.SIGNAL_AGGREGATION_BATCH_SIZE)
    
    if not nlp_results_fetched_dicts:
        logger.info("No new NLP results to process for signal extraction in this cycle.")
        logger.info(f"--- Signal Extraction job finished (no data) at {datetime.now(timezone.utc).isoformat()} ---")
        return

    logger.info(f"Fetched {len(nlp_results_fetched_dicts)} NLP result records for signal extraction.")

    # Group documents by topic and calculated time window
    # Key: (topic_identifier, topic_name, timeframe_start_dt)
    # Value: List[ProcessedDocumentInput]
    grouped_by_topic_and_time = group_documents_for_aggregation(nlp_results_fetched_dicts)
    
    if not grouped_by_topic_and_time:
        logger.info("No valid groups formed for signal aggregation from fetched NLP results.")
        # Mark all fetched as 'failed_grouping' so they are not picked up again endlessly
        source_ids_to_mark_failed_grouping = [res.get("id") for res in nlp_results_fetched_dicts if res.get("id") is not None]
        if source_ids_to_mark_failed_grouping:
            await mark_nlp_results_as_signal_extracted(source_ids_to_mark_failed_grouping, status='failed_grouping')
        logger.info(f"--- Signal Extraction job finished (no valid groups) at {datetime.now(timezone.utc).isoformat()} ---")
        return

    extracted_signals_batch: List[ExtractedSignal] = []
    processed_source_pg_ids_for_this_job: List[int] = []

    for (topic_id, topic_name, timeframe_start_dt), documents_in_group in grouped_by_topic_and_time.items():
        timeframe_end_dt = timeframe_start_dt + timedelta(hours=settings.TIME_AGGREGATION_WINDOW_HOURS)
        
        logger.debug(f"Aggregating for Topic ID: {topic_id}, Name: {topic_name}, Window: {timeframe_start_dt} - {timeframe_end_dt} with {len(documents_in_group)} docs.")

        try:
            signal = create_aggregated_signal(
                topic_id=topic_id, # This is likely the BERTopic ID
                topic_name=str(topic_name), 
                documents_for_topic=documents_in_group, # List[ProcessedDocumentInput]
                timeframe_start=timeframe_start_dt,
                timeframe_end=timeframe_end_dt 
            )
            extracted_signals_batch.append(signal)
            # Collect source PG IDs that contributed to this successful signal
            for doc_input in documents_in_group:
                if hasattr(doc_input, '_pg_nlp_result_id') and doc_input._pg_nlp_result_id is not None:
                    processed_source_pg_ids_for_this_job.append(doc_input._pg_nlp_result_id)
        except Exception as agg_err:
            logger.error(f"Error during signal aggregation for topic {topic_id} ({topic_name}): {agg_err}", exc_info=True)
            # Collect IDs of documents in the failed group to mark them as 'failed_extraction'
            ids_in_failed_group = [getattr(doc, '_pg_nlp_result_id', None) for doc in documents_in_group]
            ids_to_mark_failed = [pg_id for pg_id in ids_in_failed_group if pg_id is not None]
            if ids_to_mark_failed:
                await mark_nlp_results_as_signal_extracted(ids_to_mark_failed, status='failed_extraction')


    # Store extracted signals in TimescaleDB
    if extracted_signals_batch:
        # Define target table name based on aggregation granularity
        # Example: hourly aggregation
        target_table_name = f"{settings.TARGET_SIGNALS_TABLE_PREFIX}_topic_hourly"
        
        stored_count = await store_extracted_signals(extracted_signals_batch, target_table_name)
        logger.info(f"Attempted to store {len(extracted_signals_batch)} aggregated signals in '{target_table_name}', {stored_count} operations reported by DB.")
        
        if stored_count == len(extracted_signals_batch) and processed_source_pg_ids_for_this_job:
             # Remove duplicates before marking
            await mark_nlp_results_as_signal_extracted(list(set(processed_source_pg_ids_for_this_job)), status='completed')
        elif processed_source_pg_ids_for_this_job:
            logger.warning("Not all extracted signals may have been stored or count mismatch. Marking source NLP results as 'pending_sig_store_verification'.")
            await mark_nlp_results_as_signal_extracted(list(set(processed_source_pg_ids_for_this_job)), status='pending_sig_store_verification')
    else:
        logger.info("No signals were extracted in this run (after grouping and aggregation attempts).")
        # If all initial documents led to grouping/aggregation errors, they would have been marked 'failed_extraction' above.
        # If some were just filtered out by grouping logic (e.g. not enough data for a window), they remain 'pending'.

    job_end_time = datetime.now(timezone.utc)
    logger.info(f"--- Signal Extraction job finished. Duration: {(job_end_time - job_start_time).total_seconds():.2f}s. Generated {len(extracted_signals_batch)} signals. ---")