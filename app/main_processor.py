# signal_extraction_service/app/main_processor.py
import asyncio
from loguru import logger
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict
import json
import httpx # Ensure httpx is imported for sending feedback

from app.config import settings
from app.db_connectors.pg_source_connector import (
    fetch_nlp_results_for_signal_extraction,
    mark_nlp_results_as_signal_extracted
)
from app.db_connectors.ts_target_connector import (
    store_extracted_signals,
    create_signal_hypertable_if_not_exists
)
from app.models import ProcessedDocumentInput, ExtractedSignal, SentimentScoreEntry
from app.logic.aggregation_logic import create_aggregated_signal


def _parse_jsonb_field(jsonb_data: Optional[Any]) -> Optional[List[Dict[str, Any]]]:
    """Helper to parse JSONB data which might be a string or already a list/dict."""
    if jsonb_data is None:
        return None
    if isinstance(jsonb_data, str):
        try:
            return json.loads(jsonb_data)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse JSON string: {str(jsonb_data)[:100]}")
            return None
    elif isinstance(jsonb_data, list): # Already parsed by asyncpg
        return jsonb_data
    else:
        logger.warning(f"Unexpected type for JSONB field: {type(jsonb_data)}. Data: {str(jsonb_data)[:100]}")
        return None

def group_documents_for_aggregation(
    nlp_results_dicts: List[Dict[str, Any]]
) -> Dict[tuple, List[ProcessedDocumentInput]]:
    """
    Groups NLP results by a primary topic identifier and a defined time window.
    """
    grouped_docs = defaultdict(list)
    window_seconds = settings.TIME_AGGREGATION_WINDOW_HOURS * 3600

    for result_dict in nlp_results_dicts:
        raw_mongo_id = result_dict.get("raw_mongo_id")
        pg_nlp_result_id = result_dict.get("id") # Primary key of the document_nlp_outputs row

        topic_id_to_aggregate_by: Any = -1 # Default to outlier
        topic_name_for_aggregation: str = "Untopiced"
        max_probability_found: float = -1.0

        assigned_topics_data = _parse_jsonb_field(result_dict.get("assigned_topics"))
        
        if assigned_topics_data and isinstance(assigned_topics_data, list):
            if not assigned_topics_data:
                logger.trace(f"No topics in assigned_topics list for raw_mongo_id {raw_mongo_id}, using outlier.")
            else:
                for topic_info_dict in assigned_topics_data:
                    if isinstance(topic_info_dict, dict):
                        current_prob = topic_info_dict.get("probability")
                        if current_prob is not None and isinstance(current_prob, (float, int)):
                            if current_prob > max_probability_found:
                                max_probability_found = current_prob
                                topic_id_to_aggregate_by = topic_info_dict.get("id", -1)
                                topic_name_for_aggregation = topic_info_dict.get("name", f"Topic {topic_id_to_aggregate_by}")
                        elif max_probability_found == -1.0: # No probabilities yet, take the first valid topic
                            topic_id_to_aggregate_by = topic_info_dict.get("id", -1)
                            topic_name_for_aggregation = topic_info_dict.get("name", f"Topic {topic_id_to_aggregate_by}")
                
                if max_probability_found == -1.0 and assigned_topics_data: # Still no prob, but had topics
                    first_topic_info = assigned_topics_data[0]
                    if isinstance(first_topic_info, dict):
                        topic_id_to_aggregate_by = first_topic_info.get("id", -1)
                        topic_name_for_aggregation = first_topic_info.get("name", f"Topic {topic_id_to_aggregate_by}")
                    logger.trace(f"No valid probabilities in assigned_topics for {raw_mongo_id}. Using first topic ID: {topic_id_to_aggregate_by}.")
        else:
            logger.trace(f"No/invalid assigned_topics for {raw_mongo_id}, using outlier topic ID -1.")

        original_ts: Optional[datetime] = result_dict.get("original_timestamp")
        if not original_ts:
            logger.warning(f"Doc (raw_mongo_id {raw_mongo_id}, pg_id {pg_nlp_result_id}) missing original_timestamp. Skipping for time-based aggregation.")
            continue
        
        if not isinstance(original_ts, datetime):
            try:
                # Handle ISO format string, ensure it becomes timezone-aware (UTC)
                parsed_ts_str = str(original_ts).replace("Z", "+00:00") if isinstance(original_ts, str) else str(original_ts)
                original_ts = datetime.fromisoformat(parsed_ts_str)
            except Exception as e:
                logger.warning(f"Could not parse original_timestamp '{original_ts}' for {raw_mongo_id}: {e}. Skipping.")
                continue
        
        original_ts = original_ts.astimezone(timezone.utc) if original_ts.tzinfo else original_ts.replace(tzinfo=timezone.utc)
        
        timestamp_seconds = original_ts.timestamp()
        timeframe_start_seconds = (timestamp_seconds // window_seconds) * window_seconds
        timeframe_start_dt = datetime.fromtimestamp(timeframe_start_seconds, tz=timezone.utc)

        try:
            overall_sentiment_data = _parse_jsonb_field(result_dict.get("overall_sentiment"))
            keywords_freq_data = _parse_jsonb_field(result_dict.get("extracted_keywords_frequency"))

            doc_input = ProcessedDocumentInput(
                raw_mongo_id=str(raw_mongo_id),
                original_timestamp=original_ts,
                overall_sentiment=[SentimentScoreEntry(**s) for s in overall_sentiment_data] if overall_sentiment_data else [],
                extracted_keywords_frequency=keywords_freq_data if keywords_freq_data else []
            )
            setattr(doc_input, '_pg_nlp_result_id', pg_nlp_result_id) # Attach PG ID for marking

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

    # --- 1. Aggregate Data for Feedback Calculation ---
    feedback_aggregation: Dict[tuple, Dict[str, Any]] = defaultdict(
        lambda: {"doc_count": 0, "sentiment_scores_sum": defaultdict(float), "sentiment_doc_count": defaultdict(int)}
    )
    total_docs_with_concept_id_in_batch = 0

    for nlp_result in nlp_results_fetched_dicts:
        concept_id = nlp_result.get("keyword_concept_id")
        keyword_lang = nlp_result.get("original_keyword_language")

        if not concept_id or not keyword_lang:
            logger.trace(f"Doc {nlp_result.get('raw_mongo_id')} missing concept_id/keyword_lang for feedback calc.")
            continue
        
        total_docs_with_concept_id_in_batch += 1
        group_key = (str(concept_id), str(keyword_lang))
        feedback_aggregation[group_key]["doc_count"] += 1
        
        overall_sentiment_data = _parse_jsonb_field(nlp_result.get("overall_sentiment"))
        if overall_sentiment_data:
            for sentiment_entry_dict in overall_sentiment_data:
                try:
                    sentiment_entry = SentimentScoreEntry(**sentiment_entry_dict)
                    if sentiment_entry.label in settings.HEALTHCARE_SENTIMENT_LABELS:
                        feedback_aggregation[group_key]["sentiment_scores_sum"][sentiment_entry.label] += sentiment_entry.score
                        feedback_aggregation[group_key]["sentiment_doc_count"][sentiment_entry.label] += 1
                except Exception as e:
                    logger.warning(f"Could not parse sentiment entry {sentiment_entry_dict} for doc {nlp_result.get('raw_mongo_id')} during feedback prep: {e}")

    # --- 2. Calculate Relevance Metric per Concept and Prepare Feedback Payloads ---
    feedback_payloads_to_send: List[Dict[str, Any]] = []
    sentiment_weights = {
        "Concerned": 1.5, "Anxious": 1.8, "Angry": 2.0, 
        "Confused": 1.2, "Satisfied": 0.5, "Grateful": 0.3, "Neutral": 0.1
    }
    max_possible_doc_sentiment_score = sum(sentiment_weights.values()) # Max score one doc can get

    for (concept_id, keyword_lang), data in feedback_aggregation.items():
        doc_count_for_concept = data["doc_count"]
        
        volume_score = 0.0
        if total_docs_with_concept_id_in_batch > 0:
            volume_score = (doc_count_for_concept / total_docs_with_concept_id_in_batch)
            volume_score = min(volume_score * 2.0, 1.0) # Amplify contribution of this concept, cap at 1

        concept_total_doc_intensity_score = 0.0
        docs_with_any_sentiment_for_concept = 0
        
        for nlp_result in nlp_results_fetched_dicts:
            if str(nlp_result.get("keyword_concept_id")) == concept_id and \
               str(nlp_result.get("original_keyword_language")) == keyword_lang:
                
                doc_intensity = 0.0
                has_sentiment_in_doc = False
                current_doc_overall_sentiment_data = _parse_jsonb_field(nlp_result.get("overall_sentiment"))
                if current_doc_overall_sentiment_data:
                    for sent_entry_dict in current_doc_overall_sentiment_data:
                        try:
                            sent_entry = SentimentScoreEntry(**sent_entry_dict)
                            doc_intensity += sent_entry.score * sentiment_weights.get(sent_entry.label, 0.0)
                            has_sentiment_in_doc = True
                        except: continue 
                if has_sentiment_in_doc:
                    concept_total_doc_intensity_score += doc_intensity
                    docs_with_any_sentiment_for_concept +=1
        
        sentiment_score_component = 0.0
        if docs_with_any_sentiment_for_concept > 0 and max_possible_doc_sentiment_score > 0:
            avg_doc_intensity = concept_total_doc_intensity_score / docs_with_any_sentiment_for_concept
            sentiment_score_component = min(max(avg_doc_intensity / max_possible_doc_sentiment_score, 0.0), 1.0)

        relevance_metric = min(max((0.4 * volume_score) + (0.6 * sentiment_score_component), 0.0), 1.0)
        
        feedback_payloads_to_send.append({
            "concept_id": concept_id,
            "language": keyword_lang,
            "relevance_metric": relevance_metric,
            "source": settings.SERVICE_NAME # Use service name from config
        })
        logger.debug(f"Feedback for Concept {concept_id} (Lang: {keyword_lang}): Relevance={relevance_metric:.3f} (Vol: {volume_score:.2f}, Sent: {sentiment_score_component:.2f}) from {doc_count_for_concept} docs.")

    # --- 3. Send Feedback to Keyword Manager ---
    if feedback_payloads_to_send and settings.KEYWORD_MANAGER_URL:
        logger.info(f"Sending {len(feedback_payloads_to_send)} feedback payloads to Keyword Manager at {settings.KEYWORD_MANAGER_URL}.")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                for payload_dict in feedback_payloads_to_send: # Renamed payload to payload_dict
                    try:
                        # Ensure URL is correctly formed
                        url = f"{str(settings.KEYWORD_MANAGER_URL).rstrip('/')}/feedback"
                        response = await client.post(url, json=payload_dict)
                        response.raise_for_status() # Raise exception for 4xx/5xx status codes
                        logger.info(f"Feedback sent for concept {payload_dict['concept_id']}, KM status: {response.status_code}")
                    except httpx.HTTPStatusError as e_http:
                        logger.error(f"HTTP error sending feedback for {payload_dict['concept_id']}: {e_http.response.status_code} - {e_http.response.text}")
                    except httpx.RequestError as e_req: # Covers network errors, DNS failures, etc.
                        logger.error(f"Request error sending feedback for {payload_dict['concept_id']}: {e_req}")
                    except Exception as e_gen:
                        logger.error(f"Unexpected error sending feedback for {payload_dict['concept_id']}: {e_gen}", exc_info=True)
        except ImportError:
            logger.error("httpx library not found. Cannot send KM feedback. Please add 'httpx' to requirements.txt.")
        except Exception as e_client_setup:
            logger.error(f"Error setting up HTTP client for KM feedback: {e_client_setup}", exc_info=True)
    elif feedback_payloads_to_send:
         logger.warning("KEYWORD_MANAGER_URL not configured. Skipping sending feedback.")
    else:
        logger.info("No feedback payloads to send to Keyword Manager in this cycle.")


    # --- 4. Proceed with Signal Aggregation ---
    grouped_by_topic_and_time = group_documents_for_aggregation(nlp_results_fetched_dicts)
    
    processed_source_pg_ids_for_this_job: List[int] = [] # To track all docs that go into successful signals
    ids_to_mark_failed_extraction_or_grouping: List[int] = [] # Docs that failed aggregation or grouping

    if not grouped_by_topic_and_time:
        logger.info("No valid groups formed for signal aggregation from fetched NLP results.")
        # Mark all fetched as 'failed_grouping' if they couldn't be grouped
        for res_dict in nlp_results_fetched_dicts:
            pg_id = res_dict.get("id")
            if pg_id is not None:
                ids_to_mark_failed_extraction_or_grouping.append(pg_id)
    else:
        extracted_signals_batch: List[ExtractedSignal] = []
        for (topic_id, topic_name, timeframe_start_dt), documents_in_group in grouped_by_topic_and_time.items():
            timeframe_end_dt = timeframe_start_dt + timedelta(hours=settings.TIME_AGGREGATION_WINDOW_HOURS)
            logger.debug(f"Aggregating for Topic ID: {topic_id}, Name: {topic_name}, Window: {timeframe_start_dt} - {timeframe_end_dt} with {len(documents_in_group)} docs.")
            try:
                signal = create_aggregated_signal(
                    topic_id=topic_id, topic_name=str(topic_name),
                    documents_for_topic=documents_in_group,
                    timeframe_start=timeframe_start_dt, timeframe_end=timeframe_end_dt
                )
                extracted_signals_batch.append(signal)
                for doc_input in documents_in_group:
                    pg_id = getattr(doc_input, '_pg_nlp_result_id', None)
                    if pg_id is not None:
                        processed_source_pg_ids_for_this_job.append(pg_id)
            except Exception as agg_err:
                logger.error(f"Error during signal aggregation for topic {topic_id} ({topic_name}): {agg_err}", exc_info=True)
                for doc_input in documents_in_group: # Mark docs in the failed group
                    pg_id = getattr(doc_input, '_pg_nlp_result_id', None)
                    if pg_id is not None:
                       ids_to_mark_failed_extraction_or_grouping.append(pg_id)
        
        if extracted_signals_batch:
            target_table_name = f"{settings.TARGET_SIGNALS_TABLE_PREFIX}_topic_hourly" # Example
            stored_count = await store_extracted_signals(extracted_signals_batch, target_table_name)
            logger.info(f"Attempted to store {len(extracted_signals_batch)} signals in '{target_table_name}', {stored_count} DB ops reported.")
            
            unique_processed_ids = list(set(processed_source_pg_ids_for_this_job))
            if stored_count == len(extracted_signals_batch) and unique_processed_ids:
                await mark_nlp_results_as_signal_extracted(unique_processed_ids, status='completed')
            elif unique_processed_ids:
                logger.warning("Signal storage count mismatch or some signals failed. Marking relevant source docs as 'pending_sig_store_verification'.")
                await mark_nlp_results_as_signal_extracted(unique_processed_ids, status='pending_sig_store_verification')
        else:
            logger.info("No signals were extracted in this run (after grouping/aggregation attempts).")
            # If all groups failed aggregation, their doc IDs are in ids_to_mark_failed_extraction_or_grouping


    # Mark documents that failed grouping or aggregation if any
    if ids_to_mark_failed_extraction_or_grouping:
         # Ensure we don't try to mark docs already successfully processed and marked 'completed'
         ids_actually_failed = list(set(ids_to_mark_failed_extraction_or_grouping) - set(processed_source_pg_ids_for_this_job))
         if ids_actually_failed:
            await mark_nlp_results_as_signal_extracted(ids_actually_failed, status='failed_extraction') # Or a more specific status

    job_end_time = datetime.now(timezone.utc)
    logger.info(f"--- Signal Extraction job finished. Duration: {(job_end_time - job_start_time).total_seconds():.2f}s. ---")