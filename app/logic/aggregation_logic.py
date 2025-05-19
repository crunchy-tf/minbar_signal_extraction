# signal_extraction_service/app/logic/aggregation_logic.py
from typing import List, Dict, Any, Optional
from collections import Counter, defaultdict
from datetime import datetime
# import numpy as np # Not strictly needed here currently
from loguru import logger

# Assuming ProcessedDocumentInput, AggregatedSentiment, etc. are in app.models
# from app.models import ProcessedDocumentInput, AggregatedSentiment, AggregatedKeyword, ExtractedSignal 
# For clarity, let's assume the Pydantic models are accessible.
# If you run this file standalone for testing, you'd need to define or import them.
from app.models import ProcessedDocumentInput, AggregatedSentiment, AggregatedKeyword, ExtractedSignal, SentimentScoreEntry # Added SentimentScoreEntry
from app.config import HEALTHCARE_SENTIMENT_LABELS, TOP_N_AGGREGATED_KEYWORDS

def aggregate_sentiment_scores(documents: List[ProcessedDocumentInput]) -> AggregatedSentiment:
    if not documents:
        return AggregatedSentiment(
            average_scores={label: 0.0 for label in HEALTHCARE_SENTIMENT_LABELS},
            dominant_sentiment_label="Neutral",
            dominant_sentiment_score=0.0
        )

    label_score_sums: Dict[str, float] = {label: 0.0 for label in HEALTHCARE_SENTIMENT_LABELS}
    valid_docs_with_sentiment = 0

    for doc in documents:
        if doc.overall_sentiment_scores:
            has_valid_scores_for_this_doc = False
            for score_entry in doc.overall_sentiment_scores: # score_entry is a SentimentScoreEntry object
                # --- CORRECTED ACCESS ---
                label = score_entry.label 
                score = score_entry.score
                # --- END CORRECTION ---
                
                if label in HEALTHCARE_SENTIMENT_LABELS and isinstance(score, (float, int)):
                    label_score_sums[label] += score
                    has_valid_scores_for_this_doc = True
            if has_valid_scores_for_this_doc:
                valid_docs_with_sentiment += 1
                
    average_scores: Dict[str, float] = {}
    if valid_docs_with_sentiment > 0:
        for label in HEALTHCARE_SENTIMENT_LABELS:
            average_scores[label] = label_score_sums[label] / valid_docs_with_sentiment
    else:
        average_scores = {label: 0.0 for label in HEALTHCARE_SENTIMENT_LABELS}

    dominant_label: Optional[str] = None
    max_avg_score: float = -1.0 

    non_neutral_max_score = -1.0
    dominant_non_neutral_label = None

    for label, avg_score in average_scores.items():
        if label != "Neutral" and avg_score > non_neutral_max_score:
            non_neutral_max_score = avg_score
            dominant_non_neutral_label = label
    
    # Determine dominant based on highest score, preferring non-neutral if it's truly highest
    if dominant_non_neutral_label and non_neutral_max_score >= average_scores.get("Neutral", -1.0): # Use >= to ensure preference
        dominant_label = dominant_non_neutral_label
        max_avg_score = non_neutral_max_score
    elif average_scores: # If non_neutral wasn't dominant or no non_neutral scores
        dominant_label = max(average_scores, key=average_scores.get)
        max_avg_score = average_scores.get(dominant_label, 0.0)
    else:
        dominant_label = "Neutral"
        max_avg_score = 0.0

    return AggregatedSentiment(
        average_scores=average_scores,
        dominant_sentiment_label=dominant_label,
        dominant_sentiment_score=max_avg_score
    )

def aggregate_keywords_frequency(documents: List[ProcessedDocumentInput]) -> List[AggregatedKeyword]:
    if not documents:
        return []

    overall_keyword_total_frequency: Counter = Counter()
    keyword_document_occurrence: Dict[str, int] = defaultdict(int)

    for doc in documents:
        keywords_in_current_doc: set[str] = set()
        if doc.extracted_keywords_frequency:
            for kw_entry in doc.extracted_keywords_frequency: # kw_entry is Dict[str, Any]
                # --- CORRECTED ACCESS for consistency, though .get() works for dicts ---
                keyword = kw_entry.get("keyword") 
                frequency = kw_entry.get("frequency", 0)
                # --- END CORRECTION ---
                
                if keyword and isinstance(keyword, str) and isinstance(frequency, int) and frequency > 0:
                    keyword = keyword.lower().strip()
                    if not keyword: continue

                    overall_keyword_total_frequency[keyword] += frequency
                    if keyword not in keywords_in_current_doc:
                        keyword_document_occurrence[keyword] += 1
                        keywords_in_current_doc.add(keyword)
    
    aggregated_keywords_list: List[AggregatedKeyword] = []
    for keyword, total_freq in overall_keyword_total_frequency.most_common(TOP_N_AGGREGATED_KEYWORDS):
        aggregated_keywords_list.append(
            AggregatedKeyword(
                keyword=keyword,
                total_frequency=total_freq,
                document_frequency=keyword_document_occurrence.get(keyword, 0)
            )
        )
    return aggregated_keywords_list

def create_aggregated_signal(
    topic_id: Any,
    topic_name: str,
    documents_for_topic: List[ProcessedDocumentInput],
    timeframe_start: datetime,
    timeframe_end: datetime
) -> ExtractedSignal:
    
    logger.info(f"Creating aggregated signal for topic '{topic_name}' (ID: {topic_id}) using {len(documents_for_topic)} documents from {timeframe_start} to {timeframe_end}.")

    doc_count = len(documents_for_topic)
    
    agg_sentiment = aggregate_sentiment_scores(documents_for_topic)
    agg_keywords = aggregate_keywords_frequency(documents_for_topic)
    
    signal_ts = timeframe_end 

    return ExtractedSignal(
        signal_timestamp=signal_ts,
        topic_id=topic_id,
        topic_name=topic_name,
        document_count=doc_count,
        aggregated_sentiment=agg_sentiment,
        top_aggregated_keywords=agg_keywords,
        timeframe_start=timeframe_start,
        timeframe_end=timeframe_end
    )