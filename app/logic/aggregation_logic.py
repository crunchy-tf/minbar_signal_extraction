# signal_extraction_service/app/logic/aggregation_logic.py
from typing import List, Dict, Any, Optional
from collections import Counter, defaultdict
from datetime import datetime
import numpy as np # Though not strictly needed for the current raw freq/avg score
from loguru import logger

from app.models import ProcessedDocumentInput, AggregatedSentiment, AggregatedKeyword, ExtractedSignal
from app.config import HEALTHCARE_SENTIMENT_LABELS, TOP_N_AGGREGATED_KEYWORDS

def aggregate_sentiment_scores(documents: List[ProcessedDocumentInput]) -> AggregatedSentiment:
    if not documents:
        return AggregatedSentiment(
            average_scores={label: 0.0 for label in HEALTHCARE_SENTIMENT_LABELS},
            dominant_sentiment_label="Neutral", # Default if no docs
            dominant_sentiment_score=0.0
        )

    # Initialize sums and counts for each sentiment label
    label_score_sums: Dict[str, float] = {label: 0.0 for label in HEALTHCARE_SENTIMENT_LABELS}
    # Number of documents that contributed to each label's score sum (not strictly needed if we average over total valid docs)
    # label_doc_counts: Dict[str, int] = {label: 0 for label in HEALTHCARE_SENTIMENT_LABELS}
    
    valid_docs_with_sentiment = 0

    for doc in documents:
        if doc.overall_sentiment_scores: # Check if the document has sentiment scores
            has_scores_for_this_doc = False
            for score_entry in doc.overall_sentiment_scores:
                label = score_entry.get("label")
                score = score_entry.get("score")
                if label in HEALTHCARE_SENTIMENT_LABELS and isinstance(score, (float, int)):
                    label_score_sums[label] += score
                    # label_doc_counts[label] += 1 # Count if a doc mentions a label
                    has_scores_for_this_doc = True
            if has_scores_for_this_doc:
                valid_docs_with_sentiment += 1
                
    average_scores: Dict[str, float] = {}
    if valid_docs_with_sentiment > 0:
        for label in HEALTHCARE_SENTIMENT_LABELS:
            average_scores[label] = label_score_sums[label] / valid_docs_with_sentiment
    else: # No documents had any valid sentiment scores
        average_scores = {label: 0.0 for label in HEALTHCARE_SENTIMENT_LABELS}

    dominant_label: Optional[str] = None
    max_avg_score: float = -1.0 # Scores are typically 0-1

    # Determine dominant sentiment (excluding Neutral if other sentiments have higher scores)
    non_neutral_max_score = -1.0
    dominant_non_neutral_label = None

    for label, avg_score in average_scores.items():
        if label != "Neutral" and avg_score > non_neutral_max_score:
            non_neutral_max_score = avg_score
            dominant_non_neutral_label = label
        if avg_score > max_avg_score: # Overall max including neutral
             max_avg_score = avg_score
             # dominant_label = label # This would make neutral dominant if it's highest

    if dominant_non_neutral_label and non_neutral_max_score > average_scores.get("Neutral", 0.0):
        dominant_label = dominant_non_neutral_label
        max_avg_score = non_neutral_max_score
    elif average_scores: # If non_neutral_max_score was not higher, or no non-neutral scores
        # Fallback to highest score overall, which could be Neutral
        dominant_label = max(average_scores, key=average_scores.get)
        max_avg_score = average_scores[dominant_label]
    else: # Should not happen if average_scores is populated
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
        # Track keywords already seen in this document to count document frequency correctly
        keywords_in_current_doc: set[str] = set()
        if doc.extracted_keywords_frequency:
            for kw_entry in doc.extracted_keywords_frequency:
                keyword = kw_entry.get("keyword")
                frequency = kw_entry.get("frequency", 0) # Default to 0 if missing
                
                if keyword and isinstance(keyword, str) and isinstance(frequency, int) and frequency > 0:
                    keyword = keyword.lower().strip() # Normalize keyword
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
                document_frequency=keyword_document_occurrence.get(keyword, 0) # Should always be found if total_freq > 0
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
    
    # Timestamp for the signal point - typically the end of the aggregation window
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