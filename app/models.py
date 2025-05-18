# signal_extraction_service/app/models.py
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime

# --- Input Models ---

# +++ NEW MODEL FOR INDIVIDUAL SENTIMENT SCORE +++
class SentimentScoreEntry(BaseModel):
    label: str
    score: float
# ++++++++++++++++++++++++++++++++++++++++++++++++

class ProcessedDocumentInput(BaseModel):
    raw_mongo_id: str = Field(..., description="Unique identifier for the original document")
    original_timestamp: datetime = Field(..., description="Timestamp of the original document")
    # overall_sentiment_scores: List[Dict[str, float]] = Field( # <<< OLD DEFINITION
    overall_sentiment_scores: List[SentimentScoreEntry] = Field( # <<< NEW DEFINITION using SentimentScoreEntry
        ..., 
        alias="overall_sentiment",  
        description="List of sentiment scores, e.g., [{'label': 'Concerned', 'score': 0.7}]"
    )
    extracted_keywords_frequency: List[Dict[str, Any]] = Field( 
        # For extracted_keywords_frequency, Dict[str, Any] is fine as frequency should be int.
        # If you wanted to be stricter, you could make another model:
        # class KeywordFrequencyEntry(BaseModel):
        #     keyword: str
        #     frequency: int
        # extracted_keywords_frequency: List[KeywordFrequencyEntry]
        ..., 
        description="List of extracted keywords and their frequencies from the document, e.g., [{'keyword': 'fever', 'frequency': 2}]"
    )

class SignalExtractionRequest(BaseModel):
    topic_id: Any = Field(..., description="Identifier for the topic (can be int or string)")
    topic_name: str = Field(..., description="Human-readable name of the topic")
    documents: List[ProcessedDocumentInput] = Field(..., description="List of processed documents for this topic in the timeframe")
    timeframe_start: datetime = Field(..., description="Start of the aggregation timeframe")
    timeframe_end: datetime = Field(..., description="End of the aggregation timeframe")

# --- Output Models ---
# (These Output Models were already correctly defined and do not need changes for this fix)

class AggregatedKeyword(BaseModel):
    keyword: str
    total_frequency: int = Field(..., description="Sum of frequencies across all documents in the timeframe for this topic")
    document_frequency: int = Field(..., description="Number of documents in the timeframe for this topic that mentioned this keyword")

class AggregatedSentiment(BaseModel):
    average_scores: Dict[str, float] = Field(..., description="Average score for each sentiment label across documents")
    dominant_sentiment_label: Optional[str] = Field(None, description="The sentiment label with the highest average score (excluding Neutral if others are higher)")
    dominant_sentiment_score: Optional[float] = Field(None, description="The score of the dominant sentiment label")

class ExtractedSignal(BaseModel):
    signal_timestamp: datetime = Field(..., description="Timestamp representing this aggregated signal point (e.g., end of timeframe)")
    topic_id: Any
    topic_name: str
    
    document_count: int = Field(..., description="Number of documents aggregated to create this signal point")
    
    aggregated_sentiment: AggregatedSentiment
    top_aggregated_keywords: List[AggregatedKeyword]
    
    timeframe_start: datetime
    timeframe_end: datetime