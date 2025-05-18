# signal_extraction_service/app/config.py
from typing import List

# Configuration for Signal Extraction
TOP_N_AGGREGATED_KEYWORDS: int = 10 # How many top keywords to include in the extracted signal
LOG_LEVEL: str = "INFO"

# Define your sentiment labels here, consistent with NLP Analyzer
# This is important for iterating and ensuring all labels are present in output if desired
HEALTHCARE_SENTIMENT_LABELS: List[str] = [
    "Satisfied", "Grateful", "Concerned", "Anxious", "Confused", "Angry", "Neutral"
]