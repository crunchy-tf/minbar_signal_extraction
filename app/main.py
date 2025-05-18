# signal_extraction_service/app/main.py
from fastapi import FastAPI, HTTPException
from typing import List
from loguru import logger
import sys

from app.models import SignalExtractionRequest, ExtractedSignal
from app.logic.aggregation_logic import create_aggregated_signal # Corrected import path
from app.config import LOG_LEVEL

# --- Logger Configuration ---
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL)


app = FastAPI(
    title="Minbar Signal Extraction Service",
    description="Microservice for aggregating NLP insights into unified topic signals over timeframes.",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    logger.info("Signal Extraction Service starting up...")
    # Any startup logic, e.g., connecting to a DB if needed for other tasks, can go here
    logger.info("Signal Extraction Service startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Signal Extraction Service shutting down...")
    # Any cleanup logic
    logger.info("Signal Extraction Service shutdown complete.")


@app.post("/extract-signal", response_model=ExtractedSignal, summary="Extract Aggregated Topic Signal")
async def post_extract_signal_endpoint(request: SignalExtractionRequest):
    """
    Aggregates insights from multiple documents for a given topic and timeframe
    to produce a unified signal.
    """
    logger.info(f"Received signal extraction request for topic: '{request.topic_name}' (ID: {request.topic_id}) for timeframe {request.timeframe_start} to {request.timeframe_end}.")
    
    if not request.documents:
        logger.warning(f"No documents provided for topic: {request.topic_name}. Cannot extract signal.")
        raise HTTPException(status_code=400, detail="No documents provided for signal extraction.")

    try:
        signal = create_aggregated_signal(
            topic_id=request.topic_id,
            topic_name=request.topic_name,
            documents_for_topic=request.documents,
            timeframe_start=request.timeframe_start,
            timeframe_end=request.timeframe_end
        )
        logger.success(f"Successfully extracted signal for topic: {request.topic_name}")
        return signal
    except Exception as e:
        logger.error(f"Error during signal extraction for topic {request.topic_name}: {e}", exc_info=True)
        # It's good to return a more generic error to the client for 500s
        raise HTTPException(status_code=500, detail="Internal server error during signal extraction.")

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Signal Extraction Service for local development on port 8002...")
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level=LOG_LEVEL.lower())