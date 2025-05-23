# signal_extraction_service/app/main.py
from fastapi import FastAPI, HTTPException, status # Added status
from typing import List
from loguru import logger
import sys
import asyncio # Added
from contextlib import asynccontextmanager # Added

# Use Pydantic settings
from app.config import settings # MODIFIED: Use Pydantic settings
from app.models import SignalExtractionRequest, ExtractedSignal
from app.logic.aggregation_logic import create_aggregated_signal

# Import new DB and Scheduler modules
from app.db_connectors.pg_source_connector import connect_db as connect_pg_source, close_db as close_pg_source
from app.db_connectors.ts_target_connector import connect_db as connect_ts_target, close_db as close_ts_target, store_extracted_signals, create_signal_hypertable_if_not_exists
from app.services.scheduler_service import start_scheduler, stop_scheduler

logger.remove()
logger.add(sys.stderr, level=settings.LOG_LEVEL.upper()) # Use settings for log level

@asynccontextmanager
async def lifespan(app: FastAPI): # Use new lifespan manager
    logger.info(f"{settings.SERVICE_NAME} starting up...")
    
    db_pg_source_ok = False
    db_ts_target_ok = False

    # 1. Connect to Databases
    try:
        await connect_pg_source() # Connect to source PostgreSQL (NLP results)
        logger.info("Connected to Source PostgreSQL DB (NLP Results).")
        db_pg_source_ok = True
    except Exception as e:
        logger.critical(f"Source PostgreSQL connection failed during startup: {e}", exc_info=True)

    try:
        await connect_ts_target() # Connect to target TimescaleDB (Signals)
        logger.info("Connected to Target TimescaleDB (Signals).")
        db_ts_target_ok = True
    except Exception as e:
        logger.critical(f"Target TimescaleDB connection failed during startup: {e}", exc_info=True)

    if not (db_pg_source_ok and db_ts_target_ok):
        # Attempt cleanup before raising
        if db_pg_source_ok: await close_pg_source()
        if db_ts_target_ok: await close_ts_target()
        raise RuntimeError(f"One or more Database connections failed. PG Source OK: {db_pg_source_ok}, TS Target OK: {db_ts_target_ok}")

    # 2. Start APScheduler
    try:
        await start_scheduler()
    except Exception as e:
        logger.error(f"APScheduler failed to start for Signal Extraction: {e}", exc_info=True)
        # If scheduler is critical for this polling service, it should halt startup
        await close_pg_source()
        await close_ts_target()
        raise RuntimeError(f"APScheduler failed to start: {e}") from e

    logger.info(f"{settings.SERVICE_NAME} startup complete.")
    
    yield # Application runs
    
    logger.info(f"{settings.SERVICE_NAME} shutting down...")
    await stop_scheduler()
    await close_ts_target()
    await close_pg_source()
    logger.info("APScheduler and DB connections shut down.")
    logger.info(f"{settings.SERVICE_NAME} shutdown complete.")


app = FastAPI(
    title=settings.SERVICE_NAME, 
    description="Microservice for aggregating NLP insights into unified topic signals over timeframes.",
    version="1.0.0",
    lifespan=lifespan 
)

# The /extract-signal endpoint is primarily for testing the aggregation logic
# The main workflow will be via the scheduled job.
@app.post("/extract-signal", response_model=ExtractedSignal, summary="Extract Aggregated Topic Signal (Manual/Test)")
async def post_extract_signal_endpoint(request: SignalExtractionRequest):
    logger.info(f"API /extract-signal request for topic: '{request.topic_name}' (ID: {request.topic_id}) for timeframe {request.timeframe_start} to {request.timeframe_end}.")
    
    if not request.documents:
        logger.warning(f"No documents provided for topic: {request.topic_name}. Cannot extract signal.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No documents provided for signal extraction.")

    try:
        # The core aggregation logic
        signal = create_aggregated_signal(
            topic_id=request.topic_id,
            topic_name=request.topic_name,
            documents_for_topic=request.documents,
            timeframe_start=request.timeframe_start,
            timeframe_end=request.timeframe_end
        )
        
        if signal:
            target_table_api = f"{settings.TARGET_SIGNALS_TABLE_PREFIX}_topic_manual_api" 
            try:
                # Ensure table exists before attempting to store - create_signal_hypertable_if_not_exists is idempotent
                await create_signal_hypertable_if_not_exists(target_table_api) # Make sure this func is available or part of store
                await store_extracted_signals([signal], target_table_api)
                logger.info(f"API generated signal for topic {request.topic_name} stored in {target_table_api}")
            except Exception as db_e:
                logger.error(f"Failed to store API generated signal for {request.topic_name} in DB: {db_e}", exc_info=True)
                # Don't fail the API response if storage fails, but log it.
        
        logger.success(f"Successfully extracted signal via API for topic: {request.topic_name}")
        return signal
    except HTTPException as http_exc:
        raise http_exc 
    except Exception as e:
        logger.error(f"Error during API signal extraction for topic {request.topic_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error during signal extraction.")

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting {settings.SERVICE_NAME} for local development on port 8002...")
    service_port = 8002 # Matching Dockerfile EXPOSE
    # Optionally make this configurable via settings for local runs if 8002 is taken
    # try: service_port = settings.SERVICE_PORT 
    # except AttributeError: pass
    uvicorn.run("app.main:app", host="0.0.0.0", port=service_port, log_level=settings.LOG_LEVEL.lower(), reload=True)