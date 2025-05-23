# signal_extraction_service/app/services/scheduler_service.py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from app.config import settings
from app.main_processor import scheduled_signal_extraction_job # Import the job

scheduler = AsyncIOScheduler(timezone="UTC")
_scheduler_started = False

async def start_scheduler():
    global _scheduler_started
    if scheduler.running or _scheduler_started:
        logger.info("Signal Extraction APScheduler already running or start initiated.")
        return
    try:
        scheduler.add_job(
            scheduled_signal_extraction_job,
            trigger=IntervalTrigger(minutes=settings.SCHEDULER_INTERVAL_MINUTES),
            id="signal_extraction_job",
            name="Poll NLP Results and Aggregate Signals",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=300 
        )
        scheduler.start()
        _scheduler_started = True
        logger.success(f"Signal Extraction APScheduler started. Job scheduled every {settings.SCHEDULER_INTERVAL_MINUTES} minutes.")
    except Exception as e:
        logger.error(f"Failed to start Signal Extraction APScheduler: {e}", exc_info=True)
        _scheduler_started = False


async def stop_scheduler():
    global _scheduler_started
    if scheduler.running:
        logger.info("Stopping Signal Extraction APScheduler...")
        scheduler.shutdown(wait=False)
        _scheduler_started = False
        logger.success("Signal Extraction APScheduler stopped.")