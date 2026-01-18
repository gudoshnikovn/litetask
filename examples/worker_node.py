"""
Starts a LiteTask worker node.

This script initializes and runs a LiteTask worker, which continuously
fetches and executes tasks from the configured broker.
"""

import asyncio
import logging

from examples.tasks import app
from litetask.core.exceptions import WorkerError

# Configure basic logging for the example
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    try:
        # The worker is always asynchronous internally to efficiently wait for tasks.
        asyncio.run(app.run_worker())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
    except WorkerError:
        logger.error("Worker stopped due to a critical error.")
    except Exception:
        logger.exception("Worker encountered an unexpected error and stopped.")
