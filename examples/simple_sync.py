"""
Example of a synchronous LiteTask producer.

This script demonstrates how to enqueue a task synchronously using `delay()`.
The producer enqueues the task and immediately continues its own execution
without waiting for the task to complete.
"""

import logging
import time

from tasks import shared_task

# Configure basic logging for the example
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """
    Main synchronous function to enqueue a task.
    """
    logger.info("--- SYNCHRONOUS PRODUCER ---")

    # Call the task like a regular function using .delay()
    # This will instantly write the task to SQLite and proceed.
    task_id = shared_task.delay("SyncUser", 3)

    logger.info("Task sent! ID: %s", task_id)
    logger.info("I did not wait for execution and can do other things in Django...")

    for i in range(3):
        logger.info("Synchronous code is working... %d", i)
        time.sleep(0.5)


if __name__ == "__main__":
    main()
