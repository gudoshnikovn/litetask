"""
Example of an asynchronous LiteTask producer.

This script demonstrates how to enqueue a task asynchronously using `adelay()`.
The producer does not block while the task is being processed by a worker.
"""

import asyncio
import logging

from tasks import shared_task

# Configure basic logging for the example
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    """
    Main asynchronous function to enqueue a task.
    """
    logger.info("--- ASYNCHRONOUS PRODUCER ---")

    # Use await .adelay() to ensure the task is enqueued before proceeding.
    task_id = await shared_task.adelay("AsyncUser", 10)

    logger.info("Task sent asynchronously! ID: %s", task_id)

    # The producer can perform other asynchronous operations while the worker processes the task.
    # await asyncio.sleep(1)
    logger.info("Asynchronous code is not blocked...")


if __name__ == "__main__":
    asyncio.run(main())
