"""
Defines example tasks for the LiteTask library.

This module sets up a LiteTask application instance and registers a sample
task function that can be enqueued and executed by a LiteTask worker.
"""

import logging
import time

from litetask.app import LiteTask

# Configure basic logging for the example
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize the LiteTask application with a SQLite broker
app = LiteTask("sqlite://litetask.db")


@app.task()
def shared_task(name: str, seconds: int) -> str:
    """
    A sample task that simulates work by sleeping for a given duration.

    Parameters
    ----------
    name : str
        The name of the user or entity for whom the task is being performed.
    seconds : int
        The number of seconds to sleep, simulating work.

    Returns
    -------
    str
        A result string indicating completion for the given name.
    """
    logger.info("[Worker] Started task for %s for %d seconds...", name, seconds)
    time.sleep(seconds)
    logger.info("[Worker] Task for %s completed!", name)
    return f"Result for {name}"
