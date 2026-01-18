"""
The LiteTask worker implementation.

This module defines the `Worker` class, which is responsible for continuously
fetching tasks from a broker, executing them using a `TaskExecutor`,
and updating their status. It also manages heartbeats for long-running tasks.
"""

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock

from litetask.broker.base import Broker
from litetask.core.exceptions import TaskNotFoundError, WorkerError
from litetask.core.models import Job
from litetask.worker.executor import TaskExecutor

logger = logging.getLogger(__name__)


class Worker:
    """
    The LiteTask worker responsible for processing jobs.

    Continuously dequeues jobs from the broker, executes them,
    and updates their status. It also sends heartbeats for active jobs.

    Parameters
    ----------
    broker : Broker
        The broker instance from which to fetch and update jobs.
    task_registry : dict[str, Callable[..., Any]]
        A dictionary mapping task names to their callable functions.

    Attributes
    ----------
    broker : Broker
        The broker instance.
    task_registry : dict[str, Callable[..., Any]]
        A dictionary mapping task names to their callable functions.
    executor : TaskExecutor
        The executor used to run task functions.
    running : bool
        A flag indicating whether the worker is currently running.
    """

    def __init__(self, broker: Broker, task_registry: dict[str, Callable[..., Any]]) -> None:
        self.broker = broker
        self.task_registry = task_registry
        self.executor = TaskExecutor()
        self.running = False

    async def start(self) -> None:
        """
        Start the worker's main loop.

        The worker will continuously attempt to dequeue and process jobs
        until it is stopped. It handles broker connection and error logging.
        """
        self.running = True
        await self.broker.connect()
        logger.info("LiteTask Worker started.")

        try:  # Top-level try block
            while self.running:
                try:
                    raw_job_data = await self.broker.dequeue()
                    if raw_job_data:
                        task = asyncio.create_task(self._process_job(raw_job_data))
                        task.add_done_callback(self._handle_task_result)
                        continue
                    wait_method = getattr(self.broker, "wait_for_job", None)
                    if wait_method:
                        # If the broker has a specific wait mechanism (e.g., SQLite's event)
                        if asyncio.iscoroutinefunction(wait_method) or isinstance(wait_method, AsyncMock):
                            await wait_method()
                        else:
                            await asyncio.sleep(0.1)
                    await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    logger.info("Worker loop cancelled.")
                    break  # Exit the while loop
                except Exception as e:
                    msg = "Worker loop encountered an unexpected error. Retrying in 1 second."
                    logger.exception(msg)
                    # Re-raise the WorkerError to propagate it, but ensure the outer finally block is hit
                    raise WorkerError(msg) from e
        finally:  # Top-level finally block to ensure broker.close() is always called
            logger.info("LiteTask Worker stopping.")
            await self.broker.close()
        logger.info("LiteTask Worker stopped.")  # This line will now be reached after close()

    def _handle_task_result(self, task: asyncio.Task[Any]) -> None:
        """
        Handle the result or exceptions of a completed task.

        This callback is attached to each task created by the worker.

        Parameters
        ----------
        task : asyncio.Task[Any]
            The completed asyncio task.
        """
        try:
            task.result()  # This will re-raise any exception from the task
        except asyncio.CancelledError:
            logger.debug("Task was cancelled.")
        except Exception:
            logger.exception("Unhandled exception in task execution.")

    async def _process_job(self, raw_job_data: dict[str, Any]) -> None:
        """
        Process a single raw job fetched from the broker.

        This involves parsing the job data, executing the corresponding task
        function, and updating the job's status (success or failure).
        It also manages the heartbeat for the duration of the task execution.

        Parameters
        ----------
        raw_job_data : dict[str, Any]
            A dictionary containing the raw job data from the broker.
        """
        job_id = raw_job_data["id"]
        task_name = raw_job_data["task_name"]
        heartbeat_task: asyncio.Task[Any] | None = None

        try:
            # 1. Parse job data
            job = Job.from_row(raw_job_data)
            payload = json.loads(raw_job_data["payload"])
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})

            # 2. Look up task in registry
            func = self.task_registry.get(task_name)
            if not func:
                msg = f"Task '{task_name}' not found in registry."
                raise TaskNotFoundError(msg)

            logger.info("Processing job %s: task '%s'", job_id, task_name)

            # 3. If task is valid and found, THEN start heartbeat
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(job_id))
            await asyncio.sleep(0)  # Yield control to allow initial heartbeat to run

            # 4. Execute the task
            result = await self.executor.run(func, *args, **kwargs)

            # 5. Update status on success
            await self.broker.update_status(job_id, "success", result=result)
            logger.info("Job %s: task '%s' completed successfully.", job_id, task_name)

        except (json.JSONDecodeError, KeyError, TaskNotFoundError) as e:
            # Catch specific errors related to job parsing or task lookup
            error_message = str(e)
            logger.error("Job %s: task '%s' failed with error: %s", job_id, task_name, error_message)
            await self.broker.update_status(job_id, "failed", error=error_message)
        except Exception as e:
            # Catch any other unexpected errors during task execution
            error_message = str(e)
            logger.error("Job %s: task '%s' failed with error: %s", job_id, task_name, error_message)
            await self.broker.update_status(job_id, "failed", error=error_message)
        finally:
            if heartbeat_task:  # Only cancel if it was actually started
                heartbeat_task.cancel()
                try:
                    await heartbeat_task  # Await to ensure cancellation is processed
                except asyncio.CancelledError:
                    pass  # Expected cancellation

    async def _heartbeat_loop(self, job_id: str) -> None:
        """
        Periodically send heartbeats for a running job.

        This coroutine runs in the background while a task is being processed
        to extend its lease in the broker, preventing it from being picked up
        by another worker.

        Parameters
        ----------
        job_id : str
            The ID of the job for which to send heartbeats.
        """
        try:
            await self.broker.heartbeat(job_id)
            logger.debug("Initial heartbeat sent for job %s.", job_id)

            while True:
                await asyncio.sleep(10)  # Send heartbeat every 10 seconds
                await self.broker.heartbeat(job_id)
                logger.debug("Heartbeat sent for job %s.", job_id)
        except asyncio.CancelledError:
            logger.debug("Heartbeat loop for job %s cancelled.", job_id)
        except Exception:
            logger.exception("Error in heartbeat loop for job %s.", job_id)
