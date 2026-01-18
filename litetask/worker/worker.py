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
        Starts the worker's main loop.

        The worker will continuously attempt to dequeue and process jobs
        until it is stopped. It handles broker connection and error logging.
        """
        self.running = True
        await self.broker.connect()
        logger.info("LiteTask Worker started.")

        while self.running:
            try:
                raw_job_data = await self.broker.dequeue()
                if raw_job_data:
                    task = asyncio.create_task(self._process_job(raw_job_data))
                    task.add_done_callback(self._handle_task_result)
                elif hasattr(self.broker, "wait_for_job"):
                    # If the broker has a specific wait mechanism (e.g., SQLite's event)
                    wait_method = self.broker.wait_for_job
                    if asyncio.iscoroutinefunction(wait_method):
                        await wait_method()
                else:
                    # Generic sleep for other brokers
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info("Worker loop cancelled.")
                break
            except Exception as e:
                msg = "Worker loop encountered an unexpected error. Retrying in 1 second."
                logger.exception(msg)
                raise WorkerError(msg) from e
            finally:
                if not self.running:
                    logger.info("LiteTask Worker stopping.")
                    await self.broker.close()
        logger.info("LiteTask Worker stopped.")

    def _handle_task_result(self, task: asyncio.Task[Any]) -> None:
        """
        Handles the result or exceptions of a completed task.

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
        Processes a single raw job fetched from the broker.

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

        try:
            job = Job.from_row(raw_job_data)
            payload = json.loads(raw_job_data["payload"])
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})
        except (json.JSONDecodeError, KeyError) as e:
            msg = f"Failed to parse job data for job ID {job_id}: {e}"
            logger.error(msg)
            await self.broker.update_status(job_id, "failed", error=msg)
            return

        logger.info("Processing job %s: task '%s'", job_id, task_name)

        # Start heartbeat coroutine
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(job_id))

        try:
            func = self.task_registry.get(task_name)
            if not func:
                msg = f"Task '{task_name}' not found in registry."
                raise TaskNotFoundError(msg)

            # Execute the task
            result = await self.executor.run(func, *args, **kwargs)

            # Update status on success
            await self.broker.update_status(job_id, "success", result=result)
            logger.info("Job %s: task '%s' completed successfully.", job_id, task_name)

        except Exception as e:
            # Update status on failure
            error_message = str(e)
            logger.error("Job %s: task '%s' failed with error: %s", job_id, task_name, error_message)
            await self.broker.update_status(job_id, "failed", error=error_message)

        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task  # Await to ensure cancellation is processed
            except asyncio.CancelledError:
                pass  # Expected cancellation

    async def _heartbeat_loop(self, job_id: str) -> None:
        """
        Periodically sends heartbeats for a running job.

        This coroutine runs in the background while a task is being processed
        to extend its lease in the broker, preventing it from being picked up
        by another worker.

        Parameters
        ----------
        job_id : str
            The ID of the job for which to send heartbeats.
        """
        try:
            while True:
                await asyncio.sleep(10)  # Send heartbeat every 10 seconds
                await self.broker.heartbeat(job_id)
                logger.debug("Heartbeat sent for job %s.", job_id)
        except asyncio.CancelledError:
            logger.debug("Heartbeat loop for job %s cancelled.", job_id)
        except Exception:
            logger.exception("Error in heartbeat loop for job %s.", job_id)
