"""
In-memory broker implementation for LiteTask.

This module provides a `MemoryBroker` that stores tasks in an asyncio.Queue
and a dictionary, suitable for testing or single-process applications
where persistence is not required.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, final

from litetask.broker.base import Broker
from litetask.core.models import Job

logger = logging.getLogger(__name__)


@final
class MemoryBroker(Broker):
    """
    An in-memory broker implementation for LiteTask.

    Stores jobs in an `asyncio.Queue` for pending tasks and a dictionary
    for all job states. This broker is non-persistent and suitable for
    testing or local, single-process execution.
    """

    def __init__(self) -> None:
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.jobs: dict[str, Job] = {}
        self._stopped: bool = False

    async def connect(self) -> None:
        """
        Initialize the memory broker.

        Sets the internal stopped flag to False.
        """
        self._stopped = False
        logger.debug("MemoryBroker connected.")

    async def close(self) -> None:
        """
        Close the memory broker.

        Sets the internal stopped flag to True, preventing further dequeues.
        """
        self._stopped = True
        logger.debug("MemoryBroker closed.")

    async def enqueue(self, job: Job) -> str:
        """
        Asynchronously enqueue a job into the memory broker.

        The job is stored in an internal dictionary and its ID is added to the queue.

        Parameters
        ----------
        job : Job
            The job object to enqueue.

        Returns
        -------
        str
            The ID of the enqueued job.
        """
        self.jobs[job.id] = job
        await self.queue.put(job.id)
        logger.debug("Job %s enqueued asynchronously.", job.id)
        return job.id

    def enqueue_sync(self, job: Job) -> str:
        """
        Synchronously enqueue a job into the memory broker.

        The job is stored in an internal dictionary and its ID is added to the queue
        using `put_nowait`. If the queue is full, the job is still stored but
        might not be immediately available for dequeueing.

        Parameters
        ----------
        job : Job
            The job object to enqueue.

        Returns
        -------
        str
            The ID of the enqueued job.
        """
        self.jobs[job.id] = job
        try:
            self.queue.put_nowait(job.id)
            logger.debug("Job %s enqueued synchronously.", job.id)
        except asyncio.QueueFull:
            logger.warning("MemoryBroker queue is full, job %s stored but not immediately available.", job.id)
        return job.id

    async def dequeue(self) -> dict[str, Any] | None:
        """
        Asynchronously dequeue a job from the memory broker.

        Waits for a job ID from the internal queue for a short timeout.
        If a job is found, its payload is emulated as a JSON string.

        Returns
        -------
        dict[str, Any] | None
            A dictionary containing the job's 'id', 'task_name', 'payload',
            'lease_expires_at', and 'heartbeat_at', or None if no job is available
            within the timeout or if the broker is stopped.
        """
        if self._stopped:
            return None
        try:
            # Wait for a job for 1 second to avoid blocking indefinitely
            job_id = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            job = self.jobs.get(job_id)
            if not job:
                logger.warning("Dequeued job ID %s not found in jobs dictionary.", job_id)
                return None

            # Emulate payload structure as in SQLite
            payload = json.dumps({"args": job.args, "kwargs": job.kwargs})

            logger.debug("Job %s dequeued.", job_id)
            return {
                "id": job.id,
                "task_name": job.task_name,
                "payload": payload,
                "lease_expires_at": job.lease_expires_at,
                "heartbeat_at": job.heartbeat_at,
            }
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            return None
        except Exception:
            logger.exception("Error during memory broker dequeue.")
            return None

    async def update_status(self, job_id: str, status: str, result: Any = None, error: str | None = None) -> None:
        """
        Asynchronously update the status of a job in the memory broker.

        Parameters
        ----------
        job_id : str
            The ID of the job to update.
        status : str
            The new status of the job.
        result : Any, optional
            The result of the job execution. Defaults to None.
        error : str | None, optional
            An error message if the job failed. Defaults to None.
        """
        if job_id in self.jobs:
            self.jobs[job_id].status = status
            self.jobs[job_id].result = result
            self.jobs[job_id].error = error
            logger.debug("Job %s status updated to %s.", job_id, status)
        else:
            logger.warning("Attempted to update status for non-existent job ID %s.", job_id)

    async def heartbeat(self, job_id: str) -> None:
        """
        Send a heartbeat for a running job in the memory broker.

        In a memory broker, this method primarily serves to fulfill the `Broker`
        interface, as lease management is not typically required for in-memory tasks.
        It updates the `heartbeat_at` timestamp.

        Parameters
        ----------
        job_id : str
            The ID of the job for which to send a heartbeat.
        """
        if job_id in self.jobs:
            self.jobs[job_id].heartbeat_at = datetime.now(timezone.utc)
            logger.debug("Heartbeat received for job %s.", job_id)
        else:
            logger.warning("Heartbeat received for non-existent job ID %s.", job_id)
