"""
Base abstract class for LiteTask brokers.

This module defines the `Broker` abstract base class, which all concrete
broker implementations must inherit from. It specifies the core interface
for managing tasks (jobs) within the LiteTask system.
"""

from abc import ABC, abstractmethod
from typing import Any

from litetask.core.models import Job


class Broker(ABC):
    """
    Abstract base class for LiteTask brokers.

    Defines the essential methods for connecting, closing, enqueueing,
    dequeueing, updating job status, and sending heartbeats for tasks.
    """

    @abstractmethod
    async def connect(self) -> None:
        """
        Initialize the broker connection and resources.

        This method should handle tasks like creating necessary tables,
        establishing database connections, or setting up queues.
        """
        msg = "Broker.connect() must be implemented by subclasses."
        raise NotImplementedError(msg)

    @abstractmethod
    async def close(self) -> None:
        """
        Close the broker connection and releases resources.

        This method should ensure proper shutdown, such as closing database
        connections or stopping background threads.
        """
        msg = "Broker.close() must be implemented by subclasses."
        raise NotImplementedError(msg)

    @abstractmethod
    async def enqueue(self, job: Job) -> str:
        """
        Asynchronously enqueue a job into the broker.

        Parameters
        ----------
        job : Job
            The job object to enqueue.

        Returns
        -------
        str
            The ID of the enqueued job.
        """
        msg = "Broker.enqueue() must be implemented by subclasses."
        raise NotImplementedError(msg)

    @abstractmethod
    def enqueue_sync(self, job: Job) -> str:
        """
        Synchronously enqueue a job into the broker.

        Parameters
        ----------
        job : Job
            The job object to enqueue.

        Returns
        -------
        str
            The ID of the enqueued job.
        """
        msg = "Broker.enqueue_sync() must be implemented by subclasses."
        raise NotImplementedError(msg)

    @abstractmethod
    async def dequeue(self) -> dict[str, Any] | None:
        """
        Asynchronously dequeue a job from the broker.

        This method should retrieve the next available job for processing.

        Returns
        -------
        dict[str, Any] | None
            A dictionary representing the job's raw data, or None if no job is available.
            The dictionary typically includes 'id', 'task_name', 'payload',
            'lease_expires_at', and 'heartbeat_at'.
        """
        msg = "Broker.dequeue() must be implemented by subclasses."
        raise NotImplementedError(msg)

    @abstractmethod
    async def update_status(self, job_id: str, status: str, result: Any = None, error: str | None = None) -> None:
        """
        Asynchronously update the status of a job.

        Parameters
        ----------
        job_id : str
            The ID of the job to update.
        status : str
            The new status of the job (e.g., "success", "failed", "running").
        result : Any, optional
            The result of the job execution, if successful. Defaults to None.
        error : str | None, optional
            An error message if the job failed. Defaults to None.
        """
        msg = "Broker.update_status() must be implemented by subclasses."
        raise NotImplementedError(msg)

    @abstractmethod
    async def heartbeat(self, job_id: str) -> None:
        """
        Asynchronously send a heartbeat for a running job.

        This method is used by workers to indicate that a job is still active
        and to extend its lease, preventing it from being re-queued by other workers.

        Parameters
        ----------
        job_id : str
            The ID of the job for which to send a heartbeat.
        """
        msg = "Broker.heartbeat() must be implemented by subclasses."
        raise NotImplementedError(msg)
