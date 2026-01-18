"""
The main application entry point for LiteTask.

This module defines the `LiteTask` class, which serves as the central hub for
registering tasks, configuring the broker, and running the worker.
"""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from litetask.broker.base import Broker
from litetask.broker.memory import MemoryBroker
from litetask.broker.sqlite import SQLiteBroker
from litetask.core.exceptions import BrokerError
from litetask.core.models import Job
from litetask.worker.worker import Worker

logger = logging.getLogger(__name__)


class LiteTask:
    """
    The main application class for LiteTask.

    Manages task registration, broker configuration, and worker execution.

    Parameters
    ----------
    broker_url : str, optional
        The URL for the task broker (e.g., "sqlite://tasks.db", "memory://").
        Defaults to "sqlite://tasks.db".

    Attributes
    ----------
    broker : Broker
        The configured task broker instance.
    tasks : dict[str, Callable[..., Any]]
        A dictionary mapping task names to their callable functions.
    worker : Worker
        The worker instance responsible for executing tasks.
    """

    def __init__(self, broker_url: str = "sqlite://tasks.db") -> None:
        self.broker: Broker = self._create_broker(broker_url)
        self.tasks: dict[str, Callable[..., Any]] = {}
        # Worker is decoupled, receiving broker and task_registry directly
        self.worker: Worker = Worker(self.broker, self.tasks)

    def _create_broker(self, url: str) -> Broker:
        """
        Creates a broker instance based on the provided URL.

        Parameters
        ----------
        url : str
            The broker URL (e.g., "sqlite://tasks.db", "memory://").

        Returns
        -------
        Broker
            An instance of the appropriate broker.

        Raises
        ------
        BrokerError
            If the broker scheme in the URL is unknown.
        """
        if url.startswith("sqlite://"):
            path = url.replace("sqlite://", "")
            if not path:
                path = "tasks.db"  # Default SQLite database file
            return SQLiteBroker(path)
        if url == "memory://":
            return MemoryBroker()
        msg = f"Unknown broker scheme: {url}"
        raise BrokerError(msg)

    def task(self, name: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Decorator to register a function as a LiteTask task.

        This decorator registers the decorated function with the LiteTask application
        and adds `delay` and `adelay` methods to it for synchronous and asynchronous
        task enqueueing, respectively.

        Parameters
        ----------
        name : str | None, optional
            The name to register the task under. If None, the function's name is used.

        Returns
        -------
        Callable[[Callable[..., Any]], Callable[..., Any]]
            A decorator that registers the function and adds `delay` and `adelay` methods.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or func.__name__
            self.tasks[task_name] = func

            def delay(*args: Any, **kwargs: Any) -> str:
                """
                Synchronously enqueues the task.

                Parameters
                ----------
                *args : Any
                    Positional arguments for the task function.
                **kwargs : Any
                    Keyword arguments for the task function.

                Returns
                -------
                str
                    The ID of the enqueued job.
                """
                job = Job(task_name=task_name, args=list(args), kwargs=kwargs)
                return self.broker.enqueue_sync(job)

            async def adelay(*args: Any, **kwargs: Any) -> str:
                """
                Asynchronously enqueues the task.

                Parameters
                ----------
                *args : Any
                    Positional arguments for the task function.
                **kwargs : Any
                    Keyword arguments for the task function.

                Returns
                -------
                str
                    The ID of the enqueued job.
                """
                job = Job(task_name=task_name, args=list(args), kwargs=kwargs)
                return await self.broker.enqueue(job)

            # Attach delay and adelay methods to the original function
            func.delay = delay
            func.adelay = adelay
            return func

        return decorator

    async def run_worker(self) -> None:
        """
        Starts the LiteTask worker.

        This method initializes the worker and begins processing tasks from the broker.
        For SQLite brokers, it also registers the current asyncio event loop
        to enable notifications for new jobs.
        """
        if hasattr(self.broker, "set_loop"):
            set_loop_method = self.broker.set_loop
            set_loop_method(asyncio.get_running_loop())

        await self.worker.start()
