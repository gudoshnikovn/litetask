"""
Task execution management for LiteTask workers.

This module provides the `TaskExecutor` class, which handles the execution
of both synchronous and asynchronous task functions, leveraging a thread pool
for synchronous tasks to avoid blocking the event loop.
"""

import asyncio
import inspect
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any


class TaskExecutor:
    """
    Manage the execution of task functions.

    It can execute both coroutine functions (awaiting them directly)
    and regular synchronous functions (running them in a thread pool).

    Parameters
    ----------
    max_threads : int, optional
        The maximum number of worker threads to use for synchronous tasks.
        Defaults to 10.
    """

    def __init__(self, max_threads: int = 10) -> None:
        self.thread_pool = ThreadPoolExecutor(max_workers=max_threads)

    async def run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Execute a given function, handling both async and sync callables.

        If the function is a coroutine, it's awaited directly.
        If it's a regular function, it's run in a separate thread
        from the thread pool to prevent blocking the asyncio event loop.

        Parameters
        ----------
        func : Callable[..., Any]
            The task function to execute.
        *args : Any
            Positional arguments to pass to the function.
        **kwargs : Any
            Keyword arguments to pass to the function.

        Returns
        -------
        Any
            The result of the function execution.
        """
        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.thread_pool, lambda: func(*args, **kwargs))
