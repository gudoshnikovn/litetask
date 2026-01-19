"""
Tests for the LiteTask TaskExecutor.
"""

import asyncio
import time

import pytest

from litetask.worker.executor import TaskExecutor


@pytest.fixture
def task_executor() -> TaskExecutor:
    """
    Provide a TaskExecutor instance for tests.
    """
    executor = TaskExecutor(max_threads=2)  # Use a small pool for testing
    yield executor
    executor.thread_pool.shutdown(wait=True)


async def test_task_executor_run_sync_function(task_executor: TaskExecutor) -> None:
    """
    Test that TaskExecutor can run a synchronous function.

    Parameters
    ----------
    task_executor : TaskExecutor
        A TaskExecutor instance.
    """

    def sync_func(a: int, b: int) -> int:
        time.sleep(0.01)  # Simulate work
        return a + b

    result = await task_executor.run(sync_func, 1, 2)
    assert result == 3


async def test_task_executor_run_async_function(task_executor: TaskExecutor) -> None:
    """
    Test that TaskExecutor can run an asynchronous function.

    Parameters
    ----------
    task_executor : TaskExecutor
        A TaskExecutor instance.
    """

    async def async_func(a: int, b: int) -> int:
        await asyncio.sleep(0.01)  # Simulate async work
        return a * b

    result = await task_executor.run(async_func, 3, 4)
    assert result == 12


async def test_task_executor_run_with_kwargs(task_executor: TaskExecutor) -> None:
    """
    Test that TaskExecutor can run functions with keyword arguments.

    Parameters
    ----------
    task_executor : TaskExecutor
        A TaskExecutor instance.
    """

    def sync_func_kwargs(val: str, prefix: str = "pre_") -> str:
        return prefix + val

    async def async_func_kwargs(val: str, suffix: str = "_suf") -> str:
        await asyncio.sleep(0.001)
        return val + suffix

    sync_result = await task_executor.run(sync_func_kwargs, "test", prefix="my_")
    assert sync_result == "my_test"

    async_result = await task_executor.run(async_func_kwargs, "async", suffix="_end")
    assert async_result == "async_end"


async def test_task_executor_run_sync_function_raises_exception(task_executor: TaskExecutor) -> None:
    """
    Test that TaskExecutor correctly propagates exceptions from synchronous functions.

    Parameters
    ----------
    task_executor : TaskExecutor
        A TaskExecutor instance.
    """

    def sync_func_error() -> None:
        msg = "Sync error"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="Sync error"):
        await task_executor.run(sync_func_error)


async def test_task_executor_run_async_function_raises_exception(task_executor: TaskExecutor) -> None:
    """
    Test that TaskExecutor correctly propagates exceptions from asynchronous functions.

    Parameters
    ----------
    task_executor : TaskExecutor
        A TaskExecutor instance.
    """

    async def async_func_error() -> None:
        await asyncio.sleep(0.001)
        msg = "Async error"
        raise TypeError(msg)

    with pytest.raises(TypeError, match="Async error"):
        await task_executor.run(async_func_error)


async def test_task_executor_concurrency_sync(task_executor: TaskExecutor) -> None:
    """
    Test that multiple synchronous tasks can run concurrently in the thread pool.

    Parameters
    ----------
    task_executor : TaskExecutor
        A TaskExecutor instance.
    """
    start_time = time.monotonic()

    def long_sync_func(duration: float) -> float:
        time.sleep(duration)
        return duration

    # Run two tasks that each take 0.1 seconds.
    # With a thread pool of size > 1, total time should be closer to 0.1s, not 0.2s.
    task1 = asyncio.create_task(task_executor.run(long_sync_func, 0.1))
    task2 = asyncio.create_task(task_executor.run(long_sync_func, 0.1))

    results = await asyncio.gather(task1, task2)
    end_time = time.monotonic()

    assert results == [0.1, 0.1]
    # Allow for some overhead, but it should be significantly less than 0.2s
    assert (end_time - start_time) < 0.15


async def test_task_executor_concurrency_async(task_executor: TaskExecutor) -> None:
    """
    Test that multiple asynchronous tasks run concurrently on the event loop.

    Parameters
    ----------
    task_executor : TaskExecutor
        A TaskExecutor instance.
    """
    start_time = time.monotonic()

    async def long_async_func(duration: float) -> float:
        await asyncio.sleep(duration)
        return duration

    # Run two async tasks that each take 0.1 seconds.
    # They should run concurrently on the event loop.
    task1 = asyncio.create_task(task_executor.run(long_async_func, 0.1))
    task2 = asyncio.create_task(task_executor.run(long_async_func, 0.1))

    results = await asyncio.gather(task1, task2)
    end_time = time.monotonic()

    assert results == [0.1, 0.1]
    # Allow for some overhead, but it should be significantly less than 0.2s
    assert (end_time - start_time) < 0.15
