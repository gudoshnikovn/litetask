"""
Tests for the LiteTask application class.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from litetask.app import LiteTask
from litetask.core.exceptions import BrokerError
from litetask.core.models import Job
from litetask.worker.worker import Worker


@pytest.fixture
def mock_sqlite_broker_class() -> MagicMock:
    """
    Mock the SQLiteBroker class for LiteTask._create_broker tests.
    """
    with patch("litetask.app.SQLiteBroker") as mock:
        yield mock


@pytest.fixture
def mock_memory_broker_class() -> MagicMock:
    """
    Mock the MemoryBroker class for LiteTask._create_broker tests.
    """
    with patch("litetask.app.MemoryBroker") as mock:
        yield mock


def test_litetask_init_sqlite_broker(mock_sqlite_broker_class: MagicMock) -> None:
    """
    Test LiteTask initialization with a SQLite broker URL.

    Parameters
    ----------
    mock_sqlite_broker_class : MagicMock
        Mocked SQLiteBroker class.
    """
    app = LiteTask("sqlite://test.db")
    mock_sqlite_broker_class.assert_called_once_with("test.db")
    assert isinstance(app.broker, MagicMock)  # It's a mock instance
    assert isinstance(app.worker, Worker)
    assert app.worker.broker == app.broker
    assert app.worker.task_registry == app.tasks


def test_litetask_init_memory_broker(mock_memory_broker_class: MagicMock) -> None:
    """
    Test LiteTask initialization with a Memory broker URL.

    Parameters
    ----------
    mock_memory_broker_class : MagicMock
        Mocked MemoryBroker class.
    """
    app = LiteTask("memory://")
    mock_memory_broker_class.assert_called_once()
    assert isinstance(app.broker, MagicMock)  # It's a mock instance


def test_litetask_init_default_sqlite_broker(mock_sqlite_broker_class: MagicMock) -> None:
    """
    Test LiteTask initialization with default SQLite broker URL.

    Parameters
    ----------
    mock_sqlite_broker_class : MagicMock
        Mocked SQLiteBroker class.
    """
    app = LiteTask()  # Uses default "sqlite://tasks.db"
    mock_sqlite_broker_class.assert_called_once_with("tasks.db")


def test_litetask_init_unknown_broker_scheme() -> None:
    """
    Test LiteTask initialization with an unknown broker scheme raises BrokerError.
    """
    with pytest.raises(BrokerError, match="Unknown broker scheme: unknown://"):
        LiteTask("unknown://")


def test_litetask_task_decorator_sync_delay(litetask_app_memory: LiteTask) -> None:
    """
    Test the @app.task() decorator for synchronous task registration and delay.

    Parameters
    ----------
    litetask_app_memory : LiteTask
        The LiteTask application instance with MemoryBroker.
    """
    mock_enqueue_sync = MagicMock(return_value="mock_job_id")
    litetask_app_memory.broker.enqueue_sync = mock_enqueue_sync

    @litetask_app_memory.task()
    def my_sync_task(a: int, b: str) -> str:
        return f"{a}-{b}"

    assert "my_sync_task" in litetask_app_memory.tasks
    assert litetask_app_memory.tasks["my_sync_task"] == my_sync_task

    job_id = my_sync_task.delay(1, "test")
    assert job_id == "mock_job_id"
    mock_enqueue_sync.assert_called_once()
    # Check the Job object passed to enqueue_sync
    job_arg: Job = mock_enqueue_sync.call_args[0][0]
    assert job_arg.task_name == "my_sync_task"
    assert job_arg.args == [1, "test"]
    assert job_arg.kwargs == {}


async def test_litetask_task_decorator_async_adelay(litetask_app_memory: LiteTask) -> None:
    """
    Test the @app.task() decorator for asynchronous task registration and adelay.

    Parameters
    ----------
    litetask_app_memory : LiteTask
        The LiteTask application instance with MemoryBroker.
    """
    mock_enqueue = AsyncMock(return_value="mock_async_job_id")
    litetask_app_memory.broker.enqueue = mock_enqueue

    @litetask_app_memory.task(name="custom_async_task")
    async def my_async_task(x: float, y: bool = True) -> str:
        await asyncio.sleep(0.001)
        return f"{x}-{y}"

    assert "custom_async_task" in litetask_app_memory.tasks
    assert litetask_app_memory.tasks["custom_async_task"] == my_async_task

    job_id = await my_async_task.adelay(3.14, y=False)
    assert job_id == "mock_async_job_id"
    mock_enqueue.assert_awaited_once()
    # Check the Job object passed to enqueue
    job_arg: Job = mock_enqueue.call_args[0][0]
    assert job_arg.task_name == "custom_async_task"
    assert job_arg.args == [3.14]
    assert job_arg.kwargs == {"y": False}


async def test_litetask_run_worker_sqlite_sets_loop(litetask_app_sqlite: LiteTask) -> None:
    """
    Test that run_worker sets the event loop for SQLiteBroker.

    Parameters
    ----------
    litetask_app_sqlite : LiteTask
        The LiteTask application instance with SQLiteBroker.
    """
    # Mock the worker's start method to prevent it from running indefinitely
    litetask_app_sqlite.worker.start = AsyncMock()

    # Mock the broker's set_loop method
    litetask_app_sqlite.broker.set_loop = MagicMock()

    await litetask_app_sqlite.run_worker()

    litetask_app_sqlite.broker.set_loop.assert_called_once_with(asyncio.get_running_loop())
    litetask_app_sqlite.worker.start.assert_awaited_once()


async def test_litetask_run_worker_memory_does_not_set_loop(litetask_app_memory: LiteTask) -> None:
    """
    Test that run_worker does not call set_loop for MemoryBroker (as it doesn't have it).

    Parameters
    ----------
    litetask_app_memory : LiteTask
        The LiteTask application instance with MemoryBroker.
    """
    # Mock the worker's start method to prevent it from running indefinitely
    litetask_app_memory.worker.start = AsyncMock()

    # Ensure MemoryBroker does not have set_loop
    assert not hasattr(litetask_app_memory.broker, "set_loop")

    await litetask_app_memory.run_worker()

    # Verify that set_loop was not called (it shouldn't even exist on MemoryBroker)
    # We can't assert_not_called on a non-existent method, but we can check that
    # no error was raised and worker.start was called.
    litetask_app_memory.worker.start.assert_awaited_once()
