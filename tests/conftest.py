import asyncio
import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from litetask.app import LiteTask
from litetask.broker.memory import MemoryBroker
from litetask.broker.sqlite import SQLiteBroker
from litetask.worker.worker import Worker

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@pytest.fixture
def event_loop() -> asyncio.AbstractEventLoop:
    """
    Provide an event loop for pytest-asyncio.
    """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def memory_broker() -> MemoryBroker:
    """
    Provide a connected MemoryBroker instance for tests.
    """
    broker = MemoryBroker()
    await broker.connect()
    yield broker
    await broker.close()


@pytest.fixture
async def sqlite_broker(tmp_path: Path) -> SQLiteBroker:
    """
    Provide a connected SQLiteBroker instance with a temporary database.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory path provided by pytest.

    Returns
    -------
    SQLiteBroker
        A connected SQLiteBroker instance.
    """
    db_path = tmp_path / "test_litetask.db"
    broker = SQLiteBroker(db_path)
    await broker.connect()
    yield broker
    await broker.close()


@pytest.fixture
def mock_task_func() -> Callable[..., Any]:
    """
    Provide a mock synchronous task function for testing.
    """

    def _mock_task(arg1: str, kwarg1: int) -> str:
        return f"Sync result: {arg1}-{kwarg1}"

    return _mock_task


@pytest.fixture
def mock_async_task_func() -> Callable[..., Any]:
    """
    Provide a mock asynchronous task function for testing.
    """

    async def _mock_async_task(arg1: str, kwarg1: int) -> str:
        await asyncio.sleep(0.01)  # Simulate async work
        return f"Async result: {arg1}-{kwarg1}"

    return _mock_async_task


@pytest.fixture
def litetask_app_memory() -> LiteTask:
    """
    Provide a LiteTask application instance configured with MemoryBroker.
    """
    app = LiteTask("memory://")
    return app


@pytest.fixture
def litetask_app_sqlite(tmp_path: Path) -> LiteTask:
    """
    Provide a LiteTask application instance configured with SQLiteBroker.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory path provided by pytest.

    Returns
    -------
    LiteTask
        A LiteTask application instance with SQLiteBroker.
    """
    db_path = tmp_path / "test_litetask_app.db"
    app = LiteTask(f"sqlite://{db_path}")
    return app


@pytest.fixture
def sample_job_data() -> dict[str, Any]:
    """
    Provide sample raw job data for testing Job.from_row.
    """
    now_utc = datetime.now(timezone.utc)
    return {
        "id": "test-job-id-123",
        "task_name": "my_task",
        "payload": json.dumps({"args": ["value1"], "kwargs": {"key1": "value2"}}),
        "status": "pending",
        "lease_expires_at": (now_utc + timedelta(minutes=1)).isoformat(),
        "heartbeat_at": now_utc.isoformat(),
        "retries_count": 0,
        "uniqueness_key": None,
        "group_name": None,
        "run_at": (now_utc + timedelta(seconds=5)).isoformat(),
        "result": None,
        "error": None,
        "created_at": now_utc.isoformat(),
    }


@pytest.fixture
def sample_job_data_sqlite_format() -> dict[str, Any]:
    """
    Provide sample raw job data in SQLite's typical datetime string format.
    """
    now_utc = datetime.now(timezone.utc)
    return {
        "id": "test-job-id-sqlite",
        "task_name": "my_sqlite_task",
        "payload": json.dumps({"args": ["sqlite_val"], "kwargs": {"sqlite_key": 1}}),
        "status": "pending",
        "lease_expires_at": (now_utc + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "heartbeat_at": now_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "retries_count": 0,
        "uniqueness_key": None,
        "group_name": None,
        "run_at": (now_utc + timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "result": json.dumps("SQLite result"),
        "error": None,
        "created_at": now_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
    }


@pytest.fixture
def sample_job_data_no_microseconds() -> dict[str, Any]:
    """
    Provide sample raw job data in SQLite's typical datetime string format without microseconds.
    """
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    return {
        "id": "test-job-id-no-ms",
        "task_name": "my_no_ms_task",
        "payload": json.dumps({"args": ["no_ms_val"], "kwargs": {"no_ms_key": 2}}),
        "status": "pending",
        "lease_expires_at": (now_utc + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "heartbeat_at": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "retries_count": 0,
        "uniqueness_key": None,
        "group_name": None,
        "run_at": (now_utc + timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S"),
        "result": json.dumps("No microseconds result"),
        "error": None,
        "created_at": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
    }


@pytest.fixture
def sample_job_data_none_datetimes() -> dict[str, Any]:
    """
    Provide sample raw job data with None values for datetime fields.
    """
    now_utc = datetime.now(timezone.utc)
    return {
        "id": "test-job-id-none-dt",
        "task_name": "my_none_dt_task",
        "payload": json.dumps({"args": [], "kwargs": {}}),
        "status": "pending",
        "lease_expires_at": None,
        "heartbeat_at": None,
        "retries_count": 0,
        "uniqueness_key": None,
        "group_name": None,
        "run_at": None,
        "result": None,
        "error": None,
        "created_at": now_utc.isoformat(),
    }


@pytest.fixture
def worker_with_mocks(mock_broker: Any, mock_task_executor: Any, mock_task_func: Callable[..., Any]) -> Worker:
    """
    Provide a Worker instance with mocked broker and executor.

    Ensures the real TaskExecutor's thread pool, which is created during
    Worker initialization, is properly shut down after tests.

    Parameters
    ----------
    mock_broker : Any
        A mocked broker instance.
    mock_task_executor : Any
        A mocked task executor instance.
    mock_task_func : Callable[..., Any]
        A mock task function.

    Returns
    -------
    Worker
        A Worker instance configured with mocks.
    """
    task_registry = {"my_worker_task": mock_task_func}
    worker = Worker(mock_broker, task_registry)

    # Store a reference to the real TaskExecutor's thread_pool
    # before replacing the executor with a mock.
    real_thread_pool = worker.executor.thread_pool

    # Replace the real executor with the mock for testing purposes.
    worker.executor = mock_task_executor

    yield worker

    # Ensure the real thread pool is shut down after the test finishes.
    # This prevents background threads from hanging the test suite.
    real_thread_pool.shutdown(wait=True)
