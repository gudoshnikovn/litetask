"""
Integration tests for the LiteTask system, covering full task lifecycle
with both MemoryBroker and SQLiteBroker.
"""

import asyncio
import logging
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

import pytest

from litetask.app import LiteTask
from litetask.core.models import Job

logger = logging.getLogger(__name__)


@pytest.fixture
def simple_task_func() -> Callable[[str, int], str]:
    """
    Provide a simple synchronous task function for integration tests.
    """

    def _task(name: str, delay: int) -> str:
        time.sleep(delay)
        return f"Processed {name} in {delay}s"

    return _task


@pytest.fixture
def failing_task_func() -> Callable[[str], None]:
    """
    Provide a task function that always fails.
    """

    def _task(name: str) -> None:
        msg = f"Task for {name} failed intentionally!"
        raise ValueError(msg)

    return _task


@pytest.fixture
async def litetask_app_memory_with_task(
    litetask_app_memory: LiteTask, simple_task_func: Callable[[str, int], str], failing_task_func: Callable[[str], None],
) -> LiteTask:
    """
    Provide a LiteTask app with MemoryBroker and registered simple and failing tasks.

    Parameters
    ----------
    litetask_app_memory : LiteTask
        The LiteTask application instance with MemoryBroker.
    simple_task_func : Callable[[str, int], str]
        A simple task function fixture.
    failing_task_func : Callable[[str], None]
        A failing task function fixture.

    Returns
    -------
    LiteTask
        The LiteTask application instance with registered tasks.
    """

    @litetask_app_memory.task()
    def my_simple_task(name: str, delay: int) -> str:
        return simple_task_func(name, delay)

    @litetask_app_memory.task()
    def my_failing_task(name: str) -> None:
        return failing_task_func(name)

    # Ensure broker is connected for the app
    await litetask_app_memory.broker.connect()
    yield litetask_app_memory
    await litetask_app_memory.broker.close()


@pytest.fixture
async def litetask_app_sqlite_with_task(
    litetask_app_sqlite: LiteTask, simple_task_func: Callable[[str, int], str], failing_task_func: Callable[[str], None],
) -> LiteTask:
    """
    Provide a LiteTask app with SQLiteBroker and registered simple and failing tasks.

    Parameters
    ----------
    litetask_app_sqlite : LiteTask
        The LiteTask application instance with SQLiteBroker.
    simple_task_func : Callable[[str, int], str]
        A simple task function fixture.
    failing_task_func : Callable[[str], None]
        A failing task function fixture.

    Returns
    -------
    LiteTask
        The LiteTask application instance with registered tasks.
    """

    @litetask_app_sqlite.task()
    def my_simple_task(name: str, delay: int) -> str:
        return simple_task_func(name, delay)

    @litetask_app_sqlite.task()
    def my_failing_task(name: str) -> None:
        return failing_task_func(name)

    # Ensure broker is connected for the app
    await litetask_app_sqlite.broker.connect()
    yield litetask_app_sqlite
    await litetask_app_sqlite.broker.close()


async def test_sqlite_broker_full_sync_flow(litetask_app_sqlite_with_task: LiteTask) -> None:
    """
    Test a full synchronous task flow with SQLiteBroker: enqueue, worker process, result.

    Parameters
    ----------
    litetask_app_sqlite_with_task : LiteTask
        The LiteTask application instance with SQLiteBroker and registered tasks.
    """
    app = litetask_app_sqlite_with_task
    task_func = app.tasks["my_simple_task"]

    job_id = task_func.delay("SyncUserSQLite", 1)

    worker_task = asyncio.create_task(app.run_worker())

    job: Job | None = None
    conn: sqlite3.Connection | None = None
    try:
        for _ in range(50):  # Poll for up to 50 * 0.1 = 5 seconds
            await asyncio.sleep(0.1)
            conn = sqlite3.connect(app.broker.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM litetask_jobs WHERE id=?", (job_id,))
            row = cursor.fetchone()
            if row:
                job = Job.from_row(dict(row))
                if job.status not in ("pending", "running"):
                    break
            conn.close()
            conn = None
        else:
            pytest.fail("SQLite task did not complete in time.")
    finally:
        if conn:
            conn.close()

    assert job is not None
    assert job.id == job_id
    assert job.status == "success"
    assert job.result == "Processed SyncUserSQLite in 1s"

    app.worker.running = False
    await worker_task


async def test_sqlite_broker_full_async_flow(litetask_app_sqlite_with_task: LiteTask) -> None:
    """
    Test a full asynchronous task flow with SQLiteBroker: enqueue, worker process, result.

    Parameters
    ----------
    litetask_app_sqlite_with_task : LiteTask
        The LiteTask application instance with SQLiteBroker and registered tasks.
    """
    app = litetask_app_sqlite_with_task
    task_func = app.tasks["my_simple_task"]

    job_id = await task_func.adelay("AsyncUserSQLite", 1)

    worker_task = asyncio.create_task(app.run_worker())

    job: Job | None = None
    conn: sqlite3.Connection | None = None
    try:
        for _ in range(50):  # Poll for up to 50 * 0.1 = 5 seconds
            await asyncio.sleep(0.1)
            conn = sqlite3.connect(app.broker.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM litetask_jobs WHERE id=?", (job_id,))
            row = cursor.fetchone()
            if row:
                job = Job.from_row(dict(row))
                if job.status not in ("pending", "running"):
                    break
            conn.close()
            conn = None
        else:
            pytest.fail("SQLite task did not complete in time.")
    finally:
        if conn:
            conn.close()

    assert job is not None
    assert job.id == job_id
    assert job.status == "success"
    assert job.result == "Processed AsyncUserSQLite in 1s"

    app.worker.running = False
    await worker_task


async def test_sqlite_broker_failing_task_flow(litetask_app_sqlite_with_task: LiteTask) -> None:
    """
    Test a failing task flow with SQLiteBroker.

    Parameters
    ----------
    litetask_app_sqlite_with_task : LiteTask
        The LiteTask application instance with SQLiteBroker and registered tasks.
    """
    app = litetask_app_sqlite_with_task
    task_func = app.tasks["my_failing_task"]

    job_id = await task_func.adelay("FailingUserSQLite")

    worker_task = asyncio.create_task(app.run_worker())

    job: Job | None = None
    conn: sqlite3.Connection | None = None
    try:
        for _ in range(50):  # Poll for up to 50 * 0.1 = 5 seconds
            await asyncio.sleep(0.1)
            conn = sqlite3.connect(app.broker.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM litetask_jobs WHERE id=?", (job_id,))
            row = cursor.fetchone()
            if row:
                job = Job.from_row(dict(row))
                if job.status not in ("pending", "running"):
                    break
            conn.close()
            conn = None
        else:
            pytest.fail("Failing SQLite task did not complete (fail) in time.")
    finally:
        if conn:
            conn.close()

    assert job is not None
    assert job.id == job_id
    assert job.status == "failed"
    assert job.error == "Task for FailingUserSQLite failed intentionally!"

    app.worker.running = False
    await worker_task


async def test_sqlite_broker_dequeue_with_run_at_in_integration(litetask_app_sqlite_with_task: LiteTask) -> None:
    """
    Test that SQLiteBroker's dequeue respects run_at in an integration scenario.

    Parameters
    ----------
    litetask_app_sqlite_with_task : LiteTask
        The LiteTask application instance with SQLiteBroker and registered tasks.
    """
    app = litetask_app_sqlite_with_task
    task_func = app.tasks["my_simple_task"]

    # Task to run in the future
    future_run_at = datetime.now(timezone.utc) + timedelta(seconds=2)
    job_future_id = await task_func.adelay("FutureTask", 0, run_at=future_run_at)

    # Task to run immediately
    job_now_id = await task_func.adelay("ImmediateTask", 0)

    worker_task = asyncio.create_task(app.run_worker())

    # First, only ImmediateTask should be processed
    job_now: Job | None = None
    job_future: Job | None = None
    conn: sqlite3.Connection | None = None
    try:
        for _ in range(20):  # Wait up to 20 * 0.1 = 2 seconds for immediate task
            await asyncio.sleep(0.1)
            conn = sqlite3.connect(app.broker.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM litetask_jobs WHERE id=?", (job_now_id,))
            row_now = cursor.fetchone()
            if row_now:
                job_now = Job.from_row(dict(row_now))
                if job_now.status == "success":
                    break
            conn.close()
            conn = None
        else:
            pytest.fail("ImmediateTask did not complete in time.")

        assert job_now is not None
        assert job_now.status == "success"

        # Check that future task is still pending
        conn = sqlite3.connect(app.broker.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM litetask_jobs WHERE id=?", (job_future_id,))
        row_future = cursor.fetchone()
        conn.close()
        conn = None
        assert row_future is not None
        job_future = Job.from_row(dict(row_future))
        assert job_future.status == "pending"

        # Now wait for the future task to become eligible and complete
        for _ in range(50):  # Wait up to 50 * 0.1 = 5 seconds more
            await asyncio.sleep(0.1)
            conn = sqlite3.connect(app.broker.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM litetask_jobs WHERE id=?", (job_future_id,))
            row_future = cursor.fetchone()
            if row_future:
                job_future = Job.from_row(dict(row_future))
                if job_future.status == "success":
                    break
            conn.close()
            conn = None
        else:
            pytest.fail("FutureTask did not complete in time after its run_at.")

        assert job_future is not None
        assert job_future.status == "success"

    finally:
        if conn:
            conn.close()
        app.worker.running = False
        await worker_task
