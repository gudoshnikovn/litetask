"""
Tests for the LiteTask SQLiteBroker implementation.
"""

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from litetask.broker.sqlite import SQLiteBroker
from litetask.core.models import Job


@pytest.mark.asyncio
async def test_sqlite_broker_connect_close(sqlite_broker: SQLiteBroker) -> None:
    """
    Test that the SQLiteBroker can connect and close without errors.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    assert sqlite_broker._running is True
    assert sqlite_broker._writer_thread is not None
    assert sqlite_broker._writer_thread.is_alive()

    await sqlite_broker.close()
    assert sqlite_broker._running is False
    # Give a moment for the thread to actually stop
    await asyncio.sleep(0.1)
    assert not sqlite_broker._writer_thread.is_alive()


@pytest.mark.asyncio
async def test_sqlite_broker_init_db(tmp_path: Path) -> None:
    """
    Test that _init_db creates the necessary table and index.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory path provided by pytest.
    """
    db_path = tmp_path / "test_init.db"
    broker = SQLiteBroker(db_path)
    # Manually call _init_db for testing purposes, normally called by connect
    broker._init_db()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='litetask_jobs';")
    assert cursor.fetchone() is not None

    # Check if index exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_status_run';")
    assert cursor.fetchone() is not None

    conn.close()
    await broker.close()


@pytest.mark.asyncio
async def test_sqlite_broker_enqueue_async(sqlite_broker: SQLiteBroker) -> None:
    """
    Test asynchronous enqueue of a job into SQLiteBroker.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    job = Job(task_name="test_async_task", args=[1], kwargs={"key": "value"})
    job_id = await sqlite_broker.enqueue(job)

    assert job_id == job.id

    # Verify job in database
    conn = sqlite3.connect(sqlite_broker.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id, task_name, payload, status FROM litetask_jobs WHERE id=?", (job_id,))
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == job.id
    assert row[1] == job.task_name
    assert json.loads(row[2]) == {"args": job.args, "kwargs": job.kwargs}
    assert row[3] == "pending"


@pytest.mark.asyncio
async def test_sqlite_broker_enqueue_sync(sqlite_broker: SQLiteBroker) -> None:
    """
    Test synchronous enqueue of a job into SQLiteBroker.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    job = Job(task_name="test_sync_task", args=["hello"], kwargs={"count": 5})
    job_id = sqlite_broker.enqueue_sync(job)

    assert job_id == job.id

    # Give writer thread a moment to process
    await asyncio.sleep(0.1)

    # Verify job in database
    conn = sqlite3.connect(sqlite_broker.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id, task_name, payload, status FROM litetask_jobs WHERE id=?", (job_id,))
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == job.id
    assert row[1] == job.task_name
    assert json.loads(row[2]) == {"args": job.args, "kwargs": job.kwargs}
    assert row[3] == "pending"


@pytest.mark.asyncio
async def test_sqlite_broker_dequeue(sqlite_broker: SQLiteBroker) -> None:
    """
    Test dequeuing a job from SQLiteBroker.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    job = Job(task_name="test_dequeue_task")
    await sqlite_broker.enqueue(job)

    dequeued_data = await sqlite_broker.dequeue()
    assert dequeued_data is not None
    assert dequeued_data["id"] == job.id
    assert dequeued_data["task_name"] == job.task_name
    assert json.loads(dequeued_data["payload"]) == {"args": job.args, "kwargs": job.kwargs}
    assert dequeued_data["status"] == "running"
    assert dequeued_data["lease_expires_at"] is not None
    assert dequeued_data["heartbeat_at"] is not None

    # Verify status updated in database
    conn = sqlite3.connect(sqlite_broker.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM litetask_jobs WHERE id=?", (job.id,))
    status = cursor.fetchone()[0]
    conn.close()
    assert status == "running"


@pytest.mark.asyncio
async def test_sqlite_broker_dequeue_empty(sqlite_broker: SQLiteBroker) -> None:
    """
    Test dequeue when no jobs are available.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    dequeued_data = await sqlite_broker.dequeue()
    assert dequeued_data is None


@pytest.mark.asyncio
async def test_sqlite_broker_dequeue_respects_run_at(sqlite_broker: SQLiteBroker) -> None:
    """
    Test that dequeue respects the `run_at` timestamp.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    future_time = datetime.now(timezone.utc) + timedelta(seconds=5)
    job_future = Job(task_name="future_task", run_at=future_time)
    job_now = Job(task_name="now_task", run_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    job_no_run_at = Job(task_name="no_run_at_task")

    await sqlite_broker.enqueue(job_future)
    await sqlite_broker.enqueue(job_now)
    await sqlite_broker.enqueue(job_no_run_at)

    # Only job_now and job_no_run_at should be dequeued
    dequeued1 = await sqlite_broker.dequeue()
    dequeued2 = await sqlite_broker.dequeue()
    dequeued3 = await sqlite_broker.dequeue()  # Should be None

    assert dequeued1 is not None
    assert dequeued2 is not None
    # Order is by created_at, so job_now or job_no_run_at could be first
    dequeued_ids = {dequeued1["id"], dequeued2["id"]}
    assert job_now.id in dequeued_ids
    assert job_no_run_at.id in dequeued_ids
    assert job_future.id not in dequeued_ids


@pytest.mark.asyncio
async def test_sqlite_broker_update_status(sqlite_broker: SQLiteBroker) -> None:
    """
    Test updating the status of a job in SQLiteBroker.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    job = Job(task_name="update_status_task")
    await sqlite_broker.enqueue(job)
    await sqlite_broker.dequeue()  # Set to running

    await sqlite_broker.update_status(job.id, "success", result="task_completed")

    conn = sqlite3.connect(sqlite_broker.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT status, result, error FROM litetask_jobs WHERE id=?", (job.id,))
    status, result_json, error = cursor.fetchone()
    conn.close()

    assert status == "success"
    assert json.loads(result_json) == "task_completed"
    assert error is None

    await sqlite_broker.update_status(job.id, "failed", error="something went wrong")

    conn = sqlite3.connect(sqlite_broker.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT status, result, error FROM litetask_jobs WHERE id=?", (job.id,))
    status, result_json, error = cursor.fetchone()
    conn.close()

    assert status == "failed"
    assert json.loads(result_json) == "task_completed"  # Result should persist
    assert error == "something went wrong"


@pytest.mark.asyncio
async def test_sqlite_broker_heartbeat(sqlite_broker: SQLiteBroker) -> None:
    """
    Test the heartbeat mechanism in SQLiteBroker.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    job = Job(task_name="heartbeat_task")
    await sqlite_broker.enqueue(job)
    await sqlite_broker.dequeue()  # Set to running

    conn = sqlite3.connect(sqlite_broker.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT heartbeat_at FROM litetask_jobs WHERE id=?", (job.id,))
    original_heartbeat_str = cursor.fetchone()[0]
    original_heartbeat = datetime.fromisoformat(original_heartbeat_str).astimezone(timezone.utc)
    conn.close()

    await asyncio.sleep(0.01)  # Ensure a small time difference
    await sqlite_broker.heartbeat(job.id)

    conn = sqlite3.connect(sqlite_broker.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT heartbeat_at FROM litetask_jobs WHERE id=?", (job.id,))
    new_heartbeat_str = cursor.fetchone()[0]
    new_heartbeat = datetime.fromisoformat(new_heartbeat_str).astimezone(timezone.utc)
    conn.close()

    assert new_heartbeat > original_heartbeat


@pytest.mark.asyncio
async def test_sqlite_broker_set_loop_and_wait_for_job(sqlite_broker: SQLiteBroker) -> None:
    """
    Test set_loop and wait_for_job functionality.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    loop = asyncio.get_running_loop()
    sqlite_broker.set_loop(loop)

    # Start waiting for a job in the background
    wait_task = asyncio.create_task(sqlite_broker.wait_for_job())

    # Enqueue a job synchronously, which should notify the waiting task
    job = Job(task_name="notified_task")
    sqlite_broker.enqueue_sync(job)

    # The wait_task should complete shortly after enqueue_sync
    await asyncio.wait_for(wait_task, timeout=1.0)
    assert wait_task.done()
    # After wait_for_job, the event should be cleared
    await asyncio.sleep(0.01)  # Give a moment for clear to happen
    assert not sqlite_broker._new_job_event.is_set()


@pytest.mark.asyncio
async def test_sqlite_broker_wait_for_job_timeout(sqlite_broker: SQLiteBroker) -> None:
    """
    Test that wait_for_job times out if no job is enqueued.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    loop = asyncio.get_running_loop()
    sqlite_broker.set_loop(loop)

    # wait_for_job has an internal timeout of 5 seconds
    wait_task = asyncio.create_task(sqlite_broker.wait_for_job())

    # Wait for a shorter period than the internal timeout
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(wait_task, timeout=0.1)

    # The task is cancelled by asyncio.wait_for, making it done. The pytest.raises already verifies the timeout.
    wait_task.cancel()  # Clean up the task
    try:
        await wait_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_sqlite_broker_writer_thread_error_handling(sqlite_broker: SQLiteBroker) -> None:
    """
    Test error handling in the writer thread.

    Parameters
    ----------
    sqlite_broker : SQLiteBroker
        A connected SQLiteBroker instance.
    """
    # Attempt to insert a job with a duplicate ID (violates PRIMARY KEY constraint)
    job1 = Job(task_name="error_task")
    job2 = Job(task_name="error_task", id=job1.id)

    await sqlite_broker.enqueue(job1)

    # Enqueue the duplicate, expect an exception to be set on the future
    with pytest.raises(sqlite3.IntegrityError):
        await sqlite_broker.enqueue(job2)

    # Ensure the broker is still functional afterwards
    job3 = Job(task_name="another_task")
    job_id_3 = await sqlite_broker.enqueue(job3)
    assert job_id_3 == job3.id
