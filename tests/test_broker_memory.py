# tests/test_broker_memory.py
"""
Tests for the LiteTask MemoryBroker implementation.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from litetask.broker.memory import MemoryBroker
from litetask.core.models import Job

logger = logging.getLogger(__name__)


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
def sample_job() -> Job:
    """
    Provide a sample Job object for testing.
    """
    return Job(task_name="test_task", args=[1, "hello"], kwargs={"key": "value"})


@pytest.mark.asyncio
async def test_memory_broker_connect_close(memory_broker: MemoryBroker) -> None:
    """
    Test that the MemoryBroker can connect and close without errors.
    """
    assert memory_broker._stopped is False
    await memory_broker.close()
    assert memory_broker._stopped is True
    # Reconnect to ensure it can be used again if needed (though not typical for a fixture)
    await memory_broker.connect()
    assert memory_broker._stopped is False


@pytest.mark.asyncio
async def test_memory_broker_enqueue_async(memory_broker: MemoryBroker, sample_job: Job) -> None:
    """
    Test asynchronous enqueue of a job into MemoryBroker.
    """
    job_id = await memory_broker.enqueue(sample_job)

    assert job_id == sample_job.id
    assert memory_broker.jobs[job_id] == sample_job
    assert not memory_broker.queue.empty()
    assert await memory_broker.queue.get() == job_id


@pytest.mark.asyncio
async def test_memory_broker_enqueue_sync(memory_broker: MemoryBroker, sample_job: Job) -> None:
    """
    Test synchronous enqueue of a job into MemoryBroker.
    """
    job_id = memory_broker.enqueue_sync(sample_job)

    assert job_id == sample_job.id
    assert memory_broker.jobs[job_id] == sample_job
    assert not memory_broker.queue.empty()
    assert await memory_broker.queue.get() == job_id


@pytest.mark.asyncio
async def test_memory_broker_enqueue_sync_queue_full(
    memory_broker: MemoryBroker, sample_job: Job, caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test synchronous enqueue when the internal queue is full.
    """
    with patch.object(memory_broker.queue, "put_nowait", side_effect=asyncio.QueueFull):
        with caplog.at_level(logging.WARNING):
            job_id = memory_broker.enqueue_sync(sample_job)

            assert job_id == sample_job.id
            assert memory_broker.jobs[job_id] == sample_job
            assert "MemoryBroker queue is full" in caplog.text
            assert memory_broker.queue.empty()  # put_nowait failed, so queue should be empty


@pytest.mark.asyncio
async def test_memory_broker_dequeue_success(memory_broker: MemoryBroker, sample_job: Job) -> None:
    """
    Test successful dequeuing of a job from MemoryBroker.
    """
    await memory_broker.enqueue(sample_job)

    dequeued_data = await memory_broker.dequeue()

    assert dequeued_data is not None
    assert dequeued_data["id"] == sample_job.id
    assert dequeued_data["task_name"] == sample_job.task_name
    assert json.loads(dequeued_data["payload"]) == {"args": sample_job.args, "kwargs": sample_job.kwargs}
    # lease_expires_at and heartbeat_at are not set by MemoryBroker.dequeue
    # but are part of the expected return structure, so they should be None
    assert dequeued_data["lease_expires_at"] is None
    assert dequeued_data["heartbeat_at"] is None


@pytest.mark.asyncio
async def test_memory_broker_dequeue_empty(memory_broker: MemoryBroker) -> None:
    """
    Test dequeue when no jobs are available.
    """
    dequeued_data = await memory_broker.dequeue()
    assert dequeued_data is None


@pytest.mark.asyncio
async def test_memory_broker_dequeue_when_stopped(memory_broker: MemoryBroker, sample_job: Job) -> None:
    """
    Test dequeue returns None when the broker is stopped.
    """
    await memory_broker.enqueue(sample_job)
    await memory_broker.close()  # Stop the broker

    dequeued_data = await memory_broker.dequeue()
    assert dequeued_data is None


@pytest.mark.asyncio
async def test_memory_broker_dequeue_job_id_not_in_jobs_dict(
    memory_broker: MemoryBroker, sample_job: Job, caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test dequeue when a job ID is in the queue but not in the jobs dictionary.
    """
    await memory_broker.enqueue(sample_job)
    del memory_broker.jobs[sample_job.id]  # Simulate job being removed from dict

    with caplog.at_level(logging.WARNING):
        dequeued_data = await memory_broker.dequeue()
        assert dequeued_data is None
        assert f"Dequeued job ID {sample_job.id} not found in jobs dictionary." in caplog.text


@pytest.mark.asyncio
async def test_memory_broker_dequeue_error_handling(
    memory_broker: MemoryBroker, caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test error handling during dequeue.
    """
    # Simulate an error during queue.get()
    with patch.object(memory_broker.queue, "get", side_effect=Exception("Simulated dequeue error")):
        with caplog.at_level(logging.ERROR):
            dequeued_data = await memory_broker.dequeue()
            assert dequeued_data is None
            assert "Error during memory broker dequeue." in caplog.text


@pytest.mark.asyncio
async def test_memory_broker_update_status_success(memory_broker: MemoryBroker, sample_job: Job) -> None:
    """
    Test updating the status of an existing job in MemoryBroker.
    """
    await memory_broker.enqueue(sample_job)

    new_result = {"status": "done"}
    error_msg = "No error"
    await memory_broker.update_status(sample_job.id, "success", result=new_result, error=error_msg)

    updated_job = memory_broker.jobs[sample_job.id]
    assert updated_job.status == "success"
    assert updated_job.result == new_result
    assert updated_job.error == error_msg


@pytest.mark.asyncio
async def test_memory_broker_update_status_non_existent_job(
    memory_broker: MemoryBroker, caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test updating status for a non-existent job.
    """
    non_existent_job_id = "non-existent-id"
    with caplog.at_level(logging.WARNING):
        await memory_broker.update_status(non_existent_job_id, "failed", error="test error")
        assert f"Attempted to update status for non-existent job ID {non_existent_job_id}." in caplog.text


@pytest.mark.asyncio
async def test_memory_broker_heartbeat_success(memory_broker: MemoryBroker, sample_job: Job) -> None:
    """
    Test sending a heartbeat for an existing job.
    """
    await memory_broker.enqueue(sample_job)
    initial_heartbeat = sample_job.heartbeat_at

    # Simulate time passing
    await asyncio.sleep(0.01)

    # Patch asyncio.current_datetime to control the time for the heartbeat
    with patch("litetask.broker.memory.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.now(timezone.utc) + timedelta(seconds=5)
        mock_datetime.timezone = timezone

        await memory_broker.heartbeat(sample_job.id)
        updated_job = memory_broker.jobs[sample_job.id]
        assert updated_job.heartbeat_at == mock_datetime.now.return_value
        assert updated_job.heartbeat_at > (initial_heartbeat or datetime.min.replace(tzinfo=timezone.utc))


@pytest.mark.asyncio
async def test_memory_broker_heartbeat_non_existent_job(
    memory_broker: MemoryBroker, caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test sending a heartbeat for a non-existent job.
    """
    non_existent_job_id = "non-existent-heartbeat-id"
    with caplog.at_level(logging.WARNING):
        await memory_broker.heartbeat(non_existent_job_id)
        assert f"Heartbeat received for non-existent job ID {non_existent_job_id}." in caplog.text
