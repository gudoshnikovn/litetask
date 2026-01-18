"""
Tests for the LiteTask Worker implementation.
"""

import asyncio
import json
import re
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from litetask.core.exceptions import WorkerError
from litetask.worker.worker import Worker


@pytest.fixture
def mock_broker() -> AsyncMock:
    """
    Provide a mock Broker instance for Worker tests.
    """
    broker = AsyncMock()
    broker.connect = AsyncMock()
    broker.close = AsyncMock()
    broker.dequeue = AsyncMock(return_value=None)
    broker.wait_for_job = AsyncMock(return_value=None)
    return broker


@pytest.fixture
def mock_task_executor() -> AsyncMock:
    """
    Provide a mock TaskExecutor instance for Worker tests.
    """
    executor = AsyncMock()
    executor.run = AsyncMock()
    return executor


@pytest.fixture
def sample_job_data_for_worker() -> dict[str, Any]:
    """
    Provide sample raw job data suitable for Worker processing.
    """
    return {
        "id": "worker-test-job-1",
        "task_name": "my_worker_task",
        "payload": json.dumps({"args": ["arg1"], "kwargs": {"kwarg1": "value1"}}),
        "status": "pending",
        "lease_expires_at": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        "heartbeat_at": datetime.now(timezone.utc).isoformat(),
        "retries_count": 0,
        "uniqueness_key": None,
        "group_name": None,
        "run_at": None,
        "result": None,
        "error": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def worker_with_mocks(
    mock_broker: AsyncMock, mock_task_executor: AsyncMock, mock_task_func: Callable[..., Any],
) -> Worker:
    """
    Provide a Worker instance with mocked broker and executor.

    Parameters
    ----------
    mock_broker : AsyncMock
        A mocked broker instance.
    mock_task_executor : AsyncMock
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
    worker.executor = mock_task_executor  # Replace real executor with mock
    return worker


@pytest.mark.asyncio
async def test_worker_start_and_stop(worker_with_mocks: Worker, mock_broker: AsyncMock) -> None:
    """
    Test that the worker can start and stop gracefully.

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    """
    worker_task = asyncio.create_task(worker_with_mocks.start())

    # Allow worker to run for a bit, dequeueing Nones
    await asyncio.sleep(0.1)

    # Signal the worker to stop
    worker_with_mocks.running = False

    # Wait for the worker task to complete its graceful shutdown.
    await asyncio.wait_for(worker_task, timeout=2.0)

    assert worker_with_mocks.running is False
    mock_broker.connect.assert_awaited_once()
    mock_broker.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_processes_job_successfully(
    worker_with_mocks: Worker,
    mock_broker: AsyncMock,
    mock_task_executor: AsyncMock,
    sample_job_data_for_worker: dict[str, Any],
) -> None:
    """
    Test that the worker successfully processes a job.

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    mock_task_executor : AsyncMock
        A mocked task executor instance.
    sample_job_data_for_worker : dict[str, Any]
        Sample job data for worker processing.
    """
    # Make dequeue return the sample job data once, then fall back to return_value=None
    mock_broker.dequeue.side_effect = [sample_job_data_for_worker, None]
    mock_task_executor.run.return_value = "task_result_success"

    worker_task = asyncio.create_task(worker_with_mocks.start())
    await asyncio.sleep(0.1)  # Give time for job to be processed
    worker_with_mocks.running = False
    await asyncio.wait_for(worker_task, timeout=1.0)

    mock_task_executor.run.assert_awaited_once_with(
        worker_with_mocks.task_registry["my_worker_task"], "arg1", kwarg1="value1",
    )
    mock_broker.update_status.assert_awaited_once_with(
        sample_job_data_for_worker["id"], "success", result="task_result_success",
    )
    mock_broker.heartbeat.assert_awaited()  # Heartbeat should have been called at least once


@pytest.mark.asyncio
async def test_worker_handles_task_not_found(
    worker_with_mocks: Worker, mock_broker: AsyncMock, sample_job_data_for_worker: dict[str, Any],
) -> None:
    """
    Test that the worker handles cases where the task is not found in the registry.

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    sample_job_data_for_worker : dict[str, Any]
        Sample job data for worker processing.
    """
    sample_job_data_for_worker["task_name"] = "non_existent_task"
    # Make dequeue return the sample job data once, then fall back to return_value=None
    mock_broker.dequeue.side_effect = [sample_job_data_for_worker, None]

    worker_task = asyncio.create_task(worker_with_mocks.start())
    await asyncio.sleep(0.1)
    worker_with_mocks.running = False
    await asyncio.wait_for(worker_task, timeout=1.0)

    mock_broker.update_status.assert_awaited_once_with(
        sample_job_data_for_worker["id"], "failed", error="Task 'non_existent_task' not found in registry.",
    )
    mock_broker.heartbeat.assert_not_awaited()  # No heartbeat for non-existent task


@pytest.mark.asyncio
async def test_worker_handles_task_execution_failure(
    worker_with_mocks: Worker,
    mock_broker: AsyncMock,
    mock_task_executor: AsyncMock,
    sample_job_data_for_worker: dict[str, Any],
) -> None:
    """
    Test that the worker handles exceptions during task execution.

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    mock_task_executor : AsyncMock
        A mocked task executor instance.
    sample_job_data_for_worker : dict[str, Any]
        Sample job data for worker processing.
    """
    # Make dequeue return the sample job data once, then fall back to return_value=None
    mock_broker.dequeue.side_effect = [sample_job_data_for_worker, None]
    mock_task_executor.run.side_effect = ValueError("Simulated task error")

    worker_task = asyncio.create_task(worker_with_mocks.start())
    await asyncio.sleep(0.1)
    worker_with_mocks.running = False
    await asyncio.wait_for(worker_task, timeout=1.0)

    mock_broker.update_status.assert_awaited_once_with(
        sample_job_data_for_worker["id"], "failed", error="Simulated task error",
    )
    mock_broker.heartbeat.assert_awaited()  # Heartbeat should still run until task fails


@pytest.mark.asyncio
async def test_worker_handles_invalid_job_data(
    worker_with_mocks: Worker, mock_broker: AsyncMock, sample_job_data_for_worker: dict[str, Any],
) -> None:
    """
    Test that the worker handles invalid job data (e.g., malformed payload).

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    sample_job_data_for_worker : dict[str, Any]
        Sample job data for worker processing.
    """
    # Create a fresh copy for this test to avoid modifying the fixture's data
    invalid_job_data = sample_job_data_for_worker.copy()
    invalid_job_data["payload"] = "{invalid json"  # Malformed JSON

    # Make dequeue return the invalid job data once, then fall back to return_value=None
    mock_broker.dequeue.side_effect = [invalid_job_data, None]

    worker_task = asyncio.create_task(worker_with_mocks.start())
    await asyncio.sleep(0.1)
    worker_with_mocks.running = False
    await asyncio.wait_for(worker_task, timeout=1.0)

    mock_broker.update_status.assert_awaited_once()
    call_args, call_kwargs = mock_broker.update_status.call_args
    assert call_args[0] == invalid_job_data["id"]
    assert call_args[1] == "failed"
    expected_error_regex = r"Expecting property name enclosed in double quotes: line \d+ column \d+ \(char \d+\)"
    assert re.match(expected_error_regex, call_kwargs["error"])
    mock_broker.heartbeat.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_heartbeat_loop(worker_with_mocks: Worker, mock_broker: AsyncMock) -> None:
    """
    Test that the heartbeat loop sends heartbeats periodically.

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    """
    job_id = "heartbeat-job-id"

    # Mock asyncio.sleep inside the heartbeat loop to control its execution
    with patch("asyncio.sleep") as mock_sleep:
        # Make mock_sleep raise CancelledError after one "sleep" cycle
        mock_sleep.side_effect = [None, asyncio.CancelledError]  # Sleep once, then cancel

        heartbeat_task = asyncio.create_task(worker_with_mocks._heartbeat_loop(job_id))

        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass  # Expected

        assert mock_broker.heartbeat.await_count == 2
        mock_sleep.assert_awaited_with(10)  # Should have tried to sleep for 10s


@pytest.mark.asyncio
async def test_worker_loop_error_handling(worker_with_mocks: Worker, mock_broker: AsyncMock) -> None:
    """
    Test that the worker loop handles unexpected errors and raises WorkerError.

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    """
    # Make dequeue raise an exception immediately
    mock_broker.dequeue.side_effect = Exception("Simulated dequeue error")

    with pytest.raises(WorkerError, match="Worker loop encountered an unexpected error."):
        await worker_with_mocks.start()

    mock_broker.connect.assert_awaited_once()
    # Now, broker.close() *should* be called due to the top-level finally block
    mock_broker.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_uses_broker_wait_for_job_if_available(worker_with_mocks: Worker, mock_broker: AsyncMock) -> None:
    """
    Test that the worker calls broker.wait_for_job if it exists and dequeue returns None.

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    """
    # mock_broker.dequeue.return_value is already None by default from fixture
    mock_broker.wait_for_job = AsyncMock(return_value=None)

    worker_task = asyncio.create_task(worker_with_mocks.start())
    await asyncio.sleep(0.1)  # Allow worker to enter the loop and call wait_for_job
    worker_with_mocks.running = False
    await asyncio.wait_for(worker_task, timeout=1.0)

    mock_broker.wait_for_job.assert_awaited()
    assert mock_broker.wait_for_job.call_count >= 1  # It might be called multiple times before stopping


@pytest.mark.asyncio
async def test_worker_sleeps_if_no_wait_for_job_and_no_tasks(worker_with_mocks: Worker, mock_broker: AsyncMock) -> None:
    """
    Test that the worker sleeps if no tasks are available and broker has no wait_for_job.

    Parameters
    ----------
    worker_with_mocks : Worker
        A Worker instance configured with mocks.
    mock_broker : AsyncMock
        A mocked broker instance.
    """
    # Remove wait_for_job to simulate brokers without it (e.g., MemoryBroker)
    delattr(mock_broker, "wait_for_job")

    # Mock asyncio.sleep to avoid actual delays in test
    with patch("asyncio.sleep") as mock_sleep:
        # Make mock_sleep raise CancelledError after one "sleep" cycle
        mock_sleep.side_effect = [None, asyncio.CancelledError]  # Sleep once, then cancel

        worker_task = asyncio.create_task(worker_with_mocks.start())

        try:
            await worker_task
        except asyncio.CancelledError:
            pass  # Expected

        mock_broker.dequeue.assert_awaited()
        mock_sleep.assert_awaited_with(0.1)  # Should have tried to sleep for 0.1s
