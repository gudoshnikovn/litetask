"""
Tests for the LiteTask core models, specifically the Job dataclass.
"""

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from litetask.core.models import Job


def test_job_initialization() -> None:
    """
    Test basic initialization of a Job object.
    """
    job = Job(task_name="my_task", args=[1, 2], kwargs={"a": 3})
    assert job.task_name == "my_task"
    assert job.args == [1, 2]
    assert job.kwargs == {"a": 3}
    assert isinstance(job.id, str)
    assert job.status == "pending"
    assert job.lease_expires_at is None
    assert job.heartbeat_at is None
    assert job.retries_count == 0
    assert job.uniqueness_key is None
    assert job.group_name is None
    assert job.run_at is None
    assert job.result is None
    assert job.error is None
    assert isinstance(job.created_at, datetime)
    assert job.created_at.tzinfo == timezone.utc


def test_job_from_row_full_data(sample_job_data: dict[str, Any]) -> None:
    """
    Test Job.from_row with a complete set of data, including ISO formatted datetimes.

    Parameters
    ----------
    sample_job_data : dict[str, Any]
        Sample job data in ISO format.
    """
    job = Job.from_row(sample_job_data)

    assert job.id == sample_job_data["id"]
    assert job.task_name == sample_job_data["task_name"]
    assert job.args == ["value1"]
    assert job.kwargs == {"key1": "value2"}
    assert job.status == sample_job_data["status"]
    assert isinstance(job.lease_expires_at, datetime)
    assert job.lease_expires_at.tzinfo == timezone.utc
    assert isinstance(job.heartbeat_at, datetime)
    assert job.heartbeat_at.tzinfo == timezone.utc
    assert isinstance(job.run_at, datetime)
    assert job.run_at.tzinfo == timezone.utc
    assert job.created_at.tzinfo == timezone.utc
    assert job.result is None
    assert job.error is None


def test_job_from_row_sqlite_datetime_format(sample_job_data_sqlite_format: dict[str, Any]) -> None:
    """
    Test Job.from_row with SQLite's typical datetime string format.

    Parameters
    ----------
    sample_job_data_sqlite_format : dict[str, Any]
        Sample job data in SQLite datetime string format.
    """
    job = Job.from_row(sample_job_data_sqlite_format)

    assert job.id == sample_job_data_sqlite_format["id"]
    assert job.task_name == sample_job_data_sqlite_format["task_name"]
    assert job.args == ["sqlite_val"]
    assert job.kwargs == {"sqlite_key": 1}
    assert job.status == sample_job_data_sqlite_format["status"]
    assert isinstance(job.lease_expires_at, datetime)
    assert job.lease_expires_at.tzinfo == timezone.utc
    assert isinstance(job.heartbeat_at, datetime)
    assert job.heartbeat_at.tzinfo == timezone.utc
    assert isinstance(job.run_at, datetime)
    assert job.run_at.tzinfo == timezone.utc
    assert job.created_at.tzinfo == timezone.utc
    assert job.result == "SQLite result"
    assert job.error is None


def test_job_from_row_sqlite_datetime_no_microseconds(sample_job_data_no_microseconds: dict[str, Any]) -> None:
    """
    Test Job.from_row with SQLite's typical datetime string format without microseconds.

    Parameters
    ----------
    sample_job_data_no_microseconds : dict[str, Any]
        Sample job data in SQLite datetime string format without microseconds.
    """
    job = Job.from_row(sample_job_data_no_microseconds)

    assert job.id == sample_job_data_no_microseconds["id"]
    assert job.task_name == sample_job_data_no_microseconds["task_name"]
    assert job.args == ["no_ms_val"]
    assert job.kwargs == {"no_ms_key": 2}
    assert job.status == sample_job_data_no_microseconds["status"]
    assert isinstance(job.lease_expires_at, datetime)
    assert job.lease_expires_at.tzinfo == timezone.utc
    assert isinstance(job.heartbeat_at, datetime)
    assert job.heartbeat_at.tzinfo == timezone.utc
    assert isinstance(job.run_at, datetime)
    assert job.run_at.tzinfo == timezone.utc
    assert job.created_at.tzinfo == timezone.utc
    assert job.result == "No microseconds result"
    assert job.error is None


def test_job_from_row_none_datetimes(sample_job_data_none_datetimes: dict[str, Any]) -> None:
    """
    Test Job.from_row with None values for datetime fields.

    Parameters
    ----------
    sample_job_data_none_datetimes : dict[str, Any]
        Sample job data with None datetime values.
    """
    job = Job.from_row(sample_job_data_none_datetimes)

    assert job.id == sample_job_data_none_datetimes["id"]
    assert job.task_name == sample_job_data_none_datetimes["task_name"]
    assert job.args == []
    assert job.kwargs == {}
    assert job.status == sample_job_data_none_datetimes["status"]
    assert job.lease_expires_at is None
    assert job.heartbeat_at is None
    assert job.run_at is None
    assert job.created_at.tzinfo == timezone.utc
    assert job.result is None
    assert job.error is None


def test_job_from_row_missing_optional_fields() -> None:
    """
    Test Job.from_row when some optional fields are missing from the row.
    """
    minimal_row = {
        "id": "minimal-id",
        "task_name": "minimal_task",
        "payload": json.dumps({"args": [], "kwargs": {}}),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    job = Job.from_row(minimal_row)

    assert job.id == "minimal-id"
    assert job.task_name == "minimal_task"
    assert job.status == "pending"  # Default value
    assert job.lease_expires_at is None
    assert job.heartbeat_at is None
    assert job.retries_count == 0
    assert job.uniqueness_key is None
    assert job.group_name is None
    assert job.run_at is None
    assert job.result is None
    assert job.error is None
    assert isinstance(job.created_at, datetime)


def test_job_from_row_with_result_and_error() -> None:
    """
    Test Job.from_row with result and error fields populated.
    """
    row_with_result_error = {
        "id": "result-error-id",
        "task_name": "test_task",
        "payload": json.dumps({"args": [], "kwargs": {}}),
        "status": "failed",
        "result": json.dumps({"data": "some_result"}),
        "error": "Task failed due to X",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    job = Job.from_row(row_with_result_error)

    assert job.status == "failed"
    assert job.result == {"data": "some_result"}
    assert job.error == "Task failed due to X"


def test_job_from_row_invalid_datetime_string() -> None:
    """
    Test Job.from_row with an invalid datetime string, expecting a ValueError.
    """
    invalid_row = {
        "id": "invalid-dt-id",
        "task_name": "invalid_dt_task",
        "payload": json.dumps({}),
        "created_at": "not-a-datetime-string",
    }
    with pytest.raises(ValueError, match=r"Failed to parse datetime string: 'not-a-datetime-string'"):
        Job.from_row(invalid_row)


def test_job_from_row_invalid_payload_json() -> None:
    """
    Test Job.from_row with invalid JSON in the payload, expecting JSONDecodeError.
    """
    invalid_payload_row = {
        "id": "invalid-payload-id",
        "task_name": "invalid_payload_task",
        "payload": "{invalid json",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    with pytest.raises(json.JSONDecodeError):
        Job.from_row(invalid_payload_row)


def test_job_from_row_invalid_result_json() -> None:
    """
    Test Job.from_row with invalid JSON in the result, expecting JSONDecodeError.
    """
    invalid_result_row = {
        "id": "invalid-result-id",
        "task_name": "invalid_result_task",
        "payload": json.dumps({}),
        "result": "{invalid json",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    with pytest.raises(json.JSONDecodeError):
        Job.from_row(invalid_result_row)
