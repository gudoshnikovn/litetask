"""
Data models for LiteTask.

This module defines the `Job` dataclass, which represents a single task
within the LiteTask system, including its metadata, status, and payload.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class Job:
    """
    Represents a single task (job) within the LiteTask system.

    This dataclass holds all relevant information about a task, including
    its name, arguments, status, and scheduling details.

    Attributes
    ----------
    task_name : str
        The name of the task function to be executed.
    args : list[Any]
        Positional arguments for the task function. Defaults to an empty list.
    kwargs : dict[str, Any]
        Keyword arguments for the task function. Defaults to an empty dictionary.
    id : str
        A unique identifier for the job. Defaults to a new UUID.
    status : str
        The current status of the job (e.g., "pending", "running", "success", "failed").
        Defaults to "pending".
    lease_expires_at : datetime | None
        Timestamp when the job's lease expires, indicating when it can be
        re-queued if not completed. Defaults to None.
    heartbeat_at : datetime | None
        Timestamp of the last heartbeat received for a running job. Defaults to None.
    retries_count : int
        The number of times the job has been retried. Defaults to 0.
    run_at : datetime | None
        The earliest time the job should be executed. Defaults to None.
    uniqueness_key : str | None
        A key to ensure only one instance of a specific task runs at a time.
        Defaults to None.
    group_name : str | None
        A name to group related tasks. Defaults to None.
    result : Any
        The result of the task execution if successful. Defaults to None.
    error : str | None
        An error message if the task failed. Defaults to None.
    created_at : datetime
        The timestamp when the job was created. Defaults to the current UTC time.
    """

    task_name: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid4()))
    status: str = "pending"  # pending, running, success, failed

    # Reliability
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
    retries_count: int = 0

    # Scheduling & Meta
    run_at: datetime | None = None
    uniqueness_key: str | None = None
    group_name: str | None = None

    result: Any = None
    error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Job":
        """
        Parses a dictionary row (e.g., from SQLite `row_factory=sqlite3.Row`)
        into a Job object.

        This method attempts to parse datetime strings from various common formats,
        including ISO 8601 and SQLite's DATETIME('NOW', 'UTC') format. It also
        handles JSON deserialization for payload and result fields.

        Parameters
        ----------
        row : dict[str, Any]
            A dictionary representing a job row from the database.

        Returns
        -------
        Job
            A Job instance populated with data from the row.
        """
        payload_data = json.loads(row.get("payload", "{}"))

        def parse_datetime(dt_val: str | datetime | None) -> datetime | None:
            if isinstance(dt_val, datetime):
                return dt_val.astimezone(timezone.utc) if dt_val.tzinfo is None else dt_val
            if isinstance(dt_val, str):
                try:
                    # Try parsing ISO 8601 with T and Z/offset first (from Python's isoformat)
                    return datetime.fromisoformat(dt_val)
                except ValueError:
                    # If that fails, try parsing SQLite's DATETIME('NOW', 'UTC') format
                    # which is 'YYYY-MM-DD HH:MM:SS.SSS' (naive, then make UTC)
                    try:
                        dt_obj = datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S.%f")
                        return dt_obj.replace(tzinfo=timezone.utc)
                    except ValueError:
                        try:
                            # Try parsing without microseconds
                            dt_obj = datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S")
                            return dt_obj.replace(tzinfo=timezone.utc)
                        except ValueError:
                            logger.error("Failed to parse datetime string: '%s'", dt_val)
                            raise  # Re-raise to show the original error
            return None

        return cls(
            id=row["id"],
            task_name=row["task_name"],
            args=payload_data.get("args", []),
            kwargs=payload_data.get("kwargs", {}),
            status=row.get("status", "pending"),
            lease_expires_at=parse_datetime(row.get("lease_expires_at")),
            heartbeat_at=parse_datetime(row.get("heartbeat_at")),
            retries_count=row.get("retries_count", 0),
            uniqueness_key=row.get("uniqueness_key"),
            group_name=row.get("group_name"),
            run_at=parse_datetime(row.get("run_at")),
            result=json.loads(row["result"]) if row.get("result") else None,
            error=row.get("error"),
            created_at=parse_datetime(row.get("created_at")) or datetime.now(timezone.utc),
        )
