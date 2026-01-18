"""
SQLite broker implementation for LiteTask.

This module provides a `SQLiteBroker` that uses an SQLite database for
persistent task storage and management. It supports both synchronous and
asynchronous enqueueing and uses a dedicated writer thread for database operations.
"""

import asyncio
import json
import logging
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Queue as SyncQueue
from typing import Any, final

from litetask.broker.base import Broker
from litetask.core.models import Job

logger = logging.getLogger(__name__)


@final
class SQLiteBroker(Broker):
    """
    A SQLite-based broker implementation for LiteTask.

    Manages task persistence using an SQLite database. It uses a separate
    writer thread to handle database operations, allowing for non-blocking
    asynchronous enqueueing and worker operations.

    Parameters
    ----------
    db_path : str
        The path to the SQLite database file.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path: Path = Path(db_path).resolve()
        self._write_queue: SyncQueue[
            tuple[str, tuple[Any, ...], asyncio.Future[Any] | None, asyncio.AbstractEventLoop | None] | None
        ] = SyncQueue()
        self._new_job_event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._writer_thread: threading.Thread | None = None
        self._running: bool = False
        self._lock = threading.Lock()

    def _ensure_connection(self) -> None:
        """
        Ensures the writer thread is running and the database is initialized.

        This method is called before any database operation to guarantee
        the broker's readiness.
        """
        with self._lock:
            if self._running:
                return

            self._running = True
            self._writer_thread = threading.Thread(target=self._writer_worker, daemon=True)
            self._writer_thread.start()
            self._init_db()
            logger.info("SQLiteBroker writer thread started and database initialized.")

    async def connect(self) -> None:
        """
        Initializes the SQLite broker.

        Ensures the database connection and tables are set up.
        """
        self._ensure_connection()
        logger.debug("SQLiteBroker connected.")

    async def close(self) -> None:
        """
        Closes the SQLite broker.

        Stops the writer thread and waits for it to terminate.
        """
        self._running = False
        self._write_queue.put(None)  # Poison pill to stop the writer thread
        if self._writer_thread:
            self._writer_thread.join(timeout=5.0)
            if self._writer_thread.is_alive():
                logger.warning("SQLiteBroker writer thread did not terminate gracefully.")
            else:
                logger.info("SQLiteBroker writer thread stopped.")
        logger.debug("SQLiteBroker closed.")

    def _init_db(self) -> None:
        """
        Initializes the SQLite database schema.

        Creates the database directory if it doesn't exist and sets up
        the `litetask_jobs` table with necessary indices.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS litetask_jobs (
                    id TEXT PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    payload TEXT,
                    status TEXT DEFAULT 'pending',
                    lease_expires_at TIMESTAMP,
                    heartbeat_at TIMESTAMP,
                    retries_count INTEGER DEFAULT 0,
                    uniqueness_key TEXT,
                    group_name TEXT,
                    run_at TIMESTAMP,
                    result TEXT,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status_run ON litetask_jobs (status, run_at);")
            logger.info("SQLite database schema initialized at %s.", self.db_path)

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Registers the worker's asyncio event loop.

        This allows the broker to notify the worker about new jobs from
        synchronous enqueue operations.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop of the worker.
        """
        self._loop = loop
        self._new_job_event = asyncio.Event()
        logger.debug("Worker event loop registered with SQLiteBroker.")

    def _writer_worker(self) -> None:
        """
        The target function for the dedicated writer thread.

        This thread continuously pulls database operations from `_write_queue`
        and executes them, then sets the result on the corresponding future.
        """
        conn = sqlite3.connect(self.db_path, isolation_level=None, check_same_thread=False)
        while self._running:
            item = self._write_queue.get()
            if item is None:  # Poison pill
                break
            sql, params, future, loop = item
            try:
                cursor = conn.execute(sql, params)
                res: Any
                if sql.strip().upper().startswith("SELECT") or "RETURNING" in sql.upper():
                    res = cursor.fetchall()
                else:
                    res = cursor.lastrowid
                if future and loop:
                    loop.call_soon_threadsafe(future.set_result, res)
            except Exception as e:
                logger.exception("Error in SQLite writer thread executing SQL: %s", sql)
                if future and loop:
                    loop.call_soon_threadsafe(future.set_exception, e)
            finally:
                self._write_queue.task_done()
        conn.close()
        logger.debug("SQLite writer thread stopped.")

    def _notify_worker(self) -> None:
        """
        Notifies the worker about new jobs if its event loop is registered.

        This wakes up the worker if it's waiting for new tasks.
        """
        if self._loop and self._new_job_event:
            self._loop.call_soon_threadsafe(self._new_job_event.set)
            logger.debug("Worker notified about new job.")

    async def enqueue(self, job: Job) -> str:
        """
        Asynchronously enqueues a job into the SQLite broker.

        The job is added to an internal queue for the writer thread to process.

        Parameters
        ----------
        job : Job
            The job object to enqueue.

        Returns
        -------
        str
            The ID of the enqueued job.
        """
        self._ensure_connection()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        self._put_in_queue(job, fut, loop)
        await fut
        self._notify_worker()
        logger.debug("Job %s enqueued asynchronously.", job.id)
        return job.id

    def enqueue_sync(self, job: Job) -> str:
        """
        Synchronously enqueues a job into the SQLite broker.

        The job is added to an internal queue for the writer thread to process.
        This method does not wait for the database write to complete.

        Parameters
        ----------
        job : Job
            The job object to enqueue.

        Returns
        -------
        str
            The ID of the enqueued job.
        """
        self._ensure_connection()
        self._put_in_queue(job, None, None)
        self._notify_worker()
        logger.debug("Job %s enqueued synchronously.", job.id)
        return job.id

    def _put_in_queue(self, job: Job, fut: asyncio.Future[Any] | None, loop: asyncio.AbstractEventLoop | None) -> None:
        """
        Puts a job into the writer thread's queue for database insertion.

        Parameters
        ----------
        job : Job
            The job object to insert.
        fut : asyncio.Future[Any] | None
            The future to set the result on, if an asynchronous operation.
        loop : asyncio.AbstractEventLoop | None
            The event loop associated with the future.
        """
        payload = json.dumps({"args": job.args, "kwargs": job.kwargs})
        sql = "INSERT INTO litetask_jobs (id, task_name, payload, run_at, created_at) VALUES (?, ?, ?, ?, ?)"

        # SQLite handles datetime objects directly for TIMESTAMP columns
        run_at_dt = job.run_at or datetime.now(timezone.utc)

        self._write_queue.put((sql, (job.id, job.task_name, payload, run_at_dt, job.created_at), fut, loop))

    async def wait_for_job(self) -> None:
        """
        Waits for a notification that a new job has been enqueued.

        This method is specific to the SQLite broker and helps the worker
        efficiently wait for new tasks without busy-waiting.
        """
        if not self._new_job_event:
            await asyncio.sleep(1)  # Fallback sleep if event is not set
            return
        try:
            await asyncio.wait_for(self._new_job_event.wait(), timeout=5.0)
            logger.debug("Received notification for new job.")
        except asyncio.TimeoutError:
            pass  # Timeout is expected if no new jobs arrive
        finally:
            self._new_job_event.clear()

    async def dequeue(self) -> dict[str, Any] | None:
        """
        Asynchronously dequeues a job from the SQLite broker.

        It attempts to find a pending job, updates its status to 'running',
        sets a lease expiration, and returns its details.

        Returns
        -------
        dict[str, Any] | None
            A dictionary containing the job's 'id', 'task_name', 'payload',
            'lease_expires_at', and 'heartbeat_at', or None if no job is available.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        # Selects a pending job, updates its status to 'running', and sets lease_expires_at.
        # RETURNING clause fetches the updated job details.
        lease_expires_at = datetime.now(timezone.utc) + timedelta(seconds=30)
        heartbeat_at = datetime.now(timezone.utc)

        sql = """
            UPDATE litetask_jobs
            SET status='running',
                lease_expires_at=?,
                heartbeat_at=?
            WHERE id=(
                SELECT id FROM litetask_jobs
                WHERE status='pending' AND (run_at IS NULL OR run_at <= ?)
                ORDER BY created_at ASC
                LIMIT 1
            )
            RETURNING id, task_name, payload, lease_expires_at, heartbeat_at,
                      status, retries_count, uniqueness_key, group_name, run_at,
                      result, error, created_at;
        """
        self._write_queue.put((sql, (lease_expires_at, heartbeat_at, datetime.now(timezone.utc)), fut, loop))
        rows = await fut
        if rows:
            job_data = rows[0]
            # Map the returned row to a dictionary for Job.from_row
            columns = [
                "id",
                "task_name",
                "payload",
                "lease_expires_at",
                "heartbeat_at",
                "status",
                "retries_count",
                "uniqueness_key",
                "group_name",
                "run_at",
                "result",
                "error",
                "created_at",
            ]
            result_dict = dict(zip(columns, job_data))
            logger.debug("Job %s dequeued.", result_dict["id"])
            return result_dict
        return None

    async def update_status(self, job_id: str, status: str, result: Any = None, error: str | None = None) -> None:
        """
        Asynchronously updates the status of a job in the SQLite broker.

        Parameters
        ----------
        job_id : str
            The ID of the job to update.
        status : str
            The new status of the job.
        result : Any, optional
            The result of the job execution. Defaults to None.
        error : str | None, optional
            An error message if the job failed. Defaults to None.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        sql = "UPDATE litetask_jobs SET status=?, result=?, error=? WHERE id=?"
        result_json = json.dumps(result) if result is not None else None
        self._write_queue.put((sql, (status, result_json, error, job_id), fut, loop))
        await fut
        logger.debug("Job %s status updated to %s.", job_id, status)

    async def heartbeat(self, job_id: str) -> None:
        """
        Asynchronously sends a heartbeat for a running job in the SQLite broker.

        Updates the `lease_expires_at` and `heartbeat_at` fields for the specified job.

        Parameters
        ----------
        job_id : str
            The ID of the job for which to send a heartbeat.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        lease_expires_at = datetime.now(timezone.utc) + timedelta(seconds=30)
        heartbeat_at = datetime.now(timezone.utc)
        sql = "UPDATE litetask_jobs SET lease_expires_at = ?, heartbeat_at = ? WHERE id = ?"
        self._write_queue.put((sql, (lease_expires_at, heartbeat_at, job_id), fut, loop))
        await fut
        logger.debug("Heartbeat sent for job %s.", job_id)
