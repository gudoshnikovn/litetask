# LiteTask

**A lightweight and versatile Python task queue library.**

LiteTask is designed to provide a simple yet powerful way to define, enqueue, and execute background tasks in Python applications. It aims for minimal dependencies and ease of use, currently supporting SQLite for persistent storage and an in-memory broker for testing or ephemeral tasks.

## Features

- **Simple Task Definition:** Easily turn any function into a background task using a decorator.
- **Asynchronous & Synchronous Enqueueing:** Enqueue tasks non-blocking (`.adelay()`) or blocking (`.delay()`) as needed.
- **Pluggable Brokers:**
  - **SQLite Broker:** Persistent task storage, ideal for production environments requiring durability without external services. Uses a dedicated writer thread for efficient database operations.
  - **Memory Broker:** In-memory task storage, perfect for testing, development, or short-lived tasks where persistence isn't required.
- **Robust Worker:** A dedicated worker process that fetches, executes, and manages the lifecycle of tasks.
- **Heartbeat Mechanism:** Workers send periodic heartbeats for long-running tasks to prevent them from being re-queued by other workers.
- **Error Handling:** Tasks can report success or failure, with error messages stored.
- **Minimal Dependencies:** Designed to be lightweight.

## Installation

_(For now, clone the repository and install dependencies manually. A `pyproject.toml` is present, so `pip install .` might work after cloning.)_

```bash
git clone https://github.com/gudoshnikovn/litetask.git
cd litetask
# If you have a pyproject.toml, you can install in editable mode:
pip install -e .
# Otherwise, ensure your Python environment is set up to run the scripts.
```

## Usage

LiteTask consists of three main parts: defining tasks, running a worker, and enqueueing tasks.

### 1. Define Your Tasks

Create a file (e.g., `examples/tasks.py`) where you define your LiteTask application and register your functions as tasks.

```python
# examples/tasks.py
import logging
import time

from litetask.app import LiteTask

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize the LiteTask application with a SQLite broker
# You can also use "memory://" for an in-memory broker
app = LiteTask("sqlite://litetask.db")


@app.task()
def shared_task(name: str, seconds: int) -> str:
    """
    A sample task that simulates work by sleeping for a given duration.
    """
    logger.info("[Worker] Started task for %s for %d seconds...", name, seconds)
    time.sleep(seconds)
    logger.info("[Worker] Task for %s completed!", name)
    return f"Result for {name}"
```

### 2. Run the Worker

Start a worker process that will listen for and execute tasks.

```python
# examples/worker_node.py
import asyncio
import logging

from examples.tasks import app
from litetask.core.exceptions import WorkerError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    try:
        # The worker is always asynchronous internally to efficiently wait for tasks.
        asyncio.run(app.run_worker())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
    except WorkerError:
        logger.error("Worker stopped due to a critical error.")
    except Exception:
        logger.exception("Worker encountered an unexpected error and stopped.")
```

To run the worker:

```bash
python examples/worker_node.py
```

### 3. Enqueue Tasks

You can enqueue tasks synchronously or asynchronously from your application.

#### Synchronous Enqueueing

```python
# examples/simple_sync.py
import logging
import time

from examples.tasks import shared_task

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("--- SYNCHRONOUS PRODUCER ---")

    # Call the task like a regular function using .delay()
    # This will instantly write the task to SQLite and proceed.
    task_id = shared_task.delay("SyncUser", 3)

    logger.info("Task sent! ID: %s", task_id)
    logger.info("I did not wait for execution and can do other things...")

    for i in range(3):
        logger.info("Synchronous code is working... %d", i)
        time.sleep(0.5)


if __name__ == "__main__":
    main()
```

To run the synchronous producer:

```bash
python examples/simple_sync.py
```

#### Asynchronous Enqueueing

```python
# examples/simple_async.py
import asyncio
import logging

from examples.tasks import shared_task

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("--- ASYNCHRONOUS PRODUCER ---")

    # Use await .adelay() to ensure the task is enqueued before proceeding.
    task_id = await shared_task.adelay("AsyncUser", 10)

    logger.info("Task sent asynchronously! ID: %s", task_id)

    # The producer can perform other asynchronous operations while the worker processes the task.
    # await asyncio.sleep(1)
    logger.info("Asynchronous code is not blocked...")


if __name__ == "__main__":
    asyncio.run(main())
```

To run the asynchronous producer:

```bash
python examples/simple_async.py
```

## Configuration

The broker is configured via a URL string passed to the `LiteTask` constructor:

- **SQLite:** `LiteTask("sqlite://path/to/your/database.db")`
  - If the path is omitted (e.g., `sqlite://`), it defaults to `tasks.db` in the current directory.
- **In-Memory:** `LiteTask("memory://")`

## Roadmap

- **Redis Broker:** Implement a Redis-based broker for high-performance, distributed task queues.
- **Task Scheduling:** Add support for delayed execution (`eta`) and periodic tasks.
- **Retries & Backoff:** Implement automatic task retries with configurable backoff strategies.
- **Monitoring & Admin UI:** Basic tools for monitoring task status and worker health.
- **Concurrency Control:** Mechanisms for limiting concurrent task execution.

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
