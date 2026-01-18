import asyncio
import logging

from litetask.app import LiteTask

# Configure basic logging for better visibility
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    """
    Demonstrates a simple in-memory LiteTask application.

    This example runs a task and its worker within the same process,
    using the MemoryBroker for non-persistent task management.
    """
    logger.info("Starting LiteTask MemoryBroker example...")

    # 1. Initialize LiteTask with MemoryBroker
    # The 'memory://' URL indicates an in-memory broker.
    app = LiteTask("memory://")

    # 2. Define a simple asynchronous task
    @app.task()
    async def greet_user(name: str, greeting: str = "Hello") -> str:
        """
        A simple asynchronous task that returns a greeting.

        Parameters
        ----------
        name : str
            The name of the person to greet.
        greeting : str, optional
            The greeting message. Defaults to "Hello".

        Returns
        -------
        str
            The complete greeting message.
        """
        logger.info("Executing greet_user task for %s with greeting '%s'", name, greeting)
        await asyncio.sleep(0.1)  # Simulate some asynchronous work
        return f"{greeting}, {name}!"

    # 3. Enqueue the task asynchronously
    logger.info("Enqueuing task 'greet_user'...")
    job_id = await greet_user.adelay("Alice", greeting="Hi")
    logger.info("Task enqueued with Job ID: %s", job_id)

    # 4. Run the worker in the same process
    # For MemoryBroker, the worker typically runs in the same event loop
    # and processes tasks directly from the in-memory queue.
    # We create a task for the worker and let it run briefly.
    worker_task = asyncio.create_task(app.run_worker())

    # 5. Wait for the job to complete and retrieve its result
    result = None
    for _ in range(10):  # Poll for result for up to 1 second (10 * 0.1s)
        await asyncio.sleep(0.1)
        # For MemoryBroker, we can directly access the jobs dictionary for simplicity
        job = app.broker.jobs.get(job_id)
        if job and job.status in ("success", "failed"):
            result = job.result
            break

    # 6. Stop the worker gracefully
    app.worker.running = False
    await worker_task  # Wait for the worker to finish its shutdown process

    if result:
        logger.info("Task %s completed. Result: %s", job_id, result)
        assert result == "Hi, Alice!"
    else:
        logger.error("Task %s did not complete in time or failed.", job_id)

    logger.info("LiteTask MemoryBroker example finished.")


if __name__ == "__main__":
    # Run the main asynchronous function
    asyncio.run(main())
