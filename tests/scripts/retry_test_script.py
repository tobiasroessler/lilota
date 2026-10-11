from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker


# Retries are created by the worker's heartbeat. A short interval keeps the test fast.
worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval=0.5,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


@worker.task(max_attempts=3)
def always_failing():
    raise Exception("Error")


@worker.task
def always_failing_with_default_max_attempts():
    raise Exception("Error")


def main():
    worker.start()


if __name__ == "__main__":
    main()
