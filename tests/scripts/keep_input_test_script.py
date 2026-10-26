from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker


# Retries are created by the worker's heartbeat. A short interval keeps the test fast.
worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval=0.5,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


@worker.task(keep_input=False)
def send_secret(data: dict) -> None:
    pass


@worker.task
def send_public(data: dict) -> None:
    pass


@worker.task(keep_input=False, max_attempts=2)
def fail_secret(data: dict) -> None:
    raise Exception("Error")


def main():
    worker.start()


if __name__ == "__main__":
    main()
