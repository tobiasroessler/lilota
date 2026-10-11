from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker


# Dead workers are found by the heartbeat of the worker that is the leader, once its
# own heartbeats have reached the database for as long as the node timeout. A short
# interval and a short timeout keep the test fast.
worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval=0.5,
    node_heartbeat_interval_jitter=None,
    node_timeout_sec=1,
    max_task_heartbeat_interval=0.1,
)


@worker.task
def with_timeout():
    pass


@worker.task(timeout=None)
def without_timeout():
    pass


def main():
    worker.start()


if __name__ == "__main__":
    main()
