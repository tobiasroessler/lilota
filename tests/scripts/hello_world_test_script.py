from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker


worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


@worker.task
def hello_world():
    print("Hello Word")


def main():
    worker.start()


if __name__ == "__main__":
    main()
