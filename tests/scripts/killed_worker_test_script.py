from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker
from datetime import timedelta
import sys
import time


worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


@worker.task
def write_warning():
    print("Warning written by a lilota task", file=sys.stderr)


@worker.task(timeout=timedelta(seconds=1))
def infinite_loop():
    while True:
        time.sleep(0.1)


def main():
    worker.start()


if __name__ == "__main__":
    main()
