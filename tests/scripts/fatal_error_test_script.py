from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker
import os
from pathlib import Path


# A worker script with an error that is repaired later: it fails for as long as the
# file exists that the test names in this environment variable.
failure_file = os.environ.get("LILOTA_TEST_FAILURE_FILE")
if failure_file and Path(failure_file).exists():
    raise Exception("Error in script")


worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


@worker.task
def hello_world():
    print("Hello World")


def main():
    worker.start()


if __name__ == "__main__":
    main()
