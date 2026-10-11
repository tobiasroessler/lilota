from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker
import subprocess
import sys


worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


# The task does not wait for the process it starts, so that the test gets the process
# ID as the output of the task while the process is still running.


@worker.task
def start_subprocess() -> dict[str, int]:
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    return {"pid": process.pid}


def main():
    worker.start()


if __name__ == "__main__":
    main()
