from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker
import sys
from typing import Any


worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


# The test passes in what to write, as {"line": ..., "count": ...}, so that it knows
# what has to arrive.


@worker.task
def write_to_stdout(data: dict[str, Any]) -> None:
    for _ in range(data["count"]):
        print(data["line"])


@worker.task
def write_to_stderr(data: dict[str, Any]) -> None:
    for _ in range(data["count"]):
        print(data["line"], file=sys.stderr)


def main():
    worker.start()


if __name__ == "__main__":
    main()
