from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.models import TaskContext
from lilota.worker import LilotaWorker
from typing import Any


class AddInput:
    def __init__(self, a: int, b: int) -> None:
        self.a = a
        self.b = b

    def as_dict(self) -> dict[str, Any]:
        return {
            "a": self.a,
            "b": self.b,
        }


worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


@worker.task
def only_taskprogress(task_context: TaskContext) -> None:
    task_context.progress.set(50)


@worker.task
def add_with_taskprogress(data: AddInput, task_context: TaskContext):
    task_context.progress.set(50)


def main():
    worker.start()


if __name__ == "__main__":
    main()
