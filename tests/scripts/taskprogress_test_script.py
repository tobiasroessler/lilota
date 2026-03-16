from lilota.models import TaskProgress
from lilota.worker import LilotaWorker
from typing import Any


class AddInput():
  def __init__(self, a: int, b: int) -> None:
    self.a = a
    self.b = b

  def as_dict(self) -> dict[str, Any]:
    return {
      "a": self.a,
      "b": self.b,
    }


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test",
  max_task_heartbeat_interval=0.1,
  set_progress_manually=True
)


@worker.register("only_taskprogress", task_progress=TaskProgress)
def only_taskprogress(task_progress: TaskProgress) -> None:
  task_progress.set(50)


@worker.register("add_with_taskprogress", input_model=AddInput, task_progress=TaskProgress)
def add_with_taskprogress(data: AddInput, task_progress: TaskProgress):
  task_progress.set(50)


def main():
  worker.start()


if __name__ == "__main__":
  main()