# Simple mode


## Introduction

**lilota** aims to be simple. To achieve this, it provides the **Simple mode**.
To better understand what this means, we need to briefly look at the main classes involved.


## LilotaScheduler.py (lilota.scheduler)

The class [**LilotaScheduler.py**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.scheduler.LilotaScheduler) is used by the user to schedule tasks.
It does not execute tasks. Task execution can only be performed by a worker.


## LilotaWorker.py (lilota.worker)

[**LilotaWorker.py**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.worker.LilotaWorker) provides a worker that picks up scheduled tasks and executes them.
For each instance of this class, one worker is started that runs in the background when using **Simple mode**.


## Lilota.py (lilota.core)

[**Lilota.py**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.core.Lilota) is the heart of **Simple mode**, but it acts only as a facade. Internally, it creates one instance of [**LilotaScheduler.py**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.scheduler.LilotaScheduler) and one instance of [**LilotaWorker.py**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.worker.LilotaWorker).

This means that tasks scheduled by the scheduler are executed by a **single worker running in the background**.

For some applications this is already sufficient. However, there are scenarios where an application needs to scale. For this purpose, lilota provides the [**Cluster mode**](https://tobiasroessler.github.io/lilota/cluster-mode/).


## Example

``` python
from lilota.core import Lilota
from lilota.models import Task


# Create a Lilota instance
lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)


# Register a task
@lilota.register("hello-world")
def hello_world() -> None:
  print("Hello World")


def main():
  # Start scheduler and worker (Simple mode)
  lilota.start()

  # Schedule the task
  task_id = lilota.schedule("hello-world")

  # Retrieve the task information
  task: Task = lilota.get_task_by_id(task_id)
  print(task)


if __name__ == "__main__":
  main()
```