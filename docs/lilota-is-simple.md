# Lilota is simple


## Introduction

**lilota** aims to be simple. To understand what this means in practice, it helps to briefly look at the main classes involved.

Lilota is built around three concepts:
1. Schedule tasks
2. Run workers
3. Store state in a database

Most applications only need a single `Lilota` instance to handle all three.


## Lilota (lilota.core)

[**Lilota**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.core.Lilota) is the main entry point for applications using Lilota. It allows tasks to be scheduled and workers to be started to execute those tasks.

Internally, it creates a [**LilotaScheduler**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.scheduler.LilotaScheduler) instance and a worker manager responsible for starting worker processes. Each process runs a script that creates an instance of [**LilotaWorker**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.worker.LilotaWorker).

Most applications only need the **Lilota** class. The scheduler and worker classes are described below for advanced deployment scenarios.

**Create a lilota instance:**

``` python
lilota = Lilota(
  script_path="sample/myscript.py"
)
```

Here, **myscript.py** is the worker script. The worker script registers the task functions that workers are allowed to execute. 

The script is executed by worker processes managed by a worker manager. When a **Lilota** instance is created, the worker manager is automatically configured and can be started with `lilota.start()`.

 An example of such a script can be found [here](https://github.com/tobiasroessler/lilota-sample/blob/main/src/1-hello-world/workerscript.py).

**Start lilota:**

``` python
lilota.start()
```

This starts the scheduler and the worker manager.

**Schedule a task:**

``` python
task_id = lilota.schedule("add", AddInput(a=2, b=3))
```

The **schedule** function creates a task entry in the database and starts
executing it immediately. The ID of the stored task is returned.

**Retrieve task information including the output (if available):**

``` python
task: Task = lilota.get_task_by_id(task_id)
add_output = AddOutput(**task.output)
print(add_output.sum)
```


## LilotaScheduler (lilota.scheduler)

In some applications, it is useful to separate task scheduling from worker management. Worker processes can then be managed by a separate application.

**Create a scheduler:**

``` python
from lilota.core import Lilota

scheduler: LilotaScheduler = Lilota.scheduler()
```


## Workers

If you need a separate application to manage worker processes, you can create a worker manager directly:

``` python
from lilota.core import Lilota

worker_manager = Lilota.workers(
  script_path=str(Path(__file__).resolve().parent / "workerscript.py")
)
```


### Full example

A full example using the class methods can be found here:
[7-using-factory-methods](https://github.com/tobiasroessler/lilota-sample/blob/main/src/7-using-factory-methods)


## LilotaWorker (lilota.worker)

[**LilotaWorker**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.worker.LilotaWorker) implements the worker responsible for retrieving scheduled tasks and executing them.

Multiple workers can run simultaneously. Each worker independently polls the database for available tasks. When a worker retrieves a task, it locks the task, preventing other workers from executing it. This ensures that each task is executed exactly once.

Workers can run:

* on the same machine
* inside containers
* on different machines in a distributed system


## Communication between nodes

All nodes communicate through the shared database. If you do not specify a connection string, **lilota** will use **sqlite:///lilota.db** by default. This creates a SQLite database.

For production-ready applications, we recommend using a more advanced database that handles multiple processes efficiently. We recommend PostgreSQL. However, since **lilota** uses SQLAlchemy to access the database, any database supported by SQLAlchemy can be used.

For many applications this setup is sufficient. However, some scenarios require horizontal scaling. In these cases, multiple **Lilota** instances can run across several servers. They still connect to the same database.