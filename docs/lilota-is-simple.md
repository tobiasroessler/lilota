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

The **schedule** function creates a task entry in the database and returns the ID
of the stored task. The task is not executed at once: a worker that is free picks it
up within 5 seconds. A worker that finds no task waits longer every time before it
looks again, up to **max_task_heartbeat_interval**. To shorten that, pass a smaller
**max_task_heartbeat_interval** to the **LilotaWorker** in your worker script.

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


## Number of workers

A **Lilota** instance starts one worker process, unless you tell it to start more:

``` python
lilota = Lilota(
  script_path="sample/myscript.py",
  number_of_workers=4
)
```

A worker executes one task at a time. **number_of_workers** is therefore the number of tasks that can be executed in parallel. With one worker, a task that takes ten minutes lets every other task wait for ten minutes.

Every worker is a Python process of its own, so more workers are not free:

* **Memory:** about 60 MB for a worker that waits for tasks, plus everything your worker script imports.
* **Database connections:** about 2 for every worker, plus 1 for the scheduler.
* **Database load:** every worker writes a heartbeat every 5 seconds, and looks for the next task at least every 5 seconds.

The workers are started by the process that creates the **Lilota** instance. A web server that runs several processes of your application starts them once for each of these processes. 4 application processes with **number_of_workers=4** are 16 worker processes and about 36 database connections. PostgreSQL allows 100 connections by default.

The number of workers cannot be greater than the number of CPU cores * 5.


### Upgrading from lilota 1.2.1 or older

Up to 1.2.1 a **Lilota** instance without **number_of_workers** started as many workers as the machine has CPU cores. That number was different on every machine, and in a container with a CPU limit it was still the number of cores of the host.

Now it starts one worker. If your application does not set **number_of_workers** and executes several tasks in parallel, set the number you need. Otherwise the tasks are executed one after the other.


## Communication between nodes

All nodes communicate through the shared database. If you do not specify a connection string, **lilota** will use **sqlite:///lilota.db** by default. This creates a SQLite database.

For production-ready applications, we recommend using a more advanced database that handles multiple processes efficiently. We recommend PostgreSQL. However, since **lilota** uses SQLAlchemy to access the database, any database supported by SQLAlchemy can be used.

For many applications this setup is sufficient. However, some scenarios require horizontal scaling. In these cases, multiple **Lilota** instances can run across several servers. They still connect to the same database.


## Clocks on several machines

Every node uses the clock of its own machine: when it stores the time it was last seen, when it sets the time at which a task expires, and when it compares such a time with the current time. Nodes on one machine share one clock. If your nodes run on several machines, the clocks of these machines have to be in sync, for example with NTP.

A difference of less than a second does not matter. A larger one does:

* The timeout of a task is set with the clock of the worker that executes it and checked with the clock of the leader. If the clock of the leader is ahead, a task is expired and retried before its timeout is over, while it is still being executed.
* A worker that is alive is taken for dead when its clock is behind the clock of the leader by about the node timeout, which is 20 seconds by default. Its tasks are given back and executed a second time. See [Worker maintenance](worker-maintenance.md).