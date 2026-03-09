# Cluster mode

## Introduction

While **Simple mode** runs a scheduler and a single worker inside one application process, some applications require **horizontal scaling** or **distributed execution**.

For this purpose, **lilota** provides the **Cluster mode**.

In **Cluster mode**, the scheduler and workers run as **separate processes**. Multiple workers can run on the same machine or across different machines. All nodes communicate through the shared database.

This allows tasks to be executed **in parallel and at scale**.

To better understand how this works, we will look at the main classes involved.


## LilotaScheduler.py (lilota.scheduler)

The class **LilotaScheduler.py** is responsible for scheduling tasks.

It performs tasks such as:

* inserting scheduled tasks into the database
* coordinating task scheduling
* managing cluster coordination

The scheduler **does not execute tasks**.

Typically, only **one scheduler** should run in a cluster.


## LilotaWorker.py (lilota.worker)

**LilotaWorker.py** provides workers that pick up scheduled tasks and execute them.

In **Cluster mode**, multiple workers can run simultaneously. Each worker independently polls the database for available tasks. When a worker retrieves a task, it locks the task, preventing other workers from executing it. This ensures that each task is executed exactly once.

Workers can run:

* on the same machine
* inside containers
* on different machines in a distributed system


## LilotaScheduler and LilotaWorker in Cluster mode

Unlike **Simple mode**, the scheduler and workers are started separately. This allows you to scale the system by:

* running **one scheduler**
* running **multiple workers**

All nodes communicate through the shared database.


## Example

### Scheduler

```python
from lilota.scheduler import LilotaScheduler

scheduler = LilotaScheduler(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)

scheduler.start()

# Schedule a task
scheduler.schedule("hello-world")
```


### Worker

```python
from lilota.worker import LilotaWorker


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)


@worker.register("hello-world")
def hello_world():
  print("Hello World")


worker.start()
```

Multiple workers can run at the same time:

```
python worker1.py
python worker2.py
python worker3.py
```

Each worker will automatically pick up tasks from the database.


## When to use Cluster mode

You should use Cluster mode when:

* tasks need to run in parallel
* tasks are CPU intensive
* tasks should run on multiple machines
* the application must scale horizontally

For small applications or simple setups, **Simple mode** is usually sufficient.


## Simple mode vs Cluster mode

| Feature   | Simple mode | Cluster mode |
| --------- | ----------- | ------------ |
| Scheduler | 1           | 1            |
| Workers   | 1           | many         |
| Processes | 1           | multiple     |
| Machines  | 1           | many         |
| Scaling   | limited     | horizontal   |
