# Lilota is simple


## Introduction

**lilota** aims to be simple. To understand what this means in practice, it helps to briefly look at the main classes involved.


## Lilota (lilota.core)

The class [**Lilota**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.core.Lilota) is the main entry point used by the user. It allows tasks to be scheduled and workers to be started to execute those tasks.

Internally, it creates a [**LilotaScheduler**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.scheduler.LilotaScheduler) instance and starts one or more worker processes. Each process runs a script that creates an instance of [**LilotaWorker**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.worker.LilotaWorker).


## LilotaWorker (lilota.worker)

[**LilotaWorker**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.worker.LilotaWorker) implements the worker responsible for retrieving scheduled tasks and executing them.

Multiple workers can run simultaneously. Each worker independently polls the database for available tasks. When a worker retrieves a task, it locks the task, preventing other workers from executing it. This ensures that each task is executed exactly once.

Workers can run:

* on the same machine
* inside containers
* on different machines in a distributed system


## Communication between nodes

All nodes communicate through the shared database.

For many applications this setup is sufficient. However, some scenarios require horizontal scaling. In these cases, multiple **lilota** instances can run across several servers. They still connect then to the same database.