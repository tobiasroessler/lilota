# Worker Maintenance


## Introduction

In a distributed environment, worker nodes must regularly perform maintenance tasks to ensure the system remains healthy and consistent. In **lilota**, this responsibility is handled automatically through the worker heartbeat mechanism and a lightweight leader election process.


## How Maintenance Works

Each worker periodically sends a heartbeat to indicate that it is alive. During this process, workers also participate in a **leader election**. At any given time, exactly one worker acts as the leader.

Only the elected leader performs maintenance tasks. This avoids duplicate work and ensures consistent system state across all workers.


## Maintenance Tasks

When a worker becomes the leader, it executes the following maintenance tasks:

### 1. Cleanup of Dead Nodes

Workers that have not sent a heartbeat within a configured timeout are considered **dead**.

* A cutoff timestamp is calculated based on the configured node timeout
* All nodes with a `last_seen_at` older than this cutoff are marked as **dead**
* The current worker (leader) is excluded from this process

This ensures that inactive or crashed workers do not remain in an inconsistent state.


### 2. Expiration of Overdue Tasks

Tasks that exceed their configured timeout are automatically marked as **expired**.

* The task store checks for running tasks whose expiration time has passed
* These tasks are updated to the **expired** state

This prevents tasks from running indefinitely and ensures system stability.

More information can be found [here](https://tobiasroessler.github.io/lilota/task-timeout).


## Fault Tolerance

Maintenance execution is designed to be safe and resilient:

* If maintenance fails, the error is logged but does not stop the worker
* The heartbeat process continues running regardless of failures
* Leadership is automatically renewed or reassigned if a leader becomes unavailable

