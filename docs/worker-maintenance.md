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

The leader only looks for dead nodes when its own heartbeats have reached the database without a gap for as long as the node timeout. If the database could not be reached, or the whole machine stood still, the heartbeats of all workers are missing, and a worker that is alive must not be taken for dead. A worker that has just started therefore finds dead nodes after the node timeout at the earliest.


### 2. Tasks of Dead Nodes

A worker that dies cannot finish the tasks it has taken, and no other worker takes a task that belongs to another worker. The leader therefore gives these tasks back.

* The task store looks for tasks that were taken or are being executed by a worker that is dead
* These tasks get the status **created** again, as if no worker had taken them
* The next worker that is free executes them

This is not counted as an attempt. The task is also executed again with the default **max_attempts** of 1.

A worker counts as dead when it has not sent a heartbeat within the node timeout, which is 20 seconds by default (**node_timeout_sec**). A task of a dead worker is therefore executed again after about that time.

The leader writes a warning to the **lilota_log** table:

```
Gave back 1 task(s) of dead node(s). They are executed again
```

> **Note:** A task that is given back was ended in the middle and is executed again from the beginning. What it has done before, it does twice. The same happens if a worker is taken for dead although it is only not reachable for longer than the node timeout: its task is then executed by a second worker as well.

A task that has run into its timeout is not given back. It is handled as described in the next two sections.


### 3. Expiration of Overdue Tasks

Tasks that exceed their configured timeout are automatically marked as **expired**.

* The task store checks for running tasks whose expiration time has passed
* These tasks are updated to the **expired** state

This prevents tasks from running indefinitely and ensures system stability.

More information can be found [here](https://tobiasroessler.github.io/lilota/task-timeout).


### 4. Retry of Tasks

Tasks that have failed or expired are retried if they have attempts left.

* The task store looks for **failed** and **expired** tasks that were not retried yet
* For each of them a new task is created that is executed after a delay

More information can be found [here](https://tobiasroessler.github.io/lilota/retry-mechanism).


## Fault Tolerance

Maintenance execution is designed to be safe and resilient:

* If maintenance fails, the error is logged but does not stop the worker
* The heartbeat process continues running regardless of failures
* Leadership is automatically renewed or reassigned if a leader becomes unavailable

