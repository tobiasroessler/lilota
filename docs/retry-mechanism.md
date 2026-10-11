# Retry mechanism

The **Retry mechanism** is responsible for identifying failed or expired tasks and scheduling retries in a safe, concurrent environment. It ensures that:

* Tasks are retried only if eligible
* Duplicate retries are prevented
* Concurrency conflicts are handled via optimistic locking
* Retry attempts are limited and delayed progressively

In **lilota**, you have to specify the max attempts for each task. By default it is set to 1.

The max attempts can be set when registering a task.

``` python
worker = LilotaWorker(
  ...
)

@worker.task(max_attempts=5)
```


## What max_attempts means

**max_attempts** is how often a task is executed at most. The first execution counts.

| max_attempts  | A task that fails every time                              |
| ------------- | --------------------------------------------------------- |
| 1 (default)   | is executed once and is not retried.                      |
| 3             | is executed three times: once, followed by two retries.   |


## When a task is retried

A task is retried if it has attempts left and

* its function raised an exception (status **failed**), or
* it ran longer than its timeout (status **expired**, see [Task timeout](task-timeout.md)).

A task function that raises an exception does not stop the worker. The worker stores the exception in the **error** column of the task, sets its status to **failed** and goes on with the next task.


## How a task is retried

A retry is a new task: a new row in **lilota_task** with the same name and the same input.

* Its **attempts** column counts one further. The task you scheduled is attempt 1, its first retry is attempt 2.
* Its **previous_task_id** column is the ID of the task it retries.
* The task that failed keeps its status and its error, and its **retried_at** column says when the retry was created.

The ID returned by **schedule** therefore always is the ID of the first attempt.

A retry does not start at once. It waits 5 seconds for every attempt that has been made: 5 seconds before the second attempt, 10 seconds before the third.

Retries are created by the worker that currently is the leader, with its heartbeat (see [Worker maintenance](worker-maintenance.md)). This adds a few seconds. It takes longer if the worker that was stopped was the leader itself, because another worker has to become the leader first.


## Upgrading from lilota 1.2.0 or older

Two things were different up to 1.2.0:

* A task function that raised an exception stopped all workers. The task was never retried, and tasks scheduled afterwards were not executed.
* A task was executed once more than **max_attempts** said. With the default of 1, a task that ran into its timeout was executed twice.

Nothing has to be changed in your application. If you relied on the additional execution, raise **max_attempts** of the task by one.
