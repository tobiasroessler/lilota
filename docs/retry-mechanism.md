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

@worker.register("my_task", max_attempts=5)
```