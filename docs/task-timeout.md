# Task timeout

In a software application, it can happen that scheduled tasks are executed but never finish. This can occur for several reasons:

* The task contains an infinite loop
* The task simply takes too long to execute
* The task is waiting indefinitely for an external service (e.g., API call, database, or network request)
* A deadlock occurs (e.g., waiting on a locked resource)
* Resource exhaustion (e.g., CPU, memory) prevents the task from progressing

To handle such situations, **lilota** provides a mechanism to deal with tasks that never complete by stopping the corresponding worker process. When this happens, the task is terminated and its status is set to **expired**.

In **lilota**, it is recommended to specify a timeout for each task.

The timeout can be set when registering a task.

``` python
worker = LilotaWorker(
  ...
)

@worker.register("my_task", timeout=timedelta(minutes=5))
```

The default timeout is **5 minutes**. This can be adjusted using Python’s **timedelta** object.
If **None** is specified, the timeout is disabled.