# Error handling

In **Lilota**, external code is executed. Errors that occur in the executed scripts are normally caught and handled by the worker.

If an error cannot be caught by the worker, or if an exception stops parts of Lilota and leaves the system in an inconsistent state, this is reflected in the error property of the Lilota class. This property returns the last fatal error encountered by the system that was not handled by a worker.

If a worker process fails, the error is the last lines the process has written to **stderr**, which usually is the traceback of the exception. The same lines are shown in the output of your application. See [Worker output](worker-output.md).

The Lilota class also provides two methods to:

Check whether a fatal error has occurred (**has_failed**)
Wait until a fatal error occurs (**wait_for_failure**), which blocks execution

## A task raises an exception

This is an error the worker handles. It is not a fatal error.

The worker stores the exception in the **error** column of the task and sets the status of the task to **failed**. Then it goes on with the next task. If the task has attempts left, it is retried. See [Retry mechanism](retry-mechanism.md).

## A worker is killed

A worker process that is killed is replaced by a new one. This is not a fatal error either. It happens when

* a task runs longer than its timeout. The worker stops itself, because that is the only way to end the task. See [Task timeout](task-timeout.md).
* the process is terminated by a signal, for example by the operating system because it used too much memory.

What happens to the task the worker was executing depends on why the worker was killed:

* The task ran longer than its timeout: it gets the status **expired** and is retried if it has attempts left. See [Retry mechanism](retry-mechanism.md).
* The process was terminated by a signal: the task is given back and executed again by another worker, about 20 seconds later. See [Worker maintenance](worker-maintenance.md).

## The database cannot be reached

**lilota** keeps everything in the database. If the database cannot be reached for a while, for example because it is restarted, the workers keep running. This is not a fatal error, and nothing has to be restarted.

* A worker waits and tries again until the database is back. It tries as often as it looks for new tasks when there are none, which is every 5 seconds by default (**max_task_heartbeat_interval**).
* A task that is being executed is not interrupted. If it ends while the database cannot be reached, its result is stored as soon as the database is back.
* The heartbeats of the nodes fail in that time and go on afterwards.

A log entry cannot be written to the database in that time either. It is written to **stderr** instead, as one line:

```
lilota: ERROR: The database cannot be reached. The worker waits and tries again (the log entry could not be written to the database)
```

Methods that you call, like **schedule** and **get_task_by_id**, raise the error of the database to your application.

> **Note:** A worker that is started while the database cannot be reached fails, for example the worker that replaces a worker that was killed. That is a fatal error, as described in the next section.

## Worker script fails

If the worker script itself ends with an error, for example because of an exception while it is imported, starting it again would fail the same way. **Lilota** stops all workers and reports a fatal error. The error is what the process has written to **stderr**.

## Worker script does not start a worker

Each worker process runs the script passed as **script_path** as its entry point. The script has to start the worker itself:

``` python
def main():
  worker.start()


if __name__ == "__main__":
  main()
```

A started worker runs until it is stopped by **Lilota**. If the script ends without calling **worker.start()**, **Lilota** stops all workers and reports a fatal error:

```
The worker script 'myscript.py' exited without starting a worker. Did you forget to call worker.start()?
```
