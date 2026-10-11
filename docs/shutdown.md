# Shutdown

``` python
lilota.stop()
```

In a web application, you will usually not need to call this explicitly.
You start **lilota** once, then schedule tasks as needed.

As long as your application is running, **lilota** can continue running
and waiting for tasks to be scheduled.

If you do call **stop**, **lilota** will stop the started scheduler and all 
running workers.


## The application ends without stopping lilota

An application does not always get the chance to stop its workers:

* it is killed, for example with **kill -9**
* it crashes
* the operating system ends it because it used too much memory
* it is terminated and does not handle the signal

Workers do not keep running in that case. Once a second each worker checks whether the process that started it is still there. If it is gone, the worker stops itself. Processes that a task has started are stopped with it.

Before it stops, the worker writes a warning to the **lilota_log** table:

```
The worker stops because the process that started it is gone
```

A task that the worker is executing at that moment is not finished. It is handled like the task of a worker that was killed. See [Error handling](error-handling.md).

A worker script that you start yourself, without the **Lilota** class, is not affected. It keeps running when the process that started it has ended.

> **Note:** This works on Linux and macOS. On Windows a worker does not notice that the application is gone and keeps running.


## Upgrading from lilota 1.2.1 or older

Up to 1.2.1 workers kept running when the application had ended without stopping them. They took tasks and executed them with the code they were started with, and every start of the application added more workers.

Nothing has to be changed in your application. Workers that an older version has left behind do not stop on their own. Look for them once after the upgrade and end them, for example with **ps ax | grep myscript.py**.