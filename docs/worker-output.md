# Worker output

Every worker is a Python process of its own, started by **lilota**. What a task function writes with **print**, or what a library it uses writes as a warning, comes from that process and not from your application.

**lilota** shows this output where the output of your application is shown.


## Output on stdout

What a worker writes to **stdout** goes to the **stdout** of your application: to the terminal you started the application in, or to the log of its container.

``` python
@worker.task
def hello_world():
  print("Hello World")
```

**Hello World** appears as soon as the task has printed it. Workers are started with **python -u**, so the text does not wait in a buffer when the output of your application is written to a file or a pipe instead of a terminal.


## Output on stderr

What a worker writes to **stderr**, such as warnings or the traceback of an exception, is passed on line by line to the **stderr** of your application. It is written to **sys.stderr**, so an application that replaces **sys.stderr** receives it there.

Writing to **stderr** is not an error. A worker that writes a warning keeps running.

**lilota** also keeps the last 200 lines a worker has written to **stderr**. If the worker process fails, these lines are the fatal error reported by the **error** property of the **Lilota** class. See [Error handling](error-handling.md).


## Upgrading from lilota 1.2.0 or older

Up to 1.2.0 the output of a worker was collected but never read while the worker was running. It was not shown anywhere, and a worker that had written about 64 KB stopped in the middle of its task, because there was no room left for more output.

Nothing has to be changed in your application. Be aware that output of your tasks that was invisible before is shown now.
