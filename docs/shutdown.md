# Shutdown

``` python
lilota.stop()
```

In a web application, you will usually not need to call this explicitly.
You start **lilota** once, then schedule tasks as needed.

As long as your application is running, **lilota** can continue running
and waiting for tasks to be scheduled.

If you do call **stop**, **lilota** will wait for all running tasks to
finish before shutting down.