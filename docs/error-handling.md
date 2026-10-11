# Error handling

In **Lilota**, external code is executed. Errors that occur in the executed scripts are normally caught and handled by the worker.

If an error cannot be caught by the worker, or if an exception stops parts of Lilota and leaves the system in an inconsistent state, this is reflected in the error property of the Lilota class. This property returns the last fatal error encountered by the system that was not handled by a worker.

The Lilota class also provides two methods to:

Check whether a fatal error has occurred (**has_failed**)
Wait until a fatal error occurs (**wait_for_failure**), which blocks execution

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
