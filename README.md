# lilota

**lilota** is a lightweight Python library for executing long-running
tasks in the background without the overhead of full-fledged task queue
systems. While those tools are powerful and 
valuable, **lilota** focuses on scenarios where a simpler approach is sufficient.

It is designed for simple, asynchronous task execution with minimal
setup and overhead.

- [lilota](#lilota)
  - [Features](#features)
  - [When to use lilota](#when-to-use-lilota)
  - [Installation](#installation)
  - [Quick example](#quick-example)
    - [myscript.py](#myscriptpy)
    - [Integration of lilota](#integration-of-lilota)
      - [Create a Lilota instance](#create-a-lilota-instance)
      - [Start lilota](#start-lilota)
      - [Schedule a task](#schedule-a-task)
      - [Retrieve task information including the output (if available)](#retrieve-task-information-including-the-output-if-available)
  - [Documentation](#documentation)
  - [Examples](#examples)


## Features

-   Run long-running tasks
-   Simple API and minimal configuration and setup
-   Persistent task state stored in a database
-   No message broker required
-   Suitable for applications that use background jobs, i.e. web applications.


## When to use lilota

Use **lilota** when your application needs to run tasks that take time,
such as:

-   image or file processing
-   report generation
-   sending emails
-   heavy computations

Instead of blocking the request, **lilota** lets you start the task in the
background.


## Installation

``` bash
pip install lilota
```


## Quick example

This example demonstrates how to add two numbers using a function that runs in the background.

This could, of course, also be a function that generates a report or performs
a heavy computation. For simplicity, we will just add two numbers.

First, we have to create a script that has an instance of a worker (**LilotaWorker**). This worker
registers one or several task-functions that can be executed later by a scheduler. 


### myscript.py

``` python
from dataclasses import dataclass
from lilota.worker import LilotaWorker


@dataclass
class AddInput():
    a: int
    b: int


@dataclass
class AddOutput():
  sum: int


worker = LilotaWorker()


@worker.task
def add(input: AddInput) -> AddOutput:
  return AddOutput(input.a + input.b)


def main():
  worker.start()


if __name__ == "__main__":
  main()
```


### Integration of lilota

The script created above is passed to a **Lilota** instance for execution. 

That instance can start multiple processes, each of which executes the script once. You can specify the number of workers for a **Lilota** instance. By default, it is set to **cpu_count()**.

In this example, we use two model classes: one for input arguments and one for the output.
These models should typically be defined in their own module and shared between **Lilota** and **LilotaWorker**.

``` python
from dataclasses import dataclass
from lilota.core import Lilota
from lilota.models import Task
import time


@dataclass
class AddInput():
    a: int
    b: int


@dataclass
class AddOutput():
  sum: int


lilota = Lilota(
  script_path="sample/myscript.py",
  number_of_workers=8
)


def main():
  # Start lilota
  lilota.start()

  # Schedule a task
  task_id = lilota.schedule("add", AddInput(a=2, b=3))

  # Wait one second because Lilota runs in the background and decides when to pick up a task.
  # This is normally not needed. We do it here because we want to wait until the task 
  # has been executed.
  time.sleep(1)

  # Retrieve task information from the database and print the result
  task: Task = lilota.get_task_by_id(task_id)
  print(f"We add the numbers 2 and 3: ")
  print(task.output)


if __name__ == "__main__":
  main()
```


#### Create a Lilota instance

``` python
lilota = Lilota(
  script_path="sample/myscript.py",
  number_of_workers=8
)
```

Additionally it is possible to create a **Lilota scheduler** using a class method:

``` python
scheduler: LilotaScheduler = Lilota.scheduler()
```

If you need a separate application that runs workers but does not require a scheduler, you can create it like this:

``` python
workers = Lilota.workers(
  script_path=str(Path(__file__).resolve().parent / "workerscript.py"),
  number_of_workers=1
)
```

A full example using the class methods can be found here:
[7-using-factory-methods](https://github.com/tobiasroessler/lilota-sample/blob/main/src/7-using-factory-methods)



#### Start lilota

``` python
lilota.start()
```


#### Schedule a task

``` python
task_id = lilota.schedule("add", AddInput(a=2, b=3))
```

The **schedule** function creates a task entry in the database and starts
executing it immediately. The ID of the stored task is returned.


#### Retrieve task information including the output (if available)

``` python
task: Task = lilota.get_task_by_id(task_id)
add_output = AddOutput(**task.output)
print(add_output.sum)
```


## Documentation

[https://tobiasroessler.github.io/lilota/](https://tobiasroessler.github.io/lilota/)


## Examples

| Example | URL |
| ------- | ----- |
| A simple "Hello World" example | [1-hello-world](https://github.com/tobiasroessler/lilota-sample/blob/main/src/1-hello-world) |
| Add two numbers using an input and an output model | [2-add-two-numbers](https://github.com/tobiasroessler/lilota-sample/blob/main/src/2-add-two-numbers) |
| Add two numbers using a pydantic input and an output model | [3-add-two-numbers-using-pydantic](https://github.com/tobiasroessler/lilota-sample/blob/main/src/3-add-two-numbers-using-pydantic) |
| Database access inside the task function | [4-using-db-inside-task](https://github.com/tobiasroessler/lilota-sample/blob/main/src/4-using-db-inside-task) |
| Set the task progress manually in a task function | [5-setting-task-progress-manually](https://github.com/tobiasroessler/lilota-sample/blob/main/src/5-setting-task-progress-manually) |
| Use a worker script that uses modules | [6-script-with-modules](https://github.com/tobiasroessler/lilota-sample/blob/main/src/6-script-with-modules) |
| Use class methods to create a scheduler and workers | [7-using-factory-methods](https://github.com/tobiasroessler/lilota-sample/blob/main/src/7-using-factory-methods) |
