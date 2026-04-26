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
      - [Create a worker instance](#create-a-worker-instance)
      - [Register a background task](#register-a-background-task)
    - [Integration of lilota](#integration-of-lilota)
      - [Define input and output models](#define-input-and-output-models)
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


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)


@worker.register("add", input_model=AddInput, output_model=AddOutput)
def add(input: AddInput) -> AddOutput:
  return AddOutput(input.a + input.b)


def main():
  worker.start()


if __name__ == "__main__":
  main()
```


#### Create a worker instance

``` python
worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)
```

In this example we use a url to a **postgres** database. **lilota** uses **SQLAlchemy** and therefore all
databases that are supported by SQLAlchemy can be used here.


#### Register a background task

``` python
@worker.register("add", input_model=AddInput, output_model=AddOutput)
def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)
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
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
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


#### Define input and output models

* Input and output models are optional
* You do not have to use **dataclasses** for these models. You can use any
serializable model, such as **pydantic** models.
* It is only important that the models are serializable, since they are
stored in the database.
* **lilota** uses a **ModelProtocol**. To comply with it, you only need
to define an **as_dict** method. A full example using pydantic can be found here:
[3-add-two-numbers-using-pydantic](https://github.com/tobiasroessler/lilota-sample/blob/main/src/3-add-two-numbers-using-pydantic)
* lilota also supports passing a **TaskProgress** instance to the task function.
This can be used to update progress information in the database. It is important to set 
**set_progress_manually=True** when creating the **lilota** instance. A full example 
can be found here:
[5-setting-task-progress-manually](https://github.com/tobiasroessler/lilota-sample/blob/main/src/5-setting-task-progress-manually)


#### Create a Lilota instance

``` python
lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
  script_path="sample/myscript.py",
  number_of_workers=8
)
```


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
