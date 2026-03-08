# lilota

**lilota** is a lightweight Python library for executing long-running
tasks in the background without the overhead of full-fledged task queue
systems like Celery or RabbitMQ. While those tools are powerful and 
valuable, lilota focuses on scenarios where a simpler approach is sufficient.

It is designed for simple, asynchronous task execution with minimal
setup and overhead.

- [lilota](#lilota)
  - [Features](#features)
  - [When to use lilota](#when-to-use-lilota)
  - [Installation](#installation)
  - [Simple mode and Cluster mode](#simple-mode-and-cluster-mode)
  - [Quick example (Simple mode)](#quick-example-simple-mode)
    - [Define input and output models](#define-input-and-output-models)
    - [Create a lilota instance](#create-a-lilota-instance)
    - [Register a background task](#register-a-background-task)
    - [Start lilota](#start-lilota)
    - [Schedule a task](#schedule-a-task)
    - [Retrieve task information including the output (if available)](#retrieve-task-information-including-the-output-if-available)
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

Instead of blocking the request, lilota lets you start the task in the
background.


## Installation

``` bash
pip install lilota
```


## Simple mode and Cluster mode

lilota supports two modes:
1. Simple mode
2. Cluster mode

In **Simple mode** one scheduler and one worker is started. The scheduler is responsible for scheduling the
tasks and the worker executes the tasks. A worker is executing only one task at a time.

If you want to execute multiple tasks in parallel you have to run lilota in **Cluster mode**. Here 
you have one scheduler and several workers.


## Quick example (Simple mode)

This example demonstrates how to add two numbers using a function that runs in the background.

This could, of course, also be a function that generates a report or performs
a heavy computation. For simplicity, we will just add two numbers.

First, we define a class used to pass input arguments to the background task.
Here, we call this model **AddInput**, which has two properties: **a** and **b**.

We also define an output model called **AddOutput**. This model is populated
with the result of the computation and stored in the database, where it can
later be retrieved.

Here is the full example:

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


lilota = Lilota(db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample")

@lilota.register("add", input_model=AddInput, output_model=AddOutput)
def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


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

### Define input and output models

* Input and output models are optional
* You do not have to use **dataclasses** for these models. You can use any
serializable model, such as **pydantic** models.
* It is only important that the models are serializable, since they are
stored in the database.
* **lilota** uses a **ModelProtocol**. To comply with it, you only need
to define an **as_dict** method. A full example using pydantic can be found here:
[3-add-two-numbers-using-pydantic.py](https://github.com/tobiasroessler/lilota-sample/blob/main/src/3-add-two-numbers-using-pydantic.py)
* lilota also supports passing a **TaskProgress** instance to the task function.
This can be used to update progress information in the database. It is important to set 
**set_progress_manually=True** when creating the **lilota** instance. A full example 
can be found here:
[5-setting-task-progress-manually.py](https://github.com/tobiasroessler/lilota-sample/blob/main/src/5-setting-task-progress-manually.py)


### Create a lilota instance

``` python
lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)
```

In this example we use a url to a **postgres** database. **lilota** uses **SQLAlchemy** and therefore all
databases that are supported by SQLAlchemy can be used here.


### Register a background task

``` python
@lilota.register("add", input_model=AddInput, output_model=AddOutput)
def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)
```


### Start lilota

``` python
lilota.start()
```


### Schedule a task

``` python
task_id = lilota.schedule("add", AddInput(a=2, b=3))
```

The **schedule** function creates a task entry in the database and starts
executing it immediately.
The ID of the stored task is returned.


### Retrieve task information including the output (if available)

``` python
task: Task = lilota.get_task_by_id(task_id)
add_output = AddOutput(**task.output)
print(add_output.sum)
```


## Examples

| Example | URL |
| ------- | ----- |
| A simple "Hello World" example | [1-hello-world.py](https://github.com/tobiasroessler/lilota-sample/blob/main/src/1-hello-world.py) |
| Add two numbers using an input and an output model | [2-add-two-numbers.py](https://github.com/tobiasroessler/lilota-sample/blob/main/src/2-add-two-numbers.py) |
| Add two numbers using a pydantic input and an output model | [3-add-two-numbers-using-pydantic.py](https://github.com/tobiasroessler/lilota-sample/blob/main/src/3-add-two-numbers-using-pydantic.py) |
| Database access inside the task function | [4-using-db-inside-task.py](https://github.com/tobiasroessler/lilota-sample/blob/main/src/4-using-db-inside-task.py) |
| Set the task progress manually in a task function | [5-setting-task-progress-manually.py](https://github.com/tobiasroessler/lilota-sample/blob/main/src/5-setting-task-progress-manually.py) |
