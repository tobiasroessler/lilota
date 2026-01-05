# lilota

**lilota** is a lightweight Python library for executing long-running
tasks in the background without the complexity of full task queue
systems like Celery or RabbitMQ.

It is designed for simple, asynchronous task execution with minimal
setup and overhead.

- [lilota](#lilota)
  - [Features](#features)
  - [When to use lilota](#when-to-use-lilota)
  - [Installation](#installation)
  - [Quick example](#quick-example)
    - [Define input and output models](#define-input-and-output-models)
    - [Create a lilota instance](#create-a-lilota-instance)
    - [Register a background task](#register-a-background-task)
    - [Start lilota](#start-lilota)
    - [Schedule a task](#schedule-a-task)
  - [Task persistence](#task-persistence)
  - [Shutdown](#shutdown)
  - [Full example](#full-example)


## Features

-   Run long-running tasks in separate processes
-   Simple API and minimal configuration
-   Persistent task state stored in a database
-   No message broker required
-   Suitable for web applications and background jobs


## When to use lilota

Use **lilota** when your application needs to run tasks that take time,
such as:

-   image or file processing
-   report generation
-   sending emails
-   heavy computations

Instead of blocking the request, lilota lets you start the task in the
background and immediately return a response to the user.


## Installation

``` bash
pip install lilota
```


## Quick example

### Define input and output models

``` python
from dataclasses import dataclass
from lilota.core import Lilota

@dataclass
class AddInput:
  a: int
  b: int

@dataclass
class AddOutput:
  sum: int
```


### Create a lilota instance

``` python
lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)
```

In this example we use a url to a **postgres** database. **lilota** uses **SQLAlchemy** and therefore all
databases that are supported by SQLAlchemy can be used here.

> **Note:** SQLite is an excellent database in many scenarios, but for a multiprocessing application like lilota, it has fundamental limitations that often make it a poor fit.


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


## Task persistence

**schedule** will directly execute our function in a separate process. Information about the executed task are stored inside the database in the **task** table:

| Columns | Notes |
| ------- | ----- |
| id | Primary key |
| name | Task name |
| pid | Process ID |
| status | pending, running, completed, failed, cancelled |
| progress_percentage | Progress (0-100) |
| start_date_time | Start timestamp |
| end_date_time | End timestamp |
| input | Serialized input data |
| output | Serialized output data |
| exception | Exception details if the task fails |


## Shutdown

``` python
lilota.stop()
```

**lilota** will wait for running tasks to finish before exiting.


## Full example

The full example can be found here: https://github.com/tobiasroessler/lilota-sample/blob/main/src/2-add-two-numbers.py
