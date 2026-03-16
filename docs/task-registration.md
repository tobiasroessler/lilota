# Task registration


## Introduction

Before a function can be executed by **LilotaWorker**, it must be **registered as a task**. A task is simply a Python function that is registered using the **@worker.register()** decorator. Once registered, the task can be scheduled using:

```python
lilota.schedule("task-name")
```


## Basic task

The simplest task does not require input or output.

**myscript.py**
``` python
from lilota.worker import LilotaWorker

worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)

@worker.register("hello-world")
def hello_world():
  print("Hello World")
```

The task can then be scheduled using its name. Before a **lilota** instance needs to be created and started:

``` python
lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
  script_path="sample/myscript.py"
)
lilota.start()
```

After that the task can be scheduled:

``` python
task_id = lilota.schedule("hello-world")
```


## Tasks with input and output models

Tasks can optionally define **one input model** and **one output model**. The input model describes the data that the task expects. The output model describes the structure of the result produced by the task.

Using models provides several advantages:

* clearer task interfaces
* type safety
* structured task inputs and outputs
* easier serialization

Currently, **lilota** supports dataclasses, dictionaries, and objects implementing [**ModelProtocol**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.models.ModelProtocol).


### Full example with input and output model

The following example defines a task that adds two numbers.

``` python
from dataclasses import dataclass
from lilota.worker import LilotaWorker
from lilota.models import Task


@dataclass
class AddInput:
  a: int
  b: int


@dataclass
class AddOutput:
  sum: int


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)


@worker.register("add", input_model=AddInput, output_model=AddOutput)
def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


def main():
  worker.start()


if __name__ == "__main__":
  main()
```

### How it works

When the task is scheduled:

``` python
lilota.schedule("add", AddInput(a=2, b=3))
```

the input model is **automatically serialized** before being stored in the database. When the task runs, **lilota** automatically deserializes the data back into the input model:

``` python
def add(data: AddInput) -> AddOutput
```

The returned output model is then **serialized again** before being stored as the task result.


### Accessing the Result

Task results are stored as a dictionary in the database. To reconstruct the output model, you can create the object manually:

``` python
add_output = AddOutput(**task.output)
```


## Tasks with setting progress manually

Some tasks run for a longer time and should report progress. **LilotaWorker** provides the **TaskProgress** helper for this purpose.

To enable this, the **set_progress_manually** option must be enabled when creating the Lilota instance.

``` python
from lilota.worker import LilotaWorker
from lilota.models import TaskProgress

worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
  set_progress_manually=True
)
```

Now a task can receive a **TaskProgress** object:

``` python
@worker.register("do-something", task_progress=TaskProgress)
def do_something(task_progress: TaskProgress) -> None:
  for i in range(1, 101):
    task_progress.set(i)
```

The task updates its progress by calling:

``` python
task_progress.set(percentage)
```


### Full worker example with setting progress manually

``` python
from lilota.worker import LilotaWorker
from lilota.models import Task, TaskProgress


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
  set_progress_manually=True
)


@worker.register("do-something", task_progress=TaskProgress)
def do_something(task_progress: TaskProgress) -> None:
  for i in range(1, 101):
    task_progress.set(i)


def main():
  worker.start()


if __name__ == "__main__":
  main()
```