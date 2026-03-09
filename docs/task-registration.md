# Task registration


## Introduction

Before a function can be executed by **lilota**, it must be **registered as a task**. A task is simply a Python function that is registered using the **@lilota.register()** decorator. Once registered, the task can be scheduled using:

```python
lilota.schedule("task-name")
```


## Basic task

The simplest task does not require input or output.

``` python
from lilota.core import Lilota

lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)

@lilota.register("hello-world")
def hello_world():
  print("Hello World")
```

The task can then be scheduled using its name:

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
from lilota.core import Lilota
from lilota.models import Task


@dataclass
class AddInput:
  a: int
  b: int


@dataclass
class AddOutput:
  sum: int


lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)


@lilota.register("add", input_model=AddInput, output_model=AddOutput)
def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


def main():
  number1 = 2
  number2 = 3

  # Start lilota
  lilota.start()

  # Schedule the task
  task_id = lilota.schedule("add", AddInput(a=number1, b=number2))

  # After worker picked up the task (put a sleep here to wait a bit)
  task: Task = lilota.get_task_by_id(task_id)

  # Get the ouput model
  add_output = AddOutput(**task.output)

  # Print the result
  print(f"{number1} + {number2} = {add_output.sum}")  # 2 + 3 = 5


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

Some tasks run for a longer time and should report progress. **Lilota** provides the **TaskProgress** helper for this purpose.

To enable this, the **set_progress_manually** option must be enabled when creating the Lilota instance.

``` python
from lilota.core import Lilota
from lilota.models import TaskProgress

lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
  set_progress_manually=True
)
```

Now a task can receive a **TaskProgress** object:

``` python
@lilota.register("do-something", task_progress=TaskProgress)
def do_something(task_progress: TaskProgress) -> None:
  for i in range(1, 101):
    task_progress.set(i)
```

The task updates its progress by calling:

``` python
task_progress.set(percentage)
```


### Full example with setting progress manually

``` python
from lilota.core import Lilota
from lilota.models import Task, TaskProgress


lilota = Lilota(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
  set_progress_manually=True
)


@lilota.register("do-something", task_progress=TaskProgress)
def do_something(task_progress: TaskProgress) -> None:
  for i in range(1, 101):
    task_progress.set(i)


def main():
  # Start lilota
  lilota.start()

  # Schedule the task
  task_id = lilota.schedule("do-something")

  # After worker picked up the task (put a sleep here to wait a bit)
  task: Task = lilota.get_task_by_id(task_id)
  print(task.progress_percentage)  # Should be 100


if __name__ == "__main__":
  main()
```