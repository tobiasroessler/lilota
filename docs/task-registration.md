# Task registration


## Introduction

Before a function can be executed by **LilotaWorker**, it must be **registered as a task**. A task is simply a Python function that is registered using the **@worker.task** decorator. Once registered, the task can be scheduled using:

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

@worker.task
def hello_world():
  print("Hello World")
```

By default a task is regsitered using the method name (here "hello_world"). Before a **lilota** instance needs to be created and started:

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

A task name can also be specified if needed.

``` python
@worker.task("mytask")
def hello_world():
  print("Hello World")
```

 When scheduling the task this name is used.

``` python
task_id = lilota.schedule("mytask")
```


## Tasks with input and output models

Tasks can optionally define **one input model** and **one output model**. The input model describes the data that the task expects. The output model describes the structure of the result produced by the task.

Using models provides several advantages:

* clearer task interfaces
* type safety
* structured task inputs and outputs
* easier serialization

Currently, **lilota** supports dataclasses, dictionaries, and objects implementing [**ModelProtocol**](https://tobiasroessler.github.io/lilota/lilota-reference/#lilota.models.ModelProtocol).


### Define input and output models

* Input and output models are optional
* You do not have to use **dataclasses** for these models. You can use any
serializable model, such as **pydantic** models.
* It is only important that the models are serializable, since they are
stored in the database.
* **lilota** uses a **ModelProtocol**. To comply with it, you only need
to define an **as_dict** method. A full example using pydantic can be found here:
[3-add-two-numbers-using-pydantic](https://github.com/tobiasroessler/lilota-sample/blob/main/src/3-add-two-numbers-using-pydantic)
* lilota also supports passing a **TaskContext** instance to the task function.
It contains a **progress** field (of type **TaskProgress**) that can be used to update progress 
information in the database. 
It also contains a **logger** field (of type **logging.Logger**) that can be used to use the logging
mechanism of Lilota.
A full example using the **progress** field can be found here:
[5-setting-task-progress-manually](https://github.com/tobiasroessler/lilota-sample/blob/main/src/5-setting-task-progress-manually)


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


@worker.task
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

Some tasks run for a longer time and should report progress. **LilotaWorker** provides the **TaskProgress** helper for this purpose. It is part of the **TaskContext** object.

``` python
@worker.task
def do_something(task_context: TaskContext) -> None:
  for i in range(1, 101):
    task_context.progress.set(i)
```


### Full worker example with setting progress manually

``` python
from lilota.worker import LilotaWorker
from lilota.models import Task, TaskContext


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)


@worker.task
def do_something(task_context: TaskContext) -> None:
  for i in range(1, 101):
    task_context.progress.set(i)


def main():
  worker.start()


if __name__ == "__main__":
  main()
```