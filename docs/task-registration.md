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

worker = LilotaWorker()

@worker.task
def hello_world():
  print("Hello World")
```

By default a task is regsitered using the method name (here "hello_world"). Before a **lilota** instance needs to be created and started:

``` python
lilota = Lilota(
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


worker = LilotaWorker()


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


## What a task can return

The return value of a task function is stored as JSON in the **output** column of the task. A task function can return

* a dataclass, a dictionary or an object implementing **ModelProtocol**
* any other JSON value: a string, a number, a boolean, a list or **None**

``` python
@worker.task
def count_words(data: dict[str, str]) -> int:
  return len(data["text"].split())
```

Values that JSON does not know are converted, also inside of a dataclass, a dictionary or a list:

| Returned value                         | Stored as                                     |
| -------------------------------------- | --------------------------------------------- |
| **datetime**, **date**, **time**       | text in ISO 8601 format, like **2026-01-02T03:04:05+00:00** |
| **UUID**                               | text                                          |
| **Decimal**                            | text, like **9.99**, so that no digit is lost |
| tuple                                  | list                                          |

### A return value that cannot be stored

A value of any other type cannot be stored, for example an object of a class of your own that has no **as_dict** method.

The task function has done its work at that point. Executing it again would do that work a second time. The task therefore

* gets the status **failed**
* is **not retried**, whatever **max_attempts** says. Its **max_attempts** column is set to the attempts it has made
* gets an error that says so:

```
The task function succeeded, but its result could not be stored: Unsupported type: Report. Expected a JSON value, a ModelProtocol, a dataclass, a datetime, a date, a time, a UUID or a Decimal.
```


## Upgrading from lilota 1.2.1 or older

Up to 1.2.1 a task function could only return a dataclass, a dictionary or an object implementing **ModelProtocol**, and only with values that JSON knows. With any other return value the task was marked as **failed** after its function had returned, and it was executed again if it had attempts left.

Nothing has to be changed in your application. Tasks that failed for this reason are completed now.


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


worker = LilotaWorker()


@worker.task
def do_something(task_context: TaskContext) -> None:
  for i in range(1, 101):
    task_context.progress.set(i)


def main():
  worker.start()


if __name__ == "__main__":
  main()
```


## Tasks that use the lilota logger

It is possible to use the internal logging system from **lilota**. Via the **TaskContext** it can be injected to the task.

``` python
@worker.task
def only_logger(task_context: TaskContext) -> None:
  task_context.logger.info("Message from only_logger task")
```


## Tasks whose input should not stay in the database

The input of a task is stored as JSON in the **input** column of **lilota_task**, and it stays there until **cleanup** deletes the row (see [Database](database.md#cleaning-up)). For most tasks that is what you want: you can look up later what a task was executed with.

Some inputs should not stay. A task that sends a mail with a link to reset a password gets the link as its input, and the link works for whoever reads it. Somebody who can read the database, or a backup of it, could use it.

Register such a task with **keep_input=False**:

``` python
@worker.task(max_attempts=5, keep_input=False)
def send_mail(message: MailMessage) -> None:
  ...
```

Its input is set to **NULL** as soon as nothing can need it any more:

| The task                                     | Its input                                                     |
| -------------------------------------------- | ------------------------------------------------------------- |
| is waiting or being executed                 | stays                                                         |
| is completed                                 | is removed                                                    |
| has failed or expired with attempts left     | stays until its retry is created, which takes it over. Then it is removed from the failed task |
| has failed or expired with no attempts left  | is removed                                                    |

The row of the task stays as before: its status, its attempts, its times, its output and its error. Only the input is gone, so a task that failed can no longer be looked up for what it was executed with.

The input is removed in the same transaction that ends the task, so there is no moment in which a task has ended and its input is still waiting to be removed. A task that is given back because its worker died is not finished, and keeps its input (see [Worker maintenance](worker-maintenance.md)).

**keep_input** is stored with the task in the **keep_input** column, when a worker starts it, like **max_attempts** and **timeout**. A task that no worker has started yet keeps its input in any case.


## Upgrading from lilota 1.3.0 or older

Nothing has to be changed in your application. The new **keep_input** column of **lilota_task** is added by **lilota**'s migrations when a scheduler or a worker is created (see [Database](database.md#creating-the-tables)). Existing tasks keep their input, as before.
