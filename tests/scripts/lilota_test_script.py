from dataclasses import dataclass
from lilota.models import TaskProgress
from lilota.worker import LilotaWorker
import logging
from typing import Any


class AddInput():
  def __init__(self, a: int, b: int) -> None:
    self.a = a
    self.b = b

  def as_dict(self) -> dict[str, Any]:
    return {
      "a": self.a,
      "b": self.b,
    }


class AddOutput():
  def __init__(self, sum: int) -> None:
    self.sum = sum

  def as_dict(self) -> dict[str, Any]:
    return {
      "sum": self.sum
    }


@dataclass
class AddInputDataclass():
    a: int
    b: int


@dataclass
class AddOutputDataclass():
  sum: int


class AddInputInvalid():
  a: int
  b: int

  def __init__(self, a: int, b: int):
    self.a = a
    self.b = b


class AddOutputInvalid():
  sum: int

  def __init__(self, sum: int):
    self.sum = sum


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test",
  max_task_heartbeat_interval=0.1,
  logging_level=logging.DEBUG
)


@worker.register("add", input_model=AddInput, output_model=AddOutput)
def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


@worker.register("add_with_exception", input_model=AddInput, output_model=AddOutput)
def add_with_exception(data: AddInput) -> AddOutput:
  raise Exception("Error")


@worker.register("add_with_dataclasses", input_model=AddInputDataclass, output_model=AddOutputDataclass)
def add_with_dataclasses(data: AddInputDataclass) -> AddOutputDataclass:
  return AddOutputDataclass(sum=data.a + data.b)


@worker.register("add_with_dict", input_model=dict[str, int], output_model=dict[str, int])
def add_with_dict(data: dict[str, int]) -> dict[str, int]:
  return {
    "sum": data["a"] + data["b"]
  }


@worker.register("only_input_model", input_model=AddInput)
def only_input_model(data: AddInput) -> None:
  print("Hello World")


@worker.register("only_output_model", output_model=AddOutput)
def only_output_model() -> AddOutput:
  return AddOutput(sum=3)


@worker.register("add_with_invalid_model", input_model=AddInputInvalid, output_model=AddOutputInvalid)
def add_with_invalid_model(data: AddInputInvalid) -> AddOutputInvalid:
  pass


def main():
  worker.start()


if __name__ == "__main__":
  main()