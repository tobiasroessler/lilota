from lilota.constants import DEFAULT_TEST_DB_URL
from dataclasses import dataclass
from lilota.worker import LilotaWorker
import logging
from typing import Any


class AddInput:
    def __init__(self, a: int, b: int) -> None:
        self.a = a
        self.b = b

    def as_dict(self) -> dict[str, Any]:
        return {
            "a": self.a,
            "b": self.b,
        }


class AddOutput:
    def __init__(self, sum: int) -> None:
        self.sum = sum

    def as_dict(self) -> dict[str, Any]:
        return {"sum": self.sum}


@dataclass
class AddInputDataclass:
    a: int
    b: int


@dataclass
class AddOutputDataclass:
    sum: int


class AddInputInvalid:
    a: int
    b: int

    def __init__(self, a: int, b: int):
        self.a = a
        self.b = b


class AddOutputInvalid:
    sum: int

    def __init__(self, sum: int):
        self.sum = sum


worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
    logging_level=logging.DEBUG,
)


@worker.task
def add(data: AddInput) -> AddOutput:
    return AddOutput(sum=data.a + data.b)


@worker.task
def add_with_exception(data: AddInput) -> AddOutput:
    raise Exception("Error")


@worker.task
def add_with_dataclasses(data: AddInputDataclass) -> AddOutputDataclass:
    return AddOutputDataclass(sum=data.a + data.b)


@worker.task
def add_with_dict(data: dict[str, int]) -> dict[str, int]:
    return {"sum": data["a"] + data["b"]}


@worker.task
def only_input_model(data: AddInput) -> None:
    print("Hello World")


@worker.task
def only_output_model() -> AddOutput:
    return AddOutput(sum=3)


@worker.task
def add_with_invalid_model(data: AddInputInvalid) -> AddOutputInvalid:
    pass


def main():
    worker.start()


if __name__ == "__main__":
    main()
