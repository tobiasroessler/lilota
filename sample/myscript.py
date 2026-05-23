import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dataclasses import dataclass
from lilota.worker import LilotaWorker


@dataclass
class AddInput:
    a: int
    b: int


@dataclass
class AddOutput:
    sum: int


worker = LilotaWorker()


@worker.task
def add(input: AddInput) -> AddOutput:
    return AddOutput(input.a + input.b)


def main():
    worker.start()


if __name__ == "__main__":
    main()
