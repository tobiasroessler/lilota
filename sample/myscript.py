import sys
import os
from lilota.models import TaskContext

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


worker = LilotaWorker(
    db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)


@worker.task
def add(input: AddInput, ctx: TaskContext) -> AddOutput:
    return AddOutput(input.a + input.b)


def main():
    worker.start()


if __name__ == "__main__":
    main()
