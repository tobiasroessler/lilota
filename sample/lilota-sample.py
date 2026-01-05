import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dataclasses import dataclass
from lilota.core import Lilota
from lilota.models import Task


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
  lilota.start()
  task_id = lilota.schedule("add", AddInput(a=2, b=3))
  lilota.stop()
  task: Task = lilota.get_task_by_id(task_id)
  print("We add the numbers 2 and 3: ")
  print(task.output)


if __name__ == "__main__":
  main()