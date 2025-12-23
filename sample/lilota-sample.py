import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from lilota.core import Lilota
from pydantic import BaseModel


class AddInput(BaseModel):
    a: int
    b: int


class AddOutput(BaseModel):
  sum: int


lilota = Lilota(name="My Server", db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample")

@lilota.register("add", input_model=AddInput, output_model=AddOutput)
def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


def main():
  lilota.start()
  lilota.schedule("add", AddInput(a=2, b=3))
  lilota.stop()
  tasks = lilota.get_all_tasks()
  print(tasks)


if __name__ == "__main__":
  main()