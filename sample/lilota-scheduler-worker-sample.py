import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dataclasses import dataclass
from lilota.core import LilotaScheduler, LilotaWorker
from lilota.models import Node, Task
import time


scheduler = LilotaScheduler(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample"
)

worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample", 
  number_of_processes=1
)


@dataclass
class AddInput():
    a: int
    b: int


@dataclass
class AddOutput():
  sum: int


# @worker.register("add", input_model=AddInput, output_model=AddOutput)
# def add(input: AddInput) -> AddOutput:
#    return AddOutput(input.a + input.b)


def main():
  input("Enter a key to start the scheduler")
  scheduler.start()
  node: Node = scheduler.get_node()
  print(f"Scheduler ID: {node.id}")
  input("Enter a key to stop the scheduler")
  scheduler.stop()
  input("Enter a key to start the scheduler")
  scheduler.start()
  input("Enter a key to stop the scheduler")
  scheduler.stop()

  input("Enter a key to start the worker")
  worker.start()
  node: Node = worker.get_node()
  print(f"Worker ID: {node.id}")
  input("Enter a key to stop the worker")
  worker.stop()
  input("Enter a key to start the worker")
  worker.start()
  input("Enter a key to stop the application")
  worker.stop()

  input("Stopped")


if __name__ == "__main__":
  main()