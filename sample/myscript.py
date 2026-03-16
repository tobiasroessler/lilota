from dataclasses import dataclass
from lilota.worker import LilotaWorker


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
  max_task_heartbeat_interval=0.1
)


@dataclass
class AddInput():
    a: int
    b: int


@dataclass
class AddOutput():
  sum: int


@worker.register("add", input_model=AddInput, output_model=AddOutput)
def add(input: AddInput) -> AddOutput:
  return AddOutput(input.a + input.b)


def main():
  worker.start()


if __name__ == "__main__":
  main()