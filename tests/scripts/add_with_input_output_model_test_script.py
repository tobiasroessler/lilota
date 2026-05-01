from lilota.worker import LilotaWorker
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


worker = LilotaWorker(
    db_url="postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test",
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


@worker.register("add", input_model=AddInput, output_model=AddOutput)
def add(data: AddInput) -> AddOutput:
    return AddOutput(sum=data.a + data.b)


def main():
    worker.start()


if __name__ == "__main__":
    main()
