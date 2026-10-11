from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID


# Retries are created by the worker's heartbeat. A short interval keeps the test fast.
worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval=0.5,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


@dataclass
class Order:
    id: UUID
    created_at: datetime
    price: Decimal


class NotStorable:
    """A return value that cannot be written to the database."""


@worker.task
def return_list() -> list[int]:
    return [1, 2, 3]


@worker.task
def return_string() -> str:
    return "done"


@worker.task
def return_number() -> int:
    return 42


# The test knows these values.
@worker.task
def return_datetime_uuid_and_decimal() -> Order:
    return Order(
        id=UUID("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"),
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        price=Decimal("9.99"),
    )


@worker.task(max_attempts=2)
def return_value_that_cannot_be_stored():
    return NotStorable()


def main():
    worker.start()


if __name__ == "__main__":
    main()
