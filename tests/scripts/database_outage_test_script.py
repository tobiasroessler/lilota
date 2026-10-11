from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.stores import TaskStore
from lilota.worker import LilotaWorker
from sqlalchemy.exc import OperationalError


worker = LilotaWorker(
    db_url=DEFAULT_TEST_DB_URL,
    node_heartbeat_interval_jitter=None,
    max_task_heartbeat_interval=0.1,
)


# A database outage: the first times the worker asks for the next task, and the first
# times it stores the result of a task, the database cannot be reached. After that it
# is back.
CALLS_DURING_OUTAGE = 2


def with_outage(method):
    calls = 0

    def method_with_outage(*args):
        nonlocal calls
        calls += 1
        if calls <= CALLS_DURING_OUTAGE:
            raise OperationalError("SELECT", {}, Exception("connection refused"))
        return method(*args)

    return method_with_outage


TaskStore.get_next_task = with_outage(TaskStore.get_next_task)
TaskStore.end_task_success = with_outage(TaskStore.end_task_success)


@worker.task
def hello_world():
    print("Hello World")


def main():
    worker.start()


if __name__ == "__main__":
    main()
