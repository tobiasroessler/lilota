import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dataclasses import dataclass
from lilota.core import Lilota
from lilota.models import Task
import time


@dataclass
class AddInput:
    a: int
    b: int


@dataclass
class AddOutput:
    sum: int


lilota = Lilota(
    db_url="postgresql+psycopg://postgres:postgres@localhost:5432/lilota_sample",
    script_path="sample/myscript.py",
    number_of_workers=1,
)


def main():
    # Start lilota
    lilota.start()

    # Schedule a task
    task_id = lilota.schedule("add", AddInput(a=2, b=3))

    # Wait one second because Lilota runs in the background and decides when to pick up a task.
    # This is normally not needed. We do it here because we want to wait until the task
    # has been executed.
    time.sleep(3)

    # Retrieve task information from the database and print the result
    task: Task = lilota.get_task_by_id(task_id)
    print("We add the numbers 2 and 3: ")
    print(task.output)

    # Stop lilota and stop all running processes
    lilota.stop()


try:
    main()
except Exception as e:
    import json
    import traceback
    import sys

    print(
        json.dumps(
            {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            }
        ),
        file=sys.stderr,
    )
    sys.exit(1)


# if __name__ == "__main__":
#   main()
