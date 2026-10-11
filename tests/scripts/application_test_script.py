from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.core import Lilota
import sys
import time


# An application that starts lilota and runs until it is ended from the outside. It
# never calls lilota.stop(). The test passes the worker script as the first argument
# and reads the process IDs of the workers from stdout.


def main():
    lilota = Lilota(
        db_url=DEFAULT_TEST_DB_URL,
        script_path=sys.argv[1],
        number_of_workers=2,
    )
    lilota.start()
    print(*(mp.proc.pid for mp in lilota._process_manager._processes), flush=True)

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
