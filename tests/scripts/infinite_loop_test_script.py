from lilota.worker import LilotaWorker
from datetime import timedelta
import time


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test",
  node_heartbeat_interval_jitter=None,
  max_task_heartbeat_interval=0.1
)


@worker.register("infinite_loop", timeout=timedelta(seconds=1))
def hello_world():
  while True:
    time.sleep(0.1)


def main():
  worker.start()


if __name__ == "__main__":
  main()