from lilota.worker import LilotaWorker


worker = LilotaWorker(
  db_url="postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test",
  max_task_heartbeat_interval=0.1
)


@worker.register("hello_world")
def hello_world():
  print("Hello Word")


def main():
  worker.start()


if __name__ == "__main__":
  main()