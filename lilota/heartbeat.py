from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
import threading
import logging
from .stores import SqlAlchemyNodeStore, SqlAlchemyNodeLeaderStore


class HeartbeatTask(ABC):

  def __init__(self, interval: float):
    self.interval = interval


  @abstractmethod
  def execute(self):
    pass



class Heartbeat(threading.Thread):

  def __init__(self, name: str, task: HeartbeatTask, logger: logging.Logger):
    super().__init__(name=name, daemon=True)
    self._task = task
    self._logger = logger
    self._stop_event = threading.Event()


  def run(self) -> None:
    while not self._stop_event.is_set():
      # Execute
      try:
        self._task.execute()
      except Exception:
        self._logger.exception("Heartbeat task failed")

      # Wait
      interval = max(0.0, self._task.interval)
      self._stop_event.wait(interval)


  def stop(self) -> None:
    self._stop_event.set()


  def stop_and_join(self, timeout=None):
    self.stop()
    self.join(timeout)



class TaskHeartbeatThread(threading.Thread):

  def __init__(self, name: str, max_sleep_in_sec: float = 5.0):
    super().__init__(name=name, daemon=True)
    self._stop_event = threading.Event()
    self._sleep = 0.1
    self._max_sleep_in_sec = max_sleep_in_sec
    # self._fn_get_next_task = fn_get_next_task
    # self._fn_execute_task = fn_execute_task


  def run(self) -> None:
    while not self._stop_event.is_set():
      print("Fetch task")
      # task = self._fn_get_next_task()
      task = None
      if task:
        self._sleep = 0.1
        print("Execute task")
        # self._fn_execute_task(task)
      else:
        self._sleep = min(self._sleep * 2, self._max_sleep_in_sec)
      self._stop_event.wait(self._sleep)


  def stop(self) -> None:
    self._stop_event.set()