from abc import ABC, abstractmethod
import threading
import logging


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
