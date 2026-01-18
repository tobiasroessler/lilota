import threading
from datetime import datetime, timezone
from typing import Callable
import logging


class HeartbeatThread(threading.Thread):
    
  def __init__(
    self,
    node_id: str,
    update_fn: Callable[[str, datetime], None],
    logger: logging.Logger,
    interval_seconds: int = 5,
    name: str = "lilota-heartbeat",
  ):
    super().__init__(name=name, daemon=True)
    self.node_id = node_id
    self.update_fn = update_fn
    self.interval_seconds = interval_seconds
    self._stop_event = threading.Event()

    # Child logger
    self._logger = logger.getChild(f"{name}-{node_id}")


  def run(self) -> None:
    while not self._stop_event.is_set():
      # Call function
      try:
        self.update_fn(self.node_id)
      except Exception as ex:
        self._logger.exception(f"Heartbeat update failed for node_id={self.node_id}")

      # Wait
      self._stop_event.wait(self.interval_seconds)


  def stop(self) -> None:
    self._stop_event.set()
