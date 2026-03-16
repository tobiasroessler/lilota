from abc import ABC, abstractmethod
import threading
import logging


class HeartbeatTask(ABC):
  """Abstract base class for heartbeat tasks.

  A heartbeat task defines logic that should be executed periodically
  by a :class:`Heartbeat` thread.
  """

  def __init__(self, interval: float):
    """Initialize the heartbeat task.

    Args:
      interval (float): Interval in seconds between task executions.
    """
    self.interval = interval


  @abstractmethod
  def execute(self):
    """Execute the heartbeat logic.

    Subclasses must implement this method to define the work performed
    during each heartbeat cycle.
    """
    pass



class Heartbeat(threading.Thread):
  """Background thread that executes a heartbeat task periodically.

  The heartbeat repeatedly calls the provided :class:`HeartbeatTask`
  at the configured interval until the thread is stopped.
  """

  def __init__(self, name: str, task: HeartbeatTask, logger: logging.Logger):
    """Initialize the heartbeat thread.

    Args:
      name (str): Name of the thread.
      task (HeartbeatTask): Task executed periodically by the heartbeat.
      logger (logging.Logger): Logger used for reporting errors.
    """
    super().__init__(name=name, daemon=True)
    self._task = task
    self._logger = logger
    self._stop_event = threading.Event()


  def run(self) -> None:
    """Run the heartbeat loop.

    The loop repeatedly executes the configured heartbeat task and
    waits for the defined interval before executing it again. The
    loop stops when the stop event is triggered.
    """
    while not self._stop_event.is_set():
      # Execute
      try:
        self._task.execute()
      except Exception as ex:
        if self._logger is not None:
          self._logger.exception("Heartbeat task failed")
        else:
          raise

      # Wait
      interval = max(0.0, self._task.interval)
      self._stop_event.wait(interval)


  def stop(self) -> None:
    """Signal the heartbeat thread to stop.

    The thread will stop after the current execution cycle completes.
    """
    self._stop_event.set()


  def stop_and_join(self, timeout=None):
    """Stop the heartbeat thread and wait for it to finish.

    Args:
      timeout (float | None, optional): Maximum number of seconds to
        wait for the thread to terminate. If ``None``, wait indefinitely.
    """
    self.stop()
    self.join(timeout)
