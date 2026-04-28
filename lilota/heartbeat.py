from abc import ABC, abstractmethod
import logging
import random
import threading


class HeartbeatTask(ABC):
  """Abstract base class for heartbeat tasks.

  A heartbeat task defines logic that should be executed periodically
  by a :class:`Heartbeat` thread.
  """

  def __init__(self, interval: float, jitter: float):
    """Initialize the heartbeat task.

    Args:
      interval (float): Interval in seconds between task executions.
      jitter (float): Jitter that is used for the heartbeat interval.
    """
    self.interval = interval
    self.jitter = jitter


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
      if self._task.jitter is not None:
        jitter = random.uniform(-self._task.jitter * interval, self._task.jitter * interval)
        interval = interval + jitter
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
    if self.is_alive():
      raise RuntimeError("Heartbeat thread did not stop!")
