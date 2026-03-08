from lilota.models import TaskProgress
from lilota.scheduler import LilotaScheduler
from lilota.worker import LilotaWorker
import logging
from typing import Callable, Type, Optional, Any
from uuid import UUID


class Lilota():
  """High-level interface for task scheduling and execution.

  This class coordinates a LilotaScheduler and LilotaWorker instance
  to provide a unified API for registering, scheduling, and managing tasks.

  Args:
    db_url (str): Database connection URL.
    node_heartbeat_interval (float, optional): Interval in seconds between
      node heartbeats. Defaults to 5.
    node_timeout_sec (int, optional): Time in seconds before a node is
      considered inactive. Defaults to 20.
    logging_level (int, optional): Logging level used by scheduler and worker.
      Defaults to logging.DEBUG.
    task_heartbeat_interval (float, optional): Initial interval in seconds
      between task heartbeats. Defaults to 0.1.
    max_task_heartbeat_interval (float, optional): Maximum interval in seconds
      between task heartbeats. Defaults to 5.0.
    set_progress_manually (bool, optional): Whether task progress must be
      updated manually. Defaults to False.
  """

  def __init__(self, db_url: str, node_heartbeat_interval: float = 5, node_timeout_sec: int = 20, logging_level = logging.DEBUG, task_heartbeat_interval: float = 0.1, max_task_heartbeat_interval: float = 5.0, set_progress_manually: bool = False):
    self._db_url = db_url
    self._scheduler = LilotaScheduler(
      db_url=db_url,
      node_heartbeat_interval=node_heartbeat_interval,
      node_timeout_sec=node_timeout_sec,
      logging_level=logging_level
    )
    self._worker = LilotaWorker(
      db_url=db_url,
      run_in_thread=True,
      node_heartbeat_interval=node_heartbeat_interval,
      node_timeout_sec=node_timeout_sec,
      task_heartbeat_interval=task_heartbeat_interval,
      max_task_heartbeat_interval=max_task_heartbeat_interval,
      set_progress_manually=set_progress_manually,
      logging_level=logging_level
    )
    self._is_started = False


  def _register(
    self,
    name: str,
    func: Callable,
    *,
    input_model: Optional[Type[Any]] = None,
    output_model: Optional[Type[Any]] = None,
    task_progress: Optional[TaskProgress] = None
  ):
    self._worker._register(
      name=name,
      func=func,
      input_model=input_model,
      output_model=output_model,
      task_progress=task_progress
    )


  def register(
    self,
    name: str,
    *,
    input_model=None,
    output_model=None,
    task_progress=None
  ):
    """Decorator for registering a task function.

    This method allows task registration using decorator syntax.

    Example:
      @lilota.register("my_task")
      def my_task(data):
        return data

    Args:
      name (str): Unique name of the task.
      input_model (Optional[Type[Any]]): Optional input validation model.
      output_model (Optional[Type[Any]]): Optional output validation model.
      task_progress (Optional[TaskProgress]): Task progress tracking strategy.

    Returns:
      Callable: A decorator that registers the function.
    """
    def decorator(func):
      self._worker._register(
        name=name,
        func=func,
        input_model=input_model,
        output_model=output_model,
        task_progress=task_progress
      )
      return func
    return decorator


  def start(self):
    """Start the scheduler and worker.

    Initializes both the scheduler and worker components and updates
    the internal started state.
    """
    self._scheduler.start()
    self._worker.start()
    self._is_started = self._scheduler._is_started and self._worker._is_started


  def stop(self):
    """Stop the scheduler and worker.

    Gracefully stops both components and updates the internal started state.
    """
    self._scheduler.stop()
    self._worker.stop()
    self._is_started = self._scheduler._is_started and self._worker._is_started


  def schedule(self, name: str, input: Any = None) -> int:
    """Schedule a task for execution.

    Args:
      name (str): Name of the registered task.
      input (Any, optional): Input payload for the task. Defaults to None.

    Returns:
      int: Identifier of the scheduled task.
    """
    return self._scheduler.schedule(name, input)
  

  def get_task_by_id(self, id: UUID):
    """Retrieve a task by its unique identifier.

    Args:
      id (UUID): Unique task identifier.

    Returns:
      Any: Task object associated with the given ID.
    """
    return self._scheduler.get_task_by_id(id)
  