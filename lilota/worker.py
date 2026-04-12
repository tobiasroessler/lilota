from datetime import datetime, timedelta, timezone
from typing import Callable, Type, Optional, Any
from lilota.node import LilotaNode, NodeHeartbeatTask
from lilota.models import NodeType, Task, TaskProgress, RegisteredTask
from lilota.logging import create_context_logger
from lilota.stores import SqlAlchemyNodeStore, SqlAlchemyNodeLeaderStore, SqlAlchemyTaskStore
from lilota.heartbeat import Heartbeat
from lilota.utils import exception_to_dict, error_to_dict
import logging
import os
import signal
import threading
import time



class WorkerHeartbeatTask(NodeHeartbeatTask):
  """Heartbeat task used by worker nodes.

  In addition to updating the node heartbeat, this task also performs
  leader election among workers. The elected leader periodically performs
  maintenance tasks such as cleaning up stale nodes.
  """

  def __init__(self, interval: float, node_id: str, node_timeout_sec: int, node_store: SqlAlchemyNodeStore, node_leader_store: SqlAlchemyNodeLeaderStore, task_store: SqlAlchemyTaskStore, logger: logging.Logger):
    """Initialize the worker heartbeat task.

    Args:
      interval (float): Interval in seconds between heartbeats.
      node_id (str): Unique identifier of the worker node.
      node_timeout_sec (int): Timeout in seconds after which nodes are
        considered dead.
      node_store (SqlAlchemyNodeStore): Store used for node operations.
      node_leader_store (SqlAlchemyNodeLeaderStore): Store used for leader
        election and leadership renewal.
      logger (logging.Logger): Logger instance.
    """
    super().__init__(interval, node_id, node_store, logger)
    self._node_timeout_sec = node_timeout_sec
    self._node_leader_store = node_leader_store
    self._task_store = task_store
    self._is_leader = False
    

  def execute(self):
    """Execute the heartbeat logic.

    Updates the node's last-seen timestamp and attempts to perform leader
    election. If the node becomes leader, it will also trigger cleanup tasks.
    """

    # Update last_seen_at
    super().execute()

    # Try to set leader and the leader should trigger a cleanup
    try:
      self._try_set_leader_and_run_maintenance()
    except Exception:
      self._logger.exception("Leader election failed")


  def _try_set_leader_and_run_maintenance(self):
    # Try to renew leadership
    if self._is_leader:
      self._is_leader = self._node_leader_store.renew_leadership(self._node_id)

      if not self._is_leader:
        self._logger.debug(f"Leadership lost (node id: {self._node_id})")

    # Try to acquire leadership if not leader
    if not self._is_leader:
      self._is_leader = self._node_leader_store.try_acquire_leadership(self._node_id)

    # Leader-only work
    if self._is_leader:
      self._run_maintenance()


  def _run_maintenance(self) -> None:
    try:
      self._update_status_on_dead_nodes()
      self._update_status_on_expired_tasks()
      self._retry_tasks()
    except Exception:
      # Never let maintenance kill the heartbeat thread
      self._logger.exception("Node maintenance failed")


  def _update_status_on_dead_nodes(self):
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._node_timeout_sec)
    cleaned = self._node_store.update_status_on_dead_nodes(cutoff, self._node_id)
    if cleaned > 0:
      self._logger.debug(f"Marked {cleaned} stale node(s) as DEAD")


  def _update_status_on_expired_tasks(self):
    self._task_store.expire_overdue_tasks()


  def _retry_tasks(self):
    self._task_store.retry_tasks()



class LilotaWorker(LilotaNode):
  """Worker node responsible for executing scheduled tasks.

  Workers poll the task store for pending tasks, execute registered
  functions, and update task status and progress. Each worker also
  sends periodic heartbeats and participates in leader election
  for cluster maintenance.
  """

  LOGGER_NAME = "lilota.worker"
  MAX_ATTEMPTS = 1

  def __init__(
    self,
    db_url: str,
    node_heartbeat_interval: float = 5.0,
    node_timeout_sec: int = 20,
    task_heartbeat_interval: float = 0.1,
    max_task_heartbeat_interval: float = 5.0,
    set_progress_manually: bool = False,
    logging_level=logging.INFO,
    **kwargs):
    """Initialize a worker node.

    Args:
      db_url (str): Database connection URL.
      node_heartbeat_interval (float, optional): Interval in seconds between
        node heartbeats. Defaults to 5.0.
      node_timeout_sec (int, optional): Time in seconds before a node is
        considered inactive. Defaults to 20.
      task_heartbeat_interval (float, optional): Initial interval in seconds
        between polling attempts for tasks. Defaults to 0.1.
      max_task_heartbeat_interval (float, optional): Maximum polling interval
        when no tasks are available. Defaults to 5.0.
      set_progress_manually (bool, optional): User is responsible for setting
      the task progress. Defaults to False.
      logging_level (int, optional): Logging level used by the worker.
      **kwargs: Additional keyword arguments passed to ``LilotaNode``.
    """

    super().__init__(
      db_url=db_url,
      node_type=NodeType.WORKER,
      node_heartbeat_interval=node_heartbeat_interval,
      node_timeout_sec=node_timeout_sec,
      logger_name=self.LOGGER_NAME,
      logging_level=logging_level,
      **kwargs,
    )

    self._task_heartbeat_interval: float = task_heartbeat_interval
    self._max_task_heartbeat_interval: float = max_task_heartbeat_interval
    self._set_progress_manually: bool = set_progress_manually
    self._registry: dict[str, RegisteredTask] = {}


  def _register(
    self,
    name: str,
    func: Callable,
    *,
    input_model: Optional[Type[Any]] = None,
    output_model: Optional[Type[Any]] = None,
    task_progress: Optional[TaskProgress] = None,
    timeout: Optional[timedelta] = None,
    max_attempts: int = MAX_ATTEMPTS
  ):
    if self._is_started:
      raise Exception("It is not allowed to register functions after startup. Stop by using the stop() method.")

    if name in self._registry:
      raise RuntimeError(f"Task {name!r} is already registered")
    
    if task_progress is not None and not isinstance(task_progress, type(TaskProgress)):
      raise TypeError("task_progress must be of type TaskProgress")

    # Register the task
    task = RegisteredTask(
      func=func,
      input_model=input_model,
      output_model=output_model,
      task_progress=task_progress,
      timeout=timeout,
      max_attempts=max_attempts
    )

    self._registry[name] = task


  def register(
    self,
    name: str,
    *,
    input_model=None,
    output_model=None,
    task_progress=None,
    timeout=timedelta(minutes=5),
    max_attempts=MAX_ATTEMPTS
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
      timeout (Optional[timedelta]): Optional timeout that can be set for a task.
      max_attempts (int): Upper limit on how many times the task may be executed.
        Once the number of attempts reaches this value, the task will no longer be retried.

    Returns:
      Callable: A decorator that registers the function.
    """
    def decorator(func):
      self._register(
        name=name,
        func=func,
        input_model=input_model,
        output_model=output_model,
        task_progress=task_progress,
        timeout=timeout,
        max_attempts=max_attempts
      )
      return func
    return decorator


  def _on_started(self):
    # Create node leader store
    node_leader_store = SqlAlchemyNodeLeaderStore(self._db_url, self._logger, self._node_timeout_sec)

    # Start worker heartbeat thread
    heartbeat_task = WorkerHeartbeatTask(
      self._node_heartbeat_interval, 
      self._node_id, 
      self._node_timeout_sec, 
      self._node_store,
      node_leader_store,
      self._task_store,
      self._logger
    )
    self._heartbeat = Heartbeat(f"scheduler_heartbeat_{self._node_id}", heartbeat_task, self._logger)
    self._heartbeat.start()

    # Log Node started message
    self._logger.debug(f"Node started")

    # Execute tasks
    self._execute_tasks()


  def _execute_tasks(self):
    interval: float = self._task_heartbeat_interval

    while True:
      # Get the next available task
      task = self._task_store.get_next_task(self._node_id)
      if task:
        # Initialize the variables
        task_id = task.id
        interval = 0.1

        # Configure logging
        logger: logging.Logger = create_context_logger(self._logger, node_id=self._node_id, task_id=task_id)

        # Get the registered task
        registered_task = self._registry.get(task.name)
        if registered_task is None:
          error_message = f"Task {task.name!r} not registered"
          error = error_to_dict(error_message)
          self._task_store.end_task_failure(task_id, error)
          logger.error(error_message)
        else:
          # Execute the task
          try:
            # Set status to running
            timeout = registered_task.timeout
            max_attempts = registered_task.max_attempts
            started_task = self._task_store.start_task(task_id, max_attempts, timeout)

            # Set task_progress object if available
            task_progress: TaskProgress = None 
            if registered_task.task_progress is not None:
              task_progress = TaskProgress(task_id, self._task_store.set_progress)

            # Run task
            result = self._execute_task_with_watchdog(started_task, registered_task, task_progress, logger)

            # Set status to completed
            self._task_store.end_task_success(task_id, result)
          except Exception as ex:
            # Set status to failed
            self._task_store.end_task_failure(task_id, exception_to_dict(ex))
            logger.exception(f"Task execution failed (id: {task_id})")
      else:
        # Increase the interval
        interval = min(interval * 2, self._max_task_heartbeat_interval)

      # Sleep
      time.sleep(interval)


  def _execute_task_with_watchdog(self, task: Task, registered_task: RegisteredTask, task_progress: TaskProgress, logger: logging.Logger):
    if task.expires_at is None:
      return registered_task(task.input, task_progress)
    
    timer = self._calculate_timer_and_set_watchdog(task, logger)
    try:
      return registered_task(task.input, task_progress)
    finally:
      timer.cancel()


  def _calculate_timer_and_set_watchdog(self, task: Task, logger: logging.Logger) -> threading.Timer:
    timeout: float = max(0, (task.expires_at - datetime.now(timezone.utc)).total_seconds())

    def watchdog():
      # Log that process will be killed
      logger.error(f"The process will be stopped because the task '{task.name}' ({task.id}) has expired")

      # Kill the worker process
      os.kill(os.getpid(), signal.SIGKILL)

    timer = threading.Timer(timeout, watchdog)
    timer.start()
    return timer


  def _on_stop(self):
    # Stop worker heartbeat thread
    self._stop_node_heartbeat()


  def _should_set_progress_manually(self):
    return self._set_progress_manually
