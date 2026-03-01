from datetime import datetime, timedelta, timezone
from typing import Callable, Type, Optional, Any
from lilota.node import LilotaNode, NodeHeartbeatTask
from lilota.models import NodeType, TaskProgress, RegisteredTask
from lilota.logging import create_context_logger
from lilota.stores import SqlAlchemyNodeStore, SqlAlchemyNodeLeaderStore
from lilota.heartbeat import Heartbeat
from lilota.utils import exception_to_dict, error_to_dict
import logging
from threading import Thread, Event



class WorkerHeartbeatTask(NodeHeartbeatTask):

  def __init__(self, interval: float, node_id: str, node_timeout_sec: int, node_store: SqlAlchemyNodeStore, node_leader_store: SqlAlchemyNodeLeaderStore, logger: logging.Logger):
    super().__init__(interval, node_id, node_store, logger)
    self._node_timeout_sec = node_timeout_sec
    self._node_leader_store = node_leader_store
    self._is_leader = False
    

  def execute(self):
    # Update last_seen_at
    super().execute()

    # Try to set leader and the leader should trigger a cleanup
    try:
      self._try_set_leader_and_cleanup()
    except Exception:
      self._logger.exception("Leader election failed")


  def _try_set_leader_and_cleanup(self):
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
      self._cleanup()


  def _cleanup(self) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._node_timeout_sec)

    try:
      cleaned = self._node_store.update_nodes_status_on_dead_nodes(cutoff, self._node_id)
      if cleaned > 0:
        self._logger.debug(f"Marked {cleaned} stale node(s) as DEAD")
    except Exception:
      # Never let cleanup kill the heartbeat thread
      self._logger.exception("Node cleanup failed")



class LilotaWorker(LilotaNode):

  LOGGER_NAME = "lilota.worker"

  def __init__(
    self,
    db_url: str,
    run_in_thread: bool,
    node_heartbeat_interval: float = 5.0,
    node_timeout_sec: int = 20,
    task_heartbeat_interval: float = 0.1,
    max_task_heartbeat_interval: float = 5.0,
    set_progress_manually=False,
    logging_level=logging.INFO,
    **kwargs):

    super().__init__(
      db_url=db_url,
      node_type=NodeType.WORKER,
      node_heartbeat_interval=node_heartbeat_interval,
      node_timeout_sec=node_timeout_sec,
      logger_name=self.LOGGER_NAME,
      logging_level=logging_level,
      **kwargs,
    )

    self._run_in_thread = run_in_thread
    self._thread: Thread = None
    self._task_heartbeat_interval = task_heartbeat_interval
    self._max_task_heartbeat_interval = max_task_heartbeat_interval
    self._set_progress_manually = set_progress_manually
    self._stop_event = Event()
    self._registry: dict[str, RegisteredTask] = {}


  def _register(
    self,
    name: str,
    func: Callable,
    *,
    input_model: Optional[Type[Any]] = None,
    output_model: Optional[Type[Any]] = None,
    task_progress: Optional[TaskProgress] = None
  ):
    if self._is_started:
      raise Exception("It is not allowed to register functions after startup. Stop by using the stop() method.")

    if name in self._registry:
      raise RuntimeError(f"Task {name!r} is already registered")
    
    if task_progress is not None and not isinstance(task_progress, TaskProgress):
      raise TypeError("task_progress must be of type TaskProgress")

    # Register the task
    task = RegisteredTask(
      func=func,
      input_model=input_model,
      output_model=output_model,
      task_progress=task_progress
    )

    self._registry[name] = task


  def register(
    self,
    name: str,
    *,
    input_model=None,
    output_model=None,
    task_progress=None
  ):
    def decorator(func):
      self._register(
        name=name,
        func=func,
        input_model=input_model,
        output_model=output_model,
        task_progress=task_progress
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
      self._logger
    )
    self._heartbeat = Heartbeat(f"scheduler_heartbeat_{self._node_id}", heartbeat_task, self._logger)
    self._heartbeat.start()

    # Log Node started message
    self._logger.debug(f"Node started")

    # Execute tasks
    if self._run_in_thread:
      self._thread = Thread(target=self._execute_tasks, daemon=True)
      self._thread.start()
    else:
      self._execute_tasks()


  def _execute_tasks(self):
    interval: float = self._task_heartbeat_interval

    while not self._stop_event.is_set():
      # Get the next available task
      task = self._task_store.get_next_task(self._node_id)
      if task:
        # Initialize the variables
        task_id = task.id
        interval = 0.1

        # Configure logging
        logger = create_context_logger(self._logger, node_id=self._node_id, task_id=task_id)

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
            started_task = self._task_store.start_task(task_id)

            # Set task_progress object if available
            task_progress: TaskProgress = None 
            if registered_task.task_progress is not None:
              task_progress = TaskProgress(task_id, self._task_store.set_progress)

            # Call the function
            result = registered_task(started_task.input, task_progress)

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
      self._stop_event.wait(interval)


  def _on_stop(self):
    # Set stop event
    self._stop_event.set()

    # Stop the worker thread
    if self._thread is not None:
      self._thread.join()

    # Stop worker heartbeat thread
    self._stop_node_heartbeat()
