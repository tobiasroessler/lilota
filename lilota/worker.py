from datetime import datetime, timedelta, timezone
from typing import Callable, Type, Optional, Any
from multiprocessing import cpu_count
from lilota.node import LilotaNode, NodeHeartbeatTask
from lilota.models import NodeType, TaskProgress, RegisteredTask
from lilota.stores import SqlAlchemyNodeStore, SqlAlchemyNodeLeaderStore, SqlAlchemyTaskStore
from lilota.runner import TaskRunner
from lilota.heartbeat import Heartbeat, HeartbeatTask
import logging
from uuid import uuid4



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
      self.is_leader = self._node_leader_store.renew_leadership(self._node_id)

      if not self._is_leader:
        self._logger.debug(f"Leadership lost")

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



class TaskHeartbeatTask(HeartbeatTask):

  def __init__(self, interval: float, max_interval: float, node_id: uuid4, task_store: SqlAlchemyTaskStore, runner: TaskRunner, logger: logging.Logger):
    super().__init__(interval)
    self._max_interval = max_interval
    self._node_id = node_id
    self._task_store = task_store
    self._runner = runner
    self._logger = logger


  def execute(self):
    # Get the next available task
    task = self._task_store.get_next_task(self._node_id)
    if task:
      # Set the interval
      self.interval = 0.1

      # Execute the task
      self._runner.schedule(task)
    else:
      # Increase the interval
      self.interval = min(self.interval * 2, self._max_interval)



class LilotaWorker(LilotaNode):

  LOGGER_NAME = "lilota.worker"

  def __init__(
    self,
    db_url: str,
    heartbeat_interval: float = 5.0,
    node_timeout_sec: int = 20,
    task_heartbeat_interval: float = 0.1,
    max_task_heartbeat_interval: float = 5.0,
    number_of_processes=cpu_count(),
    set_progress_manually=False,
    logging_level=logging.INFO,
    **kwargs):

    super().__init__(
      db_url=db_url,
      node_type=NodeType.WORKER,
      heartbeat_interval=heartbeat_interval,
      node_timeout_sec=node_timeout_sec,
      logger_name=self.LOGGER_NAME,
      logging_level=logging_level,
      **kwargs,
    )

    self._task_heartbeat_interval = task_heartbeat_interval
    self._max_task_heartbeat_interval = max_task_heartbeat_interval
    self._number_of_processes = number_of_processes
    self._set_progress_manually = set_progress_manually
    self._registry = {}

    # Initialize the task runner (must be done here otherwise register will not work!!!)
    self._runner = TaskRunner(
      self._db_url,
      self._runtime.queue,
      self._logging_level,
      self._logger,
      number_of_processes=self._number_of_processes,
      set_progress_manually=self._set_progress_manually,
    )


  def _register(
    self,
    name: str,
    func: Callable,
    *,
    input_model: Optional[Type[Any]] = None,
    output_model: Optional[Type[Any]] = None,
    task_progress: Optional[TaskProgress] = None
  ):
    if name in self._registry:
      raise RuntimeError(f"Task {name!r} is already registered")
    
    if not task_progress is None and not isinstance(task_progress, type(TaskProgress)):
      raise TypeError("task_progress must be of type TaskProgress")

    # Register the task
    task = RegisteredTask(
      func=func,
      input_model=input_model,
      output_model=output_model,
      task_progress=task_progress
    )

    self._runner.register(name, task)
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


  def _on_start(self):
    # Start the task runner
    self._runner.start(self._node_id)


  def _on_started(self):
    # Create node leader store
    node_leader_store = SqlAlchemyNodeLeaderStore(self._db_url, self._logger, self._node_timeout_sec)

    # Start worker heartbeat thread
    heartbeat_task = WorkerHeartbeatTask(
      self._heartbeat_interval, 
      self._node_id, 
      self._node_timeout_sec, 
      self._node_store,
      node_leader_store,
      self._logger
    )
    self._heartbeat = Heartbeat(f"scheduler_heartbeat_{self._node_id}", heartbeat_task, self._logger)
    self._heartbeat.start()

    # Start task heartbeat thread
    task_heartbeat_task = TaskHeartbeatTask(
      self._task_heartbeat_interval,
      self._max_task_heartbeat_interval,
      self._node_id,
      self._task_store,
      self._runner,
      self._logger
    )
    self._task_heartbeat = Heartbeat(f"task_heartbeat_{self._node_id}", task_heartbeat_task, self._logger)
    self._task_heartbeat.start()

    # Log Node started message
    self._logger.debug("Worker started")


  def _on_stop(self):
    # Stop task heartbeat thread
    self._stop_task_heartbeat()

    # Stop worker heartbeat thread
    self._stop_node_heartbeat()

    # Stop the task runner
    try:
      self._runner.stop()
    except Exception as ex:
      self._logger.exception("Error when stopping the task runner")


  def _stop_task_heartbeat(self):
    if self._task_heartbeat:
      self._task_heartbeat.stop_and_join(timeout=self._heartbeat_join_timeout_in_sec)
      self._task_heartbeat = None
