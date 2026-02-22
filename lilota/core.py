from abc import ABC, abstractmethod
from typing import Callable, Type, Optional, Dict, Any
from multiprocessing import cpu_count
from lilota.models import NodeType, NodeStatus, TaskProgress, RegisteredTask
from lilota.stores import SqlAlchemyNodeStore, SqlAlchemyTaskStore
from lilota.runner import TaskRunner
from lilota.heartbeat import Heartbeat, HeartbeatTask
from lilota.db.alembic import upgrade_db
from lilota.logging import configure_logging, create_context_logger, LoggingRuntime
import logging
from uuid import uuid4



class NodeHeartbeatTask(HeartbeatTask):

  def __init__(self, interval: float, node_id: str, node_store: SqlAlchemyNodeStore, logger: logging.Logger):
    super().__init__(interval)
    self._node_id = node_id
    self._node_store = node_store
    self._logger = logger


  def execute(self):
    # Update last_seen_at
    try:
      self._node_store.update_node_last_seen_at(self._node_id)
    except Exception:
      self._logger.exception(f"Heartbeat update failed for node_id '{self._node_id}'")



class LilotaNode(ABC):

  def __init__(
    self,
    *,
    db_url: str,
    node_type: NodeType,
    heartbeat_interval: float,
    node_timeout_sec: int,
    logger_name: str,
    logging_level):

    self._db_url = db_url
    self._node_type = node_type
    self._heartbeat_interval = heartbeat_interval
    self._heartbeat_join_timeout_in_sec = 60
    self._node_timeout_sec = node_timeout_sec
    self._node_id = None
    self._node_store = None
    self._task_store = None
    self._heartbeat: Heartbeat = None
    self._logger_name = logger_name
    self._logging_level = logging_level

    # Upgrade the database
    upgrade_db(self._db_url)

    # Setup logging
    self._logging_runtime = LoggingRuntime(db_url, logging_level)
    self._runtime = self._logging_runtime
    self._logger = configure_logging(self._runtime.queue, logging_level)


  def start(self):
    # Start logging first
    self._logging_runtime.start()

    if not self._node_id:
      # Create stores
      self._node_store = SqlAlchemyNodeStore(self._db_url, self._logger)
      self._task_store = SqlAlchemyTaskStore(self._db_url, self._logger)

      # Create node with status STARTING
      self._node_id = self._node_store.create_node(self._node_type, NodeStatus.STARTING)

      # Create a context logger
      self._logger = create_context_logger(self._logger, node_id=self._node_id)
    else:
      # Change status to STARTING
      self._node_store.update_node_status(self._node_id, NodeStatus.STARTING)

    # Start additional stuff
    self._on_start()

    # Change status to RUNNING
    self._node_store.update_node_status(self._node_id, NodeStatus.RUNNING)

    # On started
    self._on_started()


  def stop(self):
    # Change status to STOPPING
    self._node_store.update_node_status(self._node_id, NodeStatus.STOPPING)

    # Stop additional stuff
    self._on_stop()

    # Change status to IDLE
    self._node_store.update_node_status(self._node_id, NodeStatus.IDLE)

    # Log Node stopped message
    self._logger.debug("Node stopped")

    # Stop logging runtime after final log
    self._logging_runtime.stop()


  def get_nodes(self):
    return self._node_store.get_all_nodes()
  

  def get_node(self):
    return self._node_store.get_node_by_id(self._node_id) if self._node_id else None
  

  def get_task_by_id(self, id: uuid4):
    return self._task_store.get_task_by_id(id)
  

  def delete_task_by_id(self, id: uuid4):
    self._logger.debug(f"Delete task with id {id}")
    success = self._task_store.delete_task_by_id(id)
    if success:
      self._logger.debug(f"Task deleted!")
    else:
      self._logger.debug(f"Task not deleted!")
    return success
  

  def _stop_node_heartbeat(self):
    # Stop heartbeat thread
    if self._heartbeat:
      self._heartbeat.stop_and_join(timeout=self._heartbeat_join_timeout_in_sec)
      self._heartbeat = None


  @abstractmethod
  def _on_start(self):
    pass


  @abstractmethod
  def _on_started(self):
    pass


  @abstractmethod
  def _on_stop(self):
    pass



class Lilota:

  def __init__(self, db_url: str, number_of_processes = cpu_count(), set_progress_manually: bool = False):
    self._registry: Dict[str, RegisteredTask] = {}

    # Upgrade the database
    upgrade_db(db_url)

    # Initialize the task runner
    self._runner = TaskRunner(db_url, number_of_processes=number_of_processes, set_progress_manually=set_progress_manually)


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
  

  def start(self):
    self._runner.start()


  def schedule(self, name: str, input_model: Any = None) -> int:
    return self._runner.add(name, input_model)


  def stop(self):
    self._runner.stop()
  

  def get_task_by_id(self, id: uuid4):
    return self._runner._store.get_task_by_id(id)
  

  def delete_task_by_id(self, id: uuid4):
    return self._runner._store.delete_task_by_id(id)
  