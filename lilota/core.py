from abc import ABC, abstractmethod
from typing import Callable, Type, Optional, Dict, Any
from multiprocessing import cpu_count, Queue
from lilota.models import NodeType, NodeStatus, TaskProgress, RegisteredTask
from lilota.stores import SqlAlchemyTaskStore
from lilota.runner import TaskRunner
from lilota.heartbeat import HeartbeatThread
from lilota.db.alembic import upgrade_db
from lilota.logging import configure_logging_listener, configure_logging, configure_logging_with_dbhandler, create_context_logger
import logging
from uuid import uuid4


class LilotaNode(ABC):

  def __init__(
    self,
    *,
    db_url: str,
    node_type: NodeType,
    heartbeat_interval_sec: int = 10,
    logger_name: str,
    logging_level):

    self._db_url = db_url
    self._node_type = node_type
    self._heartbeat_interval_sec = heartbeat_interval_sec
    self._node_id = None
    self._store = None
    self._heartbeat = None
    self._logging_queue = Queue()
    self._logger_name = logger_name
    self._logging_level = logging_level
    self._logger: logging.Logger = None


  def start(self):
    # Start logging
    self._on_start_logging()

    if not self._node_id:
      # Upgrade the database
      try:
        upgrade_db(self._db_url)
      except Exception as ex:
        self._logger.exception(ex)
        raise ex

      # Create the store
      self._store = SqlAlchemyTaskStore(self._db_url)

      # Create node with status STARTING
      self._node_id = self._store.create_node(self._node_type, NodeStatus.STARTING)

      # Create a context logger
      self._logger = create_context_logger(self._logger, node_id=self._node_id)
    else:
      # Change status to STARTING
      self._store.update_node_status(self._node_id, NodeStatus.STARTING)

    # Start additional stuff
    self._on_start()

    # Change status to RUNNING
    self._store.update_node_status(self._node_id, NodeStatus.RUNNING)

    # Start heartbeat thread
    self._heartbeat = HeartbeatThread(
      node_id=self._node_id,
      update_fn=self._store.update_node_last_seen_at,
      logger=self._logger,
      interval_seconds=self._heartbeat_interval_sec,
    )
    self._heartbeat.start()

    # Log Node started message
    self._logger.info("Node started")


  def stop(self):
    # Change status to STOPPING
    self._store.update_node_status(self._node_id, NodeStatus.STOPPING)

    # Stop additional stuff
    self._on_stop()

    # Stop heartbeat thread
    if self._heartbeat:
      self._heartbeat.stop()
      self._heartbeat.join(timeout=5)
      self._heartbeat = None

    # Change status to IDLE
    self._store.update_node_status(self._node_id, NodeStatus.IDLE)

    # Log Node stopped message
    self._logger.info("Node stopped")

    # Stop logging
    self._on_stop_logging()


  def get_nodes(self):
    return self._store.get_all_nodes()
  

  def get_node(self):
    return self._store.get_node_by_id(self._node_id) if self._node_id else None
  

  def get_task_by_id(self, id: uuid4):
    return self._store.get_task_by_id(id)
  

  def delete_task_by_id(self, id: uuid4):
    self._logger.debug(f"Delete task with id {id}")
    success = self._store.delete_task_by_id(id)
    if success:
      self._logger.debug(f"Task deleted!")
    else:
      self._logger.debug(f"Task not deleted!")
    return success
  

  @abstractmethod
  def _on_start_logging(self):
    pass


  @abstractmethod
  def _on_stop_logging(self):
    pass


  @abstractmethod
  def _on_start(self):
    pass
    # Hook for subclasses (runner start, etc.)


  @abstractmethod
  def _on_stop(self):
    pass
    # Hook for subclasses (runner stop, etc.)



class LilotaScheduler(LilotaNode):

  LOGGER_NAME = "lilota_scheduler"

  def __init__(self, db_url: str, logging_level=logging.INFO, **kwargs):
    super().__init__(
      db_url=db_url,
      node_type=NodeType.SCHEDULER,
      logger_name=self.LOGGER_NAME,
      logging_level=logging_level,
      **kwargs,
    )


  def _on_start_logging(self):
    if not self._node_id:
      configure_logging_with_dbhandler(self._db_url, self._logging_level)
      self._logger = logging.getLogger(self._logger_name)


  def _on_stop_logging(self):
    pass


  def _on_start(self):
    pass


  def _on_stop(self):
    pass


  def schedule(self, name: str, input: Any = None) -> int:
    # Save the task infos in the store
    self._logger.debug(f"Create task (name: '{name}', input: {input})")
    return self._store.create_task(name, input)



class LilotaWorker(LilotaNode):

  LOGGER_NAME = "lilota_worker"

  def __init__(
    self,
    db_url: str,
    number_of_processes=cpu_count(),
    set_progress_manually=False,
    logging_level=logging.INFO,
    **kwargs):

    super().__init__(
      db_url=db_url,
      node_type=NodeType.WORKER,
      logger_name=self.LOGGER_NAME,
      logging_level=logging_level,
      **kwargs,
    )

    self._number_of_processes = number_of_processes
    self._set_progress_manually = set_progress_manually
    self._registry = {}

    # Initialize the task runner
    self._runner = TaskRunner(
      self._db_url,
      self._logging_queue,
      self._logging_level,
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
  

  def _on_start_logging(self):
    # Start centralized logging
    self._logging_listener = configure_logging_listener(self._db_url, self._logging_queue, self._logging_level)

    # Configure logging for main process
    configure_logging(self._logging_queue, self._logging_level)
    self._logger = logging.getLogger(self._logger_name)


  def _on_stop_logging(self):
    # Stop centralized logging
    if self._logging_listener:
      self._logging_listener.stop()


  def _on_start(self):
    # Start the task runner
    self._runner.start(self._node_id)


  def _on_stop(self):
    # Stop the task runner
    self._runner.stop()



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
  