from abc import ABC, abstractmethod
from typing import Callable, Type, Optional, Dict, Any
from multiprocessing import cpu_count
from lilota.models import NodeType, NodeStatus, TaskProgress, RegisteredTask
from lilota.stores import SqlAlchemyTaskStore
from lilota.runner import TaskRunner
from lilota.heartbeat import HeartbeatThread
from lilota.db.alembic import upgrade_db
import logging
from uuid import uuid4


class LilotaNode(ABC):

  LOGGING_FORMATTER_DEFAULT = "%(asctime)s [PID %(process)d]: [%(levelname)s] - %(message)s"

  def __init__(
    self,
    *,
    db_url: str,
    node_type: NodeType,
    logger_name: str,
    heartbeat_interval_sec: int = 10,
    logging_level=logging.DEBUG,
    logging_filename: str):

    self.db_url = db_url
    self.node_type = node_type
    self.heartbeat_interval_sec = heartbeat_interval_sec
    self.node_id = None
    self._store = None
    self._heartbeat = None

    self._logger = logging.getLogger(logger_name)
    self._logger.setLevel(logging_level)

    if not self._logger.handlers:
      handler = logging.FileHandler(logging_filename)
      handler.setFormatter(logging.Formatter(self.LOGGING_FORMATTER_DEFAULT))
      self._logger.addHandler(handler)


  def start(self):
    if not self.node_id:
      # Upgrade the database
      self._logger.debug("Upgrade database...")
      upgrade_db(self.db_url)
      self._logger.debug("Database upgraded!")

      # Create the store
      self._logger.debug("Create store...")
      self._store = SqlAlchemyTaskStore(self.db_url)
      self._logger.debug("Store created!")

      self.node_id = self._store.create_node(self.node_type, NodeStatus.STARTING)
    else:
      self._store.update_node_status(self.node_id, NodeStatus.STARTING)

    # Start heartbeat thread
    self._heartbeat = HeartbeatThread(
        node_id=self.node_id,
        update_fn=self._store.update_node_last_seen_at,
        logger=self._logger,
        interval_seconds=self.heartbeat_interval_sec,
    )
    self._heartbeat.start()

    # Start additional stuff
    self._on_start()

    # Change status to RUNNING
    self._store.update_node_status(self.node_id, NodeStatus.RUNNING)


  def stop(self):
    # Change status to STOPPING
    self._store.update_node_status(self.node_id, NodeStatus.STOPPING)

    # Stop additional stuff
    self._on_stop()

    # Stop heartbeat thread
    if self._heartbeat:
      self._heartbeat.stop()
      self._heartbeat = None

    # Change status to IDLE
    self._store.update_node_status(self.node_id, NodeStatus.IDLE)


  def get_nodes(self):
    return self._store.get_all_nodes()
  

  def get_node(self):
    return self._store.get_node_by_id(self.node_id) if self.node_id else None
  

  def get_task_by_id(self, id: int):
    return self._store.get_task_by_id(id)
  

  def delete_task_by_id(self, id: int):
    self._logger.debug(f"Delete task with id {id}...")
    success = self._store.delete_task_by_id(id)
    if success:
      self._logger.debug(f"Task deleted!")
    else:
      self._logger.debug(f"Task not deleted!")
    return success
  

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

  def __init__(self, db_url: str, **kwargs):
    super().__init__(
      db_url=db_url,
      node_type=NodeType.SCHEDULER,
      logger_name=self.LOGGER_NAME,
      logging_filename="lilota_scheduler.log",
      **kwargs,
    )


  def _on_start(self):
    pass


  def _on_stop(self):
    pass


  def schedule(self, name: str, input: Any = None) -> int:
    # Save the task infos in the store
    self._logger.debug(f"Create task (name: '{name}', input: {input})...")
    id = self._store.create_task(name, input)
    self._logger.debug(f"Task with id {id} created!")
    
    # Return the id of the task
    return id



class LilotaWorker(LilotaNode):

  LOGGER_NAME = "lilota_worker"

  def __init__(
    self,
    db_url: str,
    number_of_processes=cpu_count(),
    set_progress_manually=False,
    **kwargs):

    super().__init__(
      db_url=db_url,
      node_type=NodeType.WORKER,
      logger_name=self.LOGGER_NAME,
      logging_filename="lilota_worker.log",
      **kwargs,
    )

    self._registry = {}

    # Initialize the task runner
    self._runner = TaskRunner(
        db_url,
        number_of_processes=number_of_processes,
        set_progress_manually=set_progress_manually,
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
    self._runner.start()


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
  

  def get_task_by_id(self, id: int):
    return self._runner._store.get_task_by_id(id)
  

  def delete_task_by_id(self, id: int):
    return self._runner._store.delete_task_by_id(id)
  