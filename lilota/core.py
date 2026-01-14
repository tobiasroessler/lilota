from typing import Callable, Type, Optional, Dict, Any
from multiprocessing import cpu_count
from lilota.models import NodeType, NodeStatus, TaskProgress, RegisteredTask
from lilota.stores import SqlAlchemyTaskStore
from lilota.runner import TaskRunner
from lilota.db.alembic import upgrade_db
import logging
from uuid import uuid4


class LilotaScheduler:

  LOGGER_NAME = "lilota_scheduler"
  LOGGING_FORMATTER_DEFAULT = "%(asctime)s [PID %(process)d]: [%(levelname)s] - %(message)s"

  def __init__(self, db_url: str, logging_level = logging.DEBUG, logging_filename = "lilota_scheduler.log"):
    # Setup logging
    self._logger = logging.getLogger(self.LOGGER_NAME)
    self._logger.setLevel(logging_level)
    file_handler = logging.FileHandler(logging_filename)
    formatter = logging.Formatter(self.LOGGING_FORMATTER_DEFAULT)
    file_handler.setFormatter(formatter)
    self._logger.addHandler(file_handler)

    # Upgrade the database
    self._logger.debug("Upgrade database...")
    upgrade_db(db_url)
    self._logger.debug("Database upgraded!")

    # Create the store
    self._logger.debug("Create store...")
    self._store = SqlAlchemyTaskStore(db_url)
    self._logger.debug("Store created!")

    # Create a scheduler node in the database
    self.node_id = self._store.create_node(NodeType.SCHEDULER, NodeStatus.RUNNING)


  def schedule(self, name: str, input: Any = None) -> int:
    # Save the task infos in the store
    self._logger.debug(f"Create task (name: '{name}', input: {input})...")
    id = self._store.create_task(name, input)
    self._logger.debug(f"Task with id {id} created!")
    
    # Return the id of the task
    return id
  

  def get_nodes(self):
    return self._store.get_all_nodes()
  

  def get_node_by_id(self, id: uuid4):
    return self._store.get_node_by_id(id)


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



class LilotaWorker:

  LOGGER_NAME = "lilota_worker"
  LOGGING_FORMATTER_DEFAULT = "%(asctime)s [PID %(process)d]: [%(levelname)s] - %(message)s"

  def __init__(self, db_url: str, number_of_processes = cpu_count(), set_progress_manually: bool = False, logging_level = logging.DEBUG, logging_filename = "lilota_scheduler.log"):
    # Initialize the registry
    self._registry: Dict[str, RegisteredTask] = {}

    # Setup logging
    self._logger = logging.getLogger(self.LOGGER_NAME)
    self._logger.setLevel(logging_level)
    file_handler = logging.FileHandler(logging_filename)
    formatter = logging.Formatter(self.LOGGING_FORMATTER_DEFAULT)
    file_handler.setFormatter(formatter)
    self._logger.addHandler(file_handler)

    # Upgrade the database
    self._logger.debug("Upgrade database...")
    upgrade_db(db_url)
    self._logger.debug("Database upgraded!")

    # Create the store
    self._logger.debug("Create store...")
    self._store = SqlAlchemyTaskStore(db_url)
    self._logger.debug("Store created!")

    # Create a scheduler node in the database
    self.node_id = self._store.create_node(NodeType.WORKER, NodeStatus.IDLE)

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
    self._store.update_node_status(self.node_id, NodeStatus.STARTING)
    self._runner.start()
    self._store.update_node_status(self.node_id, NodeStatus.RUNNING)


  def stop(self):
    self._store.update_node_status(self.node_id, NodeStatus.STOPPING)
    self._runner.stop()
    self._store.update_node_status(self.node_id, NodeStatus.IDLE)


  def get_node_by_id(self, id: uuid4):
    return self._store.get_node_by_id(id)



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
  