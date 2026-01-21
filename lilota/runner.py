from multiprocessing import Process, Queue, cpu_count
from typing import Any
from lilota.logging import configure_logging, create_context_logger
from lilota.models import Task, RegisteredTask, TaskProgress
from lilota.stores import SqlAlchemyTaskStore
import logging
from uuid import uuid4


LOGGER_NAME = "task_runner"


def _execute(queue: Queue, registrations: dict[str, RegisteredTask], sentinel: str, node_id: uuid4, db_url: str, logging_queue: Queue, logging_level, set_progress_manually: bool):
  # Init logging
  configure_logging(logging_queue, logging_level)
  logger = logging.getLogger(LOGGER_NAME)

  # Create store
  store = SqlAlchemyTaskStore(db_url=db_url, set_progress_manually=set_progress_manually)
  
  # We get the tasks from the queue. If the sentinel is send we stop.
  try:
    for id, name in iter(queue.get, sentinel):
      # Create context logger
      logger = create_context_logger(logger, node_id=node_id, task_id=id)

      try:
        logger.info(f"Execute task (id: {id}, registered name: {name})")

        # Get the function to execute
        registered_task = registrations[name]

        # Start the task
        task: Task = store.start_task(id)
        
        # Execute the function
        result = _execute_task(registered_task, task, store)

        # Set the output
        store.end_task_success(task.id, result)
      except Exception as ex:
        store.end_task_failure(task.id, ex)
        logger.exception(str(ex))
  except Exception as ex:
    logger.exception(str(ex))


def _execute_task(registered_task: RegisteredTask, task: Task, store: SqlAlchemyTaskStore):
  task_progress: TaskProgress = None 
  if not registered_task.task_progress is None:
    task_progress = TaskProgress(task.id, store.set_progress)
  return registered_task(task.input, task_progress)



class TaskRunner():

  SENTINEL = "STOP"
  LOGGER_NAME = "lilota"
  LOGGING_FORMATTER_DEFAULT = "%(asctime)s [PID %(process)d]: [%(levelname)s] - %(message)s"
  

  def __init__(self, db_url: str, logging_queue: Queue, logging_level, number_of_processes: int = cpu_count(), set_progress_manually: bool = False):
    if not isinstance(number_of_processes, int):
      raise TypeError("'number_of_processes' is not of type int")
    
    self._db_url = db_url
    self._number_of_processes = min(number_of_processes, cpu_count())
    self._set_progress_manually = set_progress_manually
    self._store = SqlAlchemyTaskStore(db_url)
    self._registrations: dict[str, RegisteredTask] = {}
    self._input_queue = None
    self._logging_queue = logging_queue
    self._logging_level = logging_level
    self._logger = logging.getLogger(LOGGER_NAME)
    self._processes: list[Process] = []
    self._is_started = False


  def register(self, name: str, registered_task: RegisteredTask):
    # Check that the task runner is not started
    if self._is_started:
      raise Exception("It is not allowed to register functions after the task runner was started. Stop the runner first using the stop() method.")
    
    # Register the function
    self._registrations[name] = registered_task


  def start(self, node_id: uuid4):
    # Check if the task runner is already started
    if self._is_started:
      raise Exception("The task runner is already started")
    
    # Configure logging
    self._logger = create_context_logger(self._logger, node_id=node_id)

    try:
      # Initialize the queue
      self._input_queue = Queue()

      # Start the processes
      self._start_processes(node_id)
      self._is_started = True
      self._logger.info(f"Taskrunner started ({self._number_of_processes} process(es) used)")

      # Start unfinished tasks
      self._restart_unfinished_tasks()
    except Exception as ex:
      self._logger.exception(str(ex))
      raise Exception(f"An error occured when starting the processes: {str(ex)}")


  def add(self, name: str, input: Any = None) -> int:
    try:
      # Check that the task runner is started
      if not self._is_started:
        raise Exception("The task runner must be started first")
      
      # Save the task infos in the store
      self._logger.debug(f"Save task inside the store (name: '{name}', input: {input})")
      id = self._store.create_task(name, input)

      # Add task infos to the queue
      self._input_queue.put((id, name))

      # return the id of the task
      return id
    except Exception as ex:
      self._logger.exception(str(ex))
      raise ex


  def stop(self):
    # Check if task runner was started
    if not self._is_started:
      raise Exception("The task runner cannot be stopped because it was not started")

    # Send the sentinel to each of the processes
    try:
      # Stop all other processes
      for _ in range(len(self._processes)):
        self._input_queue.put(self.SENTINEL)
    except Exception as ex:
      self._logger.exception(str(ex))

    # Afterwards we wait until all processes are done
    try:
      # Wait for processes
      for p, _ in self._processes:
        p.join()

      # Cleanup
      self._input_queue = None
      self._processes.clear()
      self._is_started = False
    except Exception as ex:
      self._logger.exception(str(ex))
    finally:
      self._logger.info("Taskrunner stopped")


  def _start_processes(self, node_id: uuid4):
    for _ in range(self._number_of_processes):
      p = Process(target=_execute, args=(self._input_queue, self._registrations, self.SENTINEL, node_id, self._db_url, self._logging_queue, self._logging_level, self._set_progress_manually), daemon=True)
      p.start()
      self._processes.append((p, _execute))


  def _restart_unfinished_tasks(self):
    # Get all unfinished tasks
    unfinished_tasks = self._store.get_unfinished_tasks()

    # Schedule all tasks
    for unfinished_task in unfinished_tasks:
      # Add task infos to the queue
      self._input_queue.put((unfinished_task.id, unfinished_task.name))