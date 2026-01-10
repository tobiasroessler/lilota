from multiprocessing import Process, Queue, Lock, cpu_count
from typing import Any
import threading
from lilota.models import Task, RegisteredTask, TaskProgress
from logging.handlers import QueueHandler
from lilota.stores import SqlAlchemyTaskStore
import logging


_lock = Lock()


def _execute(queue: Queue, registrations: dict[str, RegisteredTask], sentinel: str, db_url: str, logging_queue: Queue, logging_level, set_progress_manually: bool):
  logger = logging.getLogger("lilota")
  logger.addHandler(QueueHandler(logging_queue))
  logger.setLevel(logging_level)
  store = SqlAlchemyTaskStore(db_url=db_url, set_progress_manually=set_progress_manually)
  
  # We get the tasks from the queue. If the sentinel is send we stop.
  try:
    for id, name in iter(queue.get, sentinel):
      try:
        logger.debug(f"Instantiate the task using the id {id} and the registered name '{name}'")

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


def logging_thread(queue: Queue):
  logger = logging.getLogger("lilota")
  
  while True:
    message = queue.get()
    if message is None:
      break
    logger.handle(message)



class TaskRunner():

  SENTINEL = "STOP"
  LOGGER_NAME = "lilota"
  LOGGING_FORMATTER_DEFAULT = "%(asctime)s [PID %(process)d]: [%(levelname)s] - %(message)s"
  

  def __init__(self, db_url: str, number_of_processes: int = cpu_count(), set_progress_manually: bool = False, logging_formatter = LOGGING_FORMATTER_DEFAULT, logging_level = logging.DEBUG):
    if not isinstance(number_of_processes, int):
      raise TypeError("'number_of_processes' is not of type int")
    
    self._db_url = db_url
    self._number_of_processes = min(number_of_processes, cpu_count())
    self._set_progress_manually = set_progress_manually
    self._store = SqlAlchemyTaskStore(db_url)
    self._registrations: dict[str, RegisteredTask] = {}
    self._input_queue = None
    self._logger = logging.getLogger(self.LOGGER_NAME)
    self._logging_queue = None
    self._logging_thread: threading.Thread = None
    self._logging_formatter: str = logging_formatter
    self._logging_level = logging_level
    self._processes: list[Process] = []
    self._is_started = False


  def register(self, name: str, registered_task: RegisteredTask):
    # Check that the task runner is not started
    if self._is_started:
      raise Exception("It is not allowed to register functions after the task runner was started. Stop the runner first using the stop() method.")
    
    # Register the function
    self._registrations[name] = registered_task


  def start(self):
    # Check if the task runner is already started
    if self._is_started:
      raise Exception("The task runner is already started")

    # Acquire lock
    _lock.acquire()

    try:
      # Start the logging thread
      self._start_logging()
      
      # Initialize the queue
      self._input_queue = Queue()

      # Start the processes
      self._start_processes()
      self._is_started = True
      self._logger.info(f"lilota started ({self._number_of_processes} process(es) used)")

      # Start unfinished tasks
      self._restart_unfinished_tasks()
    except Exception as ex:
      self._logger.exception(str(ex))
      raise Exception(f"An error occured when starting the processes: {str(ex)}")
    finally:
      _lock.release()


  def add(self, name: str, input: Any = None) -> int:
    _lock.acquire()
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
    finally:
      _lock.release()


  def stop(self):
    # Check if task runner was started
    _lock.acquire()
    try:
      if not self._is_started:
        raise Exception("The task runner cannot be stopped because it was not started")
    finally:
      _lock.release()

    # Send the sentinel to each of the processes
    _lock.acquire()
    try:
      # Stop all other processes
      self._logger.debug(f"Stop all processes")
      for _ in range(len(self._processes)):
        self._input_queue.put(self.SENTINEL)
      self._logger.debug(f"All processes stopped")
    except Exception as ex:
      self._logger.exception(str(ex))
    finally:
      _lock.release()

    # Afterwards we wait until all processes are done
    _lock.acquire()
    try:
      # Wait for processes
      for p, _ in self._processes:
        p.join()

      # Stop logging thread
      self._logging_queue.put(None)
      self._logging_thread.join()

      # Cleanup
      self._logging_queue = None
      self._input_queue = None
      self._logging_thread = None
      self._processes.clear()
      self._is_started = False
    except Exception as ex:
      self._logger.exception(str(ex))
    finally:
      self._logger.info("lilota stopped")
      _lock.release()


  def _start_logging(self):
    self._logging_queue = Queue()
    self._logging_thread = threading.Thread(target=logging_thread, args=(self._logging_queue,))
    self._logging_thread.start()
    file_handler = logging.FileHandler("lilota.log")
    self._logger.addHandler(file_handler)
    formatter = logging.Formatter(self._logging_formatter)
    file_handler.setFormatter(formatter)
    self._logger.setLevel(self._logging_level)


  def _start_processes(self):
    for _ in range(self._number_of_processes):
      p = Process(target=_execute, args=(self._input_queue, self._registrations, self.SENTINEL, self._db_url, self._logging_queue, self._logging_level, self._set_progress_manually), daemon=True)
      p.start()
      self._processes.append((p, _execute))
      self._logger.debug(f"Process started (PID: {p.pid})")


  def _restart_unfinished_tasks(self):
    # Get all unfinished tasks
    unfinished_tasks = self._store.get_unfinished_tasks()

    # Schedule all tasks
    for unfinished_task in unfinished_tasks:
      # Add task infos to the queue
      self._input_queue.put((unfinished_task.id, unfinished_task.name))