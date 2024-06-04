from multiprocessing import Process, Queue, Lock, cpu_count
import threading
from .stores import TaskStoreBase
from .models import TaskInfo
from datetime import datetime, UTC
from logging.handlers import QueueHandler
import logging

_lock = Lock()

def _execute(queue: Queue, registrations: dict[str, any], sentinel: str, store: TaskStoreBase, logging_queue: Queue, logging_level):
  logger = logging.getLogger("lilota")
  logger.addHandler(QueueHandler(logging_queue))
  logger.setLevel(logging_level)
  
  # We get the tasks from the queue. If the sentinel is send we stop.
  try:
    for id, name in iter(queue.get, sentinel):
      # We try to get the registered class and initialize the task
      _lock.acquire()
      try:
        logger.debug(f"Instantiate the task using the id {id} and the registered name '{name}'")

        # Load task infos from the store
        task_info: TaskInfo = store.get_by_id(id)

        # Instantiate the task that should be executed
        task = registrations[name](task_info, store.set_progress, store.set_output, logger)
      except Exception as ex:
        logger.exception(str(ex))
      finally:
        _lock.release()

      # Run the task
      try:
        logger.info(f"Start task (name: '{task_info.name}', id: {task_info.id})")
        store.set_start(task_info.id)
        start_time = datetime.now()
        task.run()
        end_time = datetime.now()
        formatted_time = get_elapsed_time(start_time, end_time)
        logger.info(f"Task executed (name: '{task_info.name}', id: {task_info.id}, elapsed time: {formatted_time})")
      except Exception as ex:
        logger.exception(str(ex))
      finally:
        store.set_end(task_info.id)
  except Exception as ex:
    logger.exception(str(ex))


def get_elapsed_time(start_time, end_time):
  elapsed_time = end_time - start_time
  total_seconds = elapsed_time.total_seconds()
  return "{:02}:{:02}:{:06.3f}".format(
    int(total_seconds // 3600),  # hours
    int((total_seconds % 3600) // 60),  # minutes
    total_seconds % 60  # seconds
  )


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
  
  def __init__(self, store, number_of_processes: int = cpu_count(), logging_formatter = LOGGING_FORMATTER_DEFAULT, logging_level = logging.DEBUG):
    if not isinstance(number_of_processes, int):
      raise TypeError("'number_of_processes' is not of type int")
    
    self._number_of_processes = min(number_of_processes, cpu_count())
    self._store: TaskStoreBase = store
    self._registrations: dict[str, any] = {}
    self._input_queue = None
    self._logger = logging.getLogger(self.LOGGER_NAME)
    self._logging_queue = None
    self._logging_thread: threading.Thread = None
    self._logging_formatter: str = logging_formatter
    self._logging_level = logging_level
    self._processes: list[Process] = []
    self._is_started = False


  def register(self, name, clazz):
    
    _lock.acquire()
    try:
      # Check that the task runner is not started
      if self._is_started:
        raise Exception("It is not allowed to register classes after the task runner was started. Stop the runner first using the stop() method.")
      
      # Register the class
      self._registrations[name] = clazz
    finally:
      _lock.release()


  def start(self):
    # Check if the task runner is already started
    _lock.acquire()
    try:
      if self._is_started:
        raise Exception("The task runner is already started")
    finally:
      _lock.release()
    
    # Initialize the queue
    self._input_queue = Queue()

    # Acquire lock
    _lock.acquire()

    # Start the logging thread
    self._logging_queue = Queue()
    self._logging_thread = threading.Thread(target=logging_thread, args=(self._logging_queue,))
    self._logging_thread.start()
    file_handler = logging.FileHandler("lilota.log")
    self._logger.addHandler(file_handler)
    formatter = logging.Formatter(self._logging_formatter)
    file_handler.setFormatter(formatter)
    self._logger.setLevel(self._logging_level)
    
    try:
      # Start the processes
      self._is_started = True

      for _ in range(self._number_of_processes):
        p = Process(target=_execute, args=(self._input_queue, self._registrations, self.SENTINEL, self._store, self._logging_queue, self._logging_level), daemon=True)
        p.start()
        self._processes.append((p, _execute))
        self._logger.debug(f"Process started (PID: {p.pid})")
      self._logger.info(f"lilota started ({self._number_of_processes} process(es) used)")
    except Exception as ex:
      self._logger.exception(str(ex))
    finally:
      _lock.release()


  def add(self, name, description, args):
    # Check that the task runner is started
    _lock.acquire()
    try:
      if not self._is_started:
        raise Exception("The task runner must be started first")
    finally:
      _lock.release()

    _lock.acquire()
    try:
      # Save the task infos in the store
      self._logger.debug(f"Save task inside the store (name: '{name}', args: {args})")
      id = self._store.insert(name, description, args)

      # Add infos to the queue
      self._input_queue.put((id, name))
    except Exception as ex:
      self._logger.exception(str(ex))
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
