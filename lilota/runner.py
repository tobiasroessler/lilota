from multiprocessing import Process, Queue, Lock, cpu_count
from .stores import TaskStoreBase
from .models import TaskInfo
from datetime import datetime, UTC

_lock = Lock()

def _execute(queue: Queue, registrations: dict[str, any], sentinel: str, store: TaskStoreBase):
  # We get the tasks from the queue. If the sentinel is send we stop.
  try:
    for id, name in iter(queue.get, sentinel):
      # We try to get the registered class and initialize the task
      _lock.acquire()
      try:
        # Load task infos from the store
        task_info: TaskInfo = store.get_by_id(id)

        # Instantiate the task that should be executed
        task = registrations[name](task_info, store.set_progress, store.set_output)
      except Exception as ex:
        # TODO: Add logging
        pass
      finally:
        _lock.release()

      # Run the task
      try:
        store.set_start(task_info.id)
        task.run()
        store.set_end(task_info.id)
      except Exception as ex:
        pass
  except Exception as ex:
    pass


class TaskRunner():

  SENTINEL = "STOP"
  
  def __init__(self, store, number_of_processes: int = cpu_count()):
    if not isinstance(number_of_processes, int):
      raise TypeError("'number_of_processes' is not of type int")
    
    self._number_of_processes = min(number_of_processes, cpu_count())
    self._store = store
    self._registrations: dict[str, any] = {}
    self._input_queue = None
    self._processes: list[Process] = []
    self._is_started = False


  def register(self, name, clazz):
    # Check that the task runner is not started
    _lock.acquire()
    try:
      if self._is_started:
        raise Exception("It is not allowed to register classes after the task runner was started. Stop the runner first using the stop() method.")
    finally:
      _lock.release()

    # Register the class
    _lock.acquire()
    try:
      self._registrations[name] = clazz
    except Exception as ex:
      # TODO: Add logging
      pass
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

    _lock.acquire()
    try:
      # Start the processes
      self._is_started = True
      for _ in range(self._number_of_processes):
        p = Process(target=_execute, args=(self._input_queue, self._registrations, self.SENTINEL, self._store), daemon=True)
        p.start()
        self._processes.append((p, _execute))
    except Exception as ex:
      # TODO: Add logging
      pass
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

    # Save the task infos in the store
    id = self._store.insert(name, description, args)

    # Add infos to the queue
    _lock.acquire()
    try:
      self._input_queue.put((id, name))
    except Exception as ex:
      # TODO: Add logging
      pass
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
      for _ in range(len(self._processes)):
        self._input_queue.put(self.SENTINEL)
    except Exception as ex:
      # TODO: Add logging
      pass
    finally:
      _lock.release()

    # Afterwards we wait until all processes are done
    _lock.acquire()
    try:
      for p, _ in self._processes:
        p.join()

      self._input_queue = None
      self._processes.clear()
      self._is_started = False
    except Exception as ex:
      # TODO: Add logging
      pass
    finally:
      _lock.release()
