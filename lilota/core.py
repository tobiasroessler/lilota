import importlib.util
from lilota.heartbeat import Heartbeat, HeartbeatTask
from lilota.logging import configure_logging
from lilota.models import Node
from lilota.scheduler import LilotaScheduler
from lilota.stores import SqlAlchemyNodeStore, SqlAlchemyTaskStore
import logging
from multiprocessing import cpu_count, Queue, Process
import os
from queue import Empty
import sys
import traceback
from typing import Any, Callable
from uuid import UUID, uuid4



def _run_script(script_path: str, error_queue: Queue) -> None:
  try:
    spec = importlib.util.spec_from_file_location("__main__", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
  except Exception as ex:
    error_queue.put(
      {
        "pid": os.getpid(),
        "type": type(ex).__name__,
        "message": str(ex),
        "traceback": traceback.format_exc()
      }
    )
    sys.exit(1)



class WorkerProcess(Process):

  def __init__(self, id: UUID, group = None, target = None, name = None, args = (), kwargs = {}, *, daemon = None):
    super().__init__(group, target, name, args, kwargs, daemon=daemon)
    self.id = id



class ProcessManagerTask(HeartbeatTask):
  """
  Periodic task responsible for monitoring child processes and propagating
  exceptions raised inside them to the parent process.

  The task checks a shared error queue for exceptions reported by child
  processes. If an error is found, it reconstructs and raises the exception
  in the parent process context so it can be handled or terminate execution.
  """

  def __init__(self, interval: float, processes: list[Process], error_queue: Queue, process_factory: Callable[[], WorkerProcess]):
    """
    Initialize the process manager task.

    Args:
        interval (float): Time interval (in seconds) between task executions.
        processes (list[Process]): List of managed child processes.
        error_queue (Queue): Queue used by child processes to report
            exceptions back to the parent process. Each entry is expected
            to contain a dictionary with exception metadata.
        process_factory (Callable[[], Process]): Function that creates a new worker
            process and returns it.
    """
    super().__init__(interval)
    self._processes: list[WorkerProcess] = processes
    self._error_queue: Queue = error_queue
    self._process_factory = process_factory


  def execute(self):
    """
    Execute one monitoring cycle.

    Attempts to retrieve an error report from the error queue without
    blocking. If an error is present, it will be reconstructed and raised
    in the parent process via `raise_child_exception`.
    The processes are also monitored. If a process is no longer alive, 
    it is removed from the list and a new worker process is started.
    """
    self._retrieve_error()
    self._monitor_processes()


  def _retrieve_error(self):
    try:
      error = self._error_queue.get_nowait()
      self.raise_child_exception(error)
    except Empty:
      pass


  def _monitor_processes(self):
    for i, p in enumerate(self._processes):
      if p.exitcode is not None:
        new_process = self._process_factory()
        self._processes[i] = new_process
        new_process.start()
    

  def raise_child_exception(self, error_info):
    """
    Reconstruct and raise an exception originating from a child process.

    The error information is expected to be a dictionary containing:
        - "type": Name of the exception class
        - "message": Original exception message
        - "traceback": Serialized traceback string from the child process

    The method attempts to recreate the exception using the built-in
    exception class matching the provided type name. If the exception
    type cannot be resolved, a RuntimeError is used as a fallback.

    Args:
        error_info (dict): Dictionary containing exception metadata from
            a child process.

    Raises:
        Exception: Reconstructed exception with the original message and
        appended child traceback.
    """
    exc_type = error_info["type"]
    exc_message = error_info["message"]
    tb = error_info["traceback"]

    # Create a new exception instance with the same message
    # Fallback to RuntimeError if type unknown
    try:
      # Get exception class from builtins
      exc_cls = getattr(__builtins__, exc_type)
    except AttributeError:
      exc_cls = RuntimeError

    # Raise it with original message, attach traceback
    raise exc_cls(f"{exc_message}\nChild traceback:\n{tb}")



class Lilota():
  """High-level interface for Lilota task scheduling and worker management.

  The Lilota class coordinates a scheduler instance and a pool of worker
  processes that execute user-defined task scripts.

  A scheduler manages task persistence, distribution, and node heartbeats,
  while worker processes execute tasks defined inside the provided script.

  Workers are started as separate Python processes that run the provided
  script file as a module entry point.
  """

  def __init__(self, db_url: str, script_path: str, number_of_workers: int = cpu_count(), scheduler_heartbeat_interval: float = 5, scheduler_timeout_sec: int = 20, process_manager_interval: float = 1.0, stop_worker_timeout: int = 60, logging_level = logging.INFO):
    """Create a new Lilota runtime instance.

    Args:
      db_url (str):
        Database connection URL used by the scheduler.

      script_path (str):
        Path to a Python script that defines and registers Lilota tasks.
        Each worker process executes this script as its entry point.

      number_of_workers (int, optional):
        Number of worker processes to spawn. Defaults to the number of CPU cores.

      scheduler_heartbeat_interval (float, optional):
        Interval in seconds between scheduler node heartbeats.
        Defaults to 5 seconds.

      scheduler_timeout_sec (int, optional):
        Time in seconds after which a node is considered inactive
        if no heartbeat is received. Defaults to 20 seconds.

      stop_worker_timeout (int, optional):
        Maximum time in seconds to wait for worker processes to
        exit gracefully before they are forcefully killed.
        Defaults to 60 seconds.

      logging_level (int, optional):
        Logging level used by the scheduler. Defaults to logging.INFO.
    """
    self._db_url = db_url
    self._script_path = script_path
    self._number_of_workers = number_of_workers
    self._process_manager_interval = process_manager_interval
    self._stop_worker_timeout = stop_worker_timeout
    self._error_queue: Queue = None
    self._processes: list[WorkerProcess] = []
    self._process_manager_heartbeat: Heartbeat = None
    
    self._scheduler = LilotaScheduler(
      db_url=db_url,
      node_heartbeat_interval=scheduler_heartbeat_interval,
      node_timeout_sec=scheduler_timeout_sec,
      logging_level=logging_level
    )

    self._node_store = SqlAlchemyNodeStore(self._db_url, None)
    self._task_store = SqlAlchemyTaskStore(self._db_url, None)
    self._is_started = False


  def start(self):
    """Start the scheduler and spawn worker processes.

    This method:
      1. Starts the Lilota scheduler.
      2. Launches worker processes that execute the configured task script.
      3. Monitors the started processes.

    After startup, the instance is ready to schedule and process tasks.
    """
    self._error_queue = Queue()
    self._start_scheduler()
    self._start_workers()
    self._start_process_manager()
    self._is_started = True


  def _start_scheduler(self):
    self._scheduler.start()


  def _start_workers(self):
    # Start processes
    for _ in range(self._number_of_workers):
      p = self._create_process()
      p.start()
      self._processes.append(p)


  def _create_process(self):
    return WorkerProcess(
      id=uuid4(),
      target=_run_script,
      args=(self._script_path, self._error_queue)
    )


  def _start_process_manager(self):
    # Start the process manager
    process_manager_task = ProcessManagerTask(
      interval=self._process_manager_interval,
      processes=self._processes,
      error_queue=self._error_queue,
      process_factory=self._create_process
    )
    self._process_manager_heartbeat = Heartbeat(f"process_manager", process_manager_task, None)
    self._process_manager_heartbeat.start()


  def stop(self):
    """Stop the scheduler and terminate all worker processes.

    Worker processes are first asked to terminate gracefully. If a worker
    does not exit within ``stop_worker_timeout`` seconds, it will be
    forcefully killed.

    After completion, all worker processes are cleaned up and removed
    from the internal process list.
    """
    self._stop_scheduler()
    self._stop_process_manager()
    self._stop_workers()
    self._is_started = False


  def _stop_scheduler(self):
    self._scheduler.stop()


  def _stop_workers(self):
    for p in self._processes:
      if p.is_alive():
        # Ask process to terminate nicely
        p.terminate()

    # Give them a chance to exit
    for p in self._processes:
      p.join(self._stop_worker_timeout)

    # Force kill if still alive (usually not needed, but safe)
    for p in self._processes:
      if p.is_alive():
        p.kill()
        p.join()

    # Remove all processes from the list
    self._processes.clear()


  def _stop_process_manager(self):
    # Stop the process manager
    if self._process_manager_heartbeat:
      self._process_manager_heartbeat.stop_and_join(timeout=self._stop_worker_timeout)
      self._process_manager_heartbeat = None

    # Close the error queue
    if self._error_queue:
      self._error_queue.close()
      self._error_queue.join_thread()
      self._error_queue = None


  def join(self):
    """Block until all worker processes have finished.

    This is typically used in long-running worker setups where the main
    process should wait until workers exit.
    """
    for p in self._processes:
      if p.is_alive():
        p.join()


  def schedule(self, name: str, input: Any = None) -> int:
    """Schedule a task for execution.

    Args:
      name (str): Name of the registered task.
      input (Any, optional): Input payload for the task. Defaults to None.

    Returns:
      int: Identifier of the scheduled task.
    """
    return self._scheduler.schedule(name, input)
  

  def get_all_nodes(self) -> list[Node]:
    """Retrieve all registered scheduler nodes.

    Returns:
        list[Node]: A list containing all nodes currently stored in the
        node store.
    """
    return self._node_store.get_all_nodes()


  def get_task_by_id(self, id: UUID):
    """Retrieve a task by its unique identifier.

    Args:
      id (UUID): Unique task identifier.

    Returns:
      Any: Task object associated with the given ID.
    """
    return self._task_store.get_task_by_id(id)
  