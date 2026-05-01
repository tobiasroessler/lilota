import atexit
from enum import Enum
from lilota.heartbeat import Heartbeat, HeartbeatTask
from lilota.models import Node
from lilota.scheduler import LilotaScheduler
from lilota.stores import NodeStore, TaskStore
import logging
from multiprocessing import cpu_count
import os
import sys
import subprocess
import signal
from typing import Any, List
from uuid import UUID, uuid4
import threading


class ManagedProcess:
    def __init__(self, proc: subprocess.Popen, script_path: str):
        self.id = uuid4()
        self.proc = proc
        self.script_path = script_path


class ProcessManager:
    def __init__(self, on_fatal_error=None):
        """Initializes the ProcessManager.

        Args:
          on_fatal_error (Callable[[str], None] | None): Optional callback that is
            invoked when a managed process exits with an error. The callback
            receives the stderr output of the failed process.
        """
        self._on_fatal_error = on_fatal_error
        self._processes: List[ManagedProcess] = []

    def start(self, script_path: str) -> subprocess.Popen:
        """Starts a new subprocess running the given Python script.

        The process is launched using the current Python interpreter and added
        to the internal list of managed processes.

        Args:
          script_path (str): Path to the Python script to execute.

        Returns:
          subprocess.Popen: The Popen object representing the started process.
        """
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=os.setsid if os.name != "nt" else None,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )

        self._processes.append(ManagedProcess(proc, script_path))
        return proc

    def monitor(self):
        """Monitors managed processes for termination and handles restarts.

        Iterates over all managed processes and checks whether they have exited.
        If a process exits with a non-zero return code and produces stderr output,
        all processes are stopped and the fatal error callback is invoked.

        If a process exits successfully, it is automatically restarted using
        the original script path.
        """
        for mp in list(self._processes):
            proc = mp.proc
            returncode = proc.poll()

            if returncode is not None:  # process exited
                # This cannot be debugged. Use print statements because if the worker is killed also the debugger stops.
                stdout, stderr = proc.communicate()

                # Error handling in case of an exception
                # = 0: Process exited successfully
                # > 0:Process exited with an error
                # < 0: Process was terminated by a signal (Unix only)
                if returncode != 0 and stderr:
                    self._handle_error(stderr)
                    return

                # Restart process
                self._processes.remove(mp)
                self.start(mp.script_path)

    def _handle_error(self, stderr: str):
        # Stop all processes
        self.stop_all()

        # Signal to caller
        if self._on_fatal_error:
            self._on_fatal_error(stderr)

    def stop_all(self, timeout: float = 5.0):
        """Stops all managed processes gracefully, then forcefully if needed.

        Attempts to terminate all running processes. If a process does not exit
        within the specified timeout, it is forcefully killed. After stopping,
        the internal process list is cleared.

        Args:
          timeout (float): Maximum time in seconds to wait for each process to
            terminate gracefully before forcefully killing it. Defaults to 5.0.
        """
        for mp in self._processes:
            proc = mp.proc
            if proc.poll() is None:
                self._terminate(proc)

        for mp in self._processes:
            proc = mp.proc
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self._kill(proc)

        self._processes.clear()

    def _terminate(self, proc):
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)

    def _kill(self, proc):
        if os.name == "nt":
            proc.kill()
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)


class ProcessManagerTask(HeartbeatTask):
    def __init__(self, interval: float, process_manager: ProcessManager):
        super().__init__(interval, None)
        self._process_manager = process_manager

    def execute(self):
        self._process_manager.monitor()


class LilotaMode(str, Enum):
    """Execution mode for the Lilota runtime.

    Attributes:
      SCHEDULER: Run only the scheduler component.
      WORKERS: Run only worker processes.
      ALL: Run both scheduler and workers.
    """

    SCHEDULER = "scheduler"
    WORKERS = "workers"
    ALL = "all"


class Lilota:
    """High-level interface for Lilota task scheduling and worker management.

    The Lilota class coordinates a scheduler instance and a pool of worker
    processes that execute user-defined task scripts.

    A scheduler manages task persistence, distribution, and node heartbeats,
    while worker processes execute tasks defined inside the provided script.

    Workers are started as separate Python processes that run the provided
    script file as a module entry point.
    """

    @classmethod
    def scheduler(cls, db_url: str, **kwargs) -> LilotaScheduler:
        """Create a LilotaScheduler instance.

        Args:
          db_url (str): Database connection URL.
          **kwargs: Additional keyword arguments passed to the constructor.

        Returns:
          LilotaScheduler: Configured LilotaScheduler instance.
        """
        lilota = cls(
            mode=LilotaMode.SCHEDULER,
            db_url=db_url,
            script_path=None,
            number_of_workers=0,
            **kwargs,
        )
        return lilota._scheduler

    @classmethod
    def workers(cls, db_url: str, script_path: str, number_of_workers: int, **kwargs):
        """Create a Lilota instance running only worker processes.

        Args:
          db_url (str): Database connection URL.
          script_path (str): Path to the worker script.
          number_of_workers (int): Number of worker processes to spawn.
          **kwargs: Additional keyword arguments passed to the constructor.

        Returns:
          Lilota: Configured instance in worker-only mode.
        """
        return cls(
            mode=LilotaMode.WORKERS,
            db_url=db_url,
            script_path=script_path,
            number_of_workers=number_of_workers,
            **kwargs,
        )

    def __init__(
        self,
        db_url: str,
        *,
        mode: LilotaMode = LilotaMode.ALL,
        script_path: str = None,
        number_of_workers: int = cpu_count(),
        scheduler_heartbeat_interval: float = 5,
        scheduler_timeout_sec: int = 20,
        process_manager_interval: float = 1.0,
        stop_worker_timeout: int = 60,
        logging_level=logging.INFO,
    ):
        """Create a new Lilota runtime instance.

        Args:
          db_url (str):
            Database connection URL used by the scheduler.

          mode (LilotaMode, optional):
            Execution mode controlling whether the scheduler, workers,
            or both are started. Defaults to ALL.

          script_path (str):
            Path to a Python script that defines and registers Lilota tasks.
            Each worker process executes this script as its entry point.

          number_of_workers (int, optional):
            Number of worker processes to spawn. Defaults to the number of CPU cores and cannot exceed this value * 5.

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
        if mode in (LilotaMode.WORKERS, LilotaMode.ALL):
            if number_of_workers <= 0:
                raise ValueError("number_of_workers must be > 0 in WORKER or ALL mode")

            number_of_cpus = cpu_count()
            max_workers = number_of_cpus * 5
            if number_of_workers > max_workers:
                raise ValueError(
                    f"number_of_workers cannot be greater than {max_workers}"
                )

        self._db_url = db_url
        self._mode = mode
        self._script_path = script_path
        self._number_of_workers = number_of_workers
        self._process_manager_interval = process_manager_interval
        self._stop_worker_timeout = stop_worker_timeout
        self._process_manager_heartbeat: Heartbeat = None
        self._process_manager = ProcessManager(on_fatal_error=self._handle_fatal_error)
        self._error = None
        self._error_event = threading.Event()
        atexit.register(self._process_manager.stop_all)

        if mode in (LilotaMode.SCHEDULER, LilotaMode.ALL):
            self._scheduler = LilotaScheduler(
                db_url=db_url,
                node_heartbeat_interval=scheduler_heartbeat_interval,
                node_timeout_sec=scheduler_timeout_sec,
                logging_level=logging_level,
            )

        self._node_store = NodeStore(self._db_url, None)
        self._task_store = TaskStore(self._db_url, None)
        self._is_started = False

    @property
    def error(self):
        """Return the last fatal error encountered by the system.

        Returns:
          Any: The error object or message captured from a worker process,
          or None if no error has occurred.
        """
        return self._error

    def has_failed(self) -> bool:
        """Check whether a fatal error has occurred.

        Returns:
          bool: True if a fatal error has been detected, otherwise False.
        """
        return self._error_event.is_set()

    def wait_for_failure(self, timeout=None):
        """Block until a fatal error occurs or a timeout is reached.

        Args:
          timeout (float | None, optional): Maximum time in seconds to wait.
            If None, waits indefinitely.

        Returns:
          bool: True if a failure occurred before the timeout, False otherwise.
        """
        return self._error_event.wait(timeout)

    def _handle_fatal_error(self, error):
        self._error = error
        self._error_event.set()

    def start(self):
        """Start the configured Lilota components.

        Depending on the selected mode, this method:
          - Starts the scheduler (SCHEDULER, ALL)
          - Spawns worker processes (WORKER, ALL)
          - Starts process monitoring for workers (WORKER, ALL)

        After startup, the instance is ready to schedule and/or execute tasks.
        """
        if self._is_started:
            return

        if self._mode in (LilotaMode.SCHEDULER, LilotaMode.ALL):
            self._start_scheduler()

        if self._mode in (LilotaMode.WORKERS, LilotaMode.ALL):
            if not self._script_path:
                raise ValueError("script_path must be provided in WORKER or ALL mode")
            self._start_workers()
            self._start_process_manager()

        self._is_started = True

    def _start_scheduler(self):
        self._scheduler.start()

    def _start_workers(self):
        # Start scripts
        for _ in range(self._number_of_workers):
            self._process_manager.start(self._script_path)

    def _start_process_manager(self):
        # Start the process manager
        process_manager_task = ProcessManagerTask(
            interval=self._process_manager_interval,
            process_manager=self._process_manager,
        )
        self._process_manager_heartbeat = Heartbeat(
            "process_manager", process_manager_task, None
        )
        self._process_manager_heartbeat.start()

    def stop(self):
        """Stop all running Lilota components.

        This includes:
          - Stopping the scheduler (if running)
          - Stopping process monitoring
          - Gracefully terminating worker processes

        Worker processes are first asked to terminate gracefully. If a worker
        does not exit within ``stop_worker_timeout`` seconds, it is forcefully killed.
        """
        if not self._is_started:
            return

        if self._mode in (LilotaMode.SCHEDULER, LilotaMode.ALL):
            self._stop_scheduler()

        if self._mode in (LilotaMode.WORKERS, LilotaMode.ALL):
            self._stop_process_manager()
            self._stop_workers()

        self._is_started = False

    def _stop_scheduler(self):
        self._scheduler.stop()

    def _stop_workers(self):
        self._process_manager.stop_all(self._stop_worker_timeout)

    def _stop_process_manager(self):
        if self._process_manager_heartbeat:
            self._process_manager_heartbeat.stop_and_join(
                timeout=self._stop_worker_timeout
            )
            self._process_manager_heartbeat = None

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
