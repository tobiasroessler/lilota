import atexit
from collections import deque
from enum import Enum
from lilota.constants import (
    DEFAULT_DB_URL,
    WORKER_EXIT_CODE_TASK_EXPIRED,
    WORKER_PARENT_PID_ENV,
)
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
import traceback
from typing import Any, Callable, List
from uuid import UUID, uuid4
import threading


class WorkerErrorOutput(threading.Thread):
    """Reads what a worker process writes to stderr, for as long as the process runs.

    The stderr of a worker process is connected to a pipe, because the text is needed
    when the process fails (see ``ProcessManager.monitor``). A pipe only holds a
    limited amount of data. If nobody reads from it, the worker blocks as soon as it
    is full, in the middle of whatever task it is executing. This thread therefore
    reads every line the moment it is written.

    Each line is passed on to the stderr of the current process, so that warnings and
    tracebacks of a worker are visible where the output of the application is. The
    last lines are also kept, to be reported as the error if the process fails.
    """

    # How much of the output is kept for the error message. A worker can run for
    # weeks, so keeping everything is not an option.
    MAX_LINES = 200
    MAX_LINE_LENGTH = 8192

    # How long to wait for the rest of the output after the process has exited. The
    # pipe stays open for as long as a process started by the worker still uses it.
    JOIN_TIMEOUT = 2.0

    def __init__(self, proc: subprocess.Popen):
        """Initializes the reader. It starts reading when ``start()`` is called.

        Args:
          proc (subprocess.Popen): Worker process, started with ``stderr=PIPE``
            in text mode.
        """
        super().__init__(name=f"lilota_worker_stderr_{proc.pid}", daemon=True)
        self._stream = proc.stderr
        self._last_lines: deque[str] = deque(maxlen=self.MAX_LINES)
        self._lock = threading.Lock()

    def run(self) -> None:
        """Reads the stream until the worker process closes it."""
        try:
            # readline with a limit instead of iterating over the stream: a worker
            # that writes megabytes without a line break is read in pieces.
            while line := self._stream.readline(self.MAX_LINE_LENGTH):
                with self._lock:
                    self._last_lines.append(line)
                self._forward(line)
        except ValueError:
            # The stream was closed while reading from it
            pass
        finally:
            self._stream.close()

    def _forward(self, line: str) -> None:
        # Nothing that goes wrong here may stop the reading. sys.stderr is looked up
        # for every line, so that an application that replaces it gets the output.
        try:
            if sys.stderr is not None:
                sys.stderr.write(line)
                sys.stderr.flush()
        except Exception:
            pass

    def text(self) -> str:
        """Returns the last lines the worker process wrote to stderr.

        Meant to be called after the process has exited. Waits for the remaining
        output to be read, but not longer than ``JOIN_TIMEOUT`` seconds.

        Returns:
          str: At most the last ``MAX_LINES`` lines, or an empty string if the
          process wrote nothing to stderr.
        """
        self.join(self.JOIN_TIMEOUT)
        with self._lock:
            return "".join(self._last_lines)


class ManagedProcess:
    def __init__(self, proc: subprocess.Popen, script_path: str):
        self.id = uuid4()
        self.proc = proc
        self.script_path = script_path
        self.error_output = WorkerErrorOutput(proc)
        self.error_output.start()


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

        What the process writes to stdout goes to the stdout of the current
        process. What it writes to stderr is read by a ``WorkerErrorOutput``,
        which passes it on to the stderr of the current process.

        The process is told the ID of the current process, so that a worker
        stops itself when the current process is gone (see
        ``ParentProcessTask`` in ``lilota.worker``).

        Args:
          script_path (str): Path to the Python script to execute.

        Returns:
          subprocess.Popen: The Popen object representing the started process.
        """
        proc = subprocess.Popen(
            # -u: without it Python buffers the output of the worker whenever it does
            # not go to a terminal. The text would show up late, and never if the
            # worker is stopped before its buffer is full.
            [sys.executable, "-u", script_path],
            # stdout is not redirected. A pipe that nobody reads from blocks the
            # worker as soon as it is full.
            stdout=None,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            # Nothing in this process can stop the workers if it is killed. They have
            # to notice on their own that it is gone.
            env={**os.environ, WORKER_PARENT_PID_ENV: str(os.getpid())},
            preexec_fn=os.setsid if os.name != "nt" else None,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )

        self._processes.append(ManagedProcess(proc, script_path))
        return proc

    def monitor(self):
        """Monitors managed processes for termination and handles restarts.

        Iterates over all managed processes and checks whether they have exited.

        A process that was killed is restarted using the original script path.
        That is a worker that stopped itself because its task has expired, or a
        process that was terminated by a signal. Such a worker has not failed by
        itself, so what it has written to stderr before does not matter.

        If a process ends on its own with a non-zero return code and has produced
        stderr output, the script has failed. All processes are stopped and the
        fatal error callback is invoked.

        The same happens if a process exits successfully. A started worker runs
        until it is stopped from the outside, so a script that ends on its own
        never started its worker.

        In all other cases the process is restarted as well.
        """
        for mp in list(self._processes):
            proc = mp.proc
            returncode = proc.poll()

            if returncode is not None:  # process exited
                # This cannot be debugged. Use print statements because if the worker is killed also the debugger stops.
                stderr = mp.error_output.text()

                # = 0: Process exited successfully
                # > 0: Process exited with an error
                # < 0: Process was terminated by a signal (Unix only)
                was_killed = (
                    returncode < 0 or returncode == WORKER_EXIT_CODE_TASK_EXPIRED
                )

                # Error handling in case of an exception. Only for a process that
                # ended on its own: the stderr output of a worker is everything it
                # has written since it was started, so a killed worker that once
                # wrote a warning would be taken for a script that failed.
                if not was_killed and returncode != 0 and stderr:
                    self._handle_error(stderr)
                    return

                # A started worker never exits on its own. Restarting the script would
                # only end up in an endless loop.
                if returncode == 0:
                    self._handle_error(
                        f"The worker script '{mp.script_path}' exited without starting a worker. "
                        "Did you forget to call worker.start()?"
                    )
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

        # Let the readers finish, so that the pipes are closed
        for mp in self._processes:
            mp.error_output.join(WorkerErrorOutput.JOIN_TIMEOUT)

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
    def scheduler(cls, db_url: str = DEFAULT_DB_URL, **kwargs) -> LilotaScheduler:
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
    def workers(
        cls,
        script_path: str,
        number_of_workers: int,
        db_url: str = DEFAULT_DB_URL,
        **kwargs,
    ):
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
        db_url: str = DEFAULT_DB_URL,
        *,
        mode: LilotaMode = LilotaMode.ALL,
        script_path: str = None,
        number_of_workers: int = cpu_count(),
        scheduler_heartbeat_interval: float = 5,
        scheduler_timeout_sec: int = 20,
        process_manager_interval: float = 1.0,
        stop_worker_timeout: int = 60,
        logging_level=logging.INFO,
        on_fatal_error: Callable[[str], None] = None,
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
            The script must call ``worker.start()``. If it ends without
            starting a worker, all workers are stopped and the failure is
            reported through ``error``.

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

          on_fatal_error (Callable[[str], None], optional):
            Function that is called when a fatal error has stopped all
            workers. It gets the error, which is also what ``error``
            returns. It is called in the thread that monitors the workers,
            so it should return quickly, and it must not call ``stop()``.
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
        self._on_fatal_error = on_fatal_error
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

        The error is reset when ``start()`` starts the workers again.

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

        if self._on_fatal_error:
            try:
                self._on_fatal_error(error)
            except Exception:
                # The function belongs to the application. An error in it must not
                # end the thread that monitors the workers.
                traceback.print_exc()

    def _reset_error(self):
        self._error = None
        self._error_event.clear()

    def start(self):
        """Start the configured Lilota components.

        Depending on the selected mode, this method:
          - Starts the scheduler (SCHEDULER, ALL)
          - Spawns worker processes (WORKER, ALL)
          - Starts process monitoring for workers (WORKER, ALL)

        After startup, the instance is ready to schedule and/or execute tasks.

        After a fatal error the method starts the workers again. ``stop()`` does
        not have to be called first. The fatal error is reset: ``has_failed()``
        returns False and ``error`` returns None, until the next fatal error.
        """
        if self._is_started:
            # A fatal error has stopped the workers. Everything else is still running.
            if self.has_failed():
                self._reset_error()
                self._start_workers()
            return

        self._reset_error()

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
        id = self._scheduler.schedule(name, input)

        if self.has_failed():
            self._scheduler._logger.warning(
                f"The task '{name}' ({id}) was scheduled, but no worker can execute "
                "it: a fatal error has stopped all workers. Call start() to start "
                "them again"
            )

        return id

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
