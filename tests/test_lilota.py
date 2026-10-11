import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from contextlib import contextmanager, redirect_stderr
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import io
import logging
import select
import signal
import subprocess
import tempfile
from unittest import TestCase, main
from typing import Any
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.core import Lilota, LilotaMode, ManagedProcess, WorkerErrorOutput
from lilota.models import (
    Node,
    NodeLeader,
    NodeStatus,
    Task,
    TaskStatus,
    LogEntry,
    NodeType,
)
from lilota.db.alembic import upgrade_db
from lilota.stores import LogStore, TaskStore
from lilota.worker import LilotaWorker
from pathlib import Path
import time
from uuid import UUID


class AddInput:
    def __init__(self, a: int, b: int) -> None:
        self.a = a
        self.b = b

    def as_dict(self) -> dict[str, Any]:
        return {
            "a": self.a,
            "b": self.b,
        }


class AddOutput:
    def __init__(self, sum: int) -> None:
        self.sum = sum

    def as_dict(self) -> dict[str, Any]:
        return {"sum": self.sum}


@dataclass
class AddInputDataclass:
    a: int
    b: int


@dataclass
class AddOutputDataclass:
    sum: int


class AddInputInvalid:
    a: int
    b: int

    def __init__(self, a: int, b: int):
        self.a = a
        self.b = b


class AddOutputInvalid:
    sum: int

    def __init__(self, sum: int):
        self.sum = sum


# What the operating system holds for a process whose output is connected to a pipe
# (Linux and macOS). A process that writes more blocks until somebody reads from the
# other end of the pipe.
PIPE_CAPACITY = 64 * 1024

OUTPUT_LINE = "This line is written by a lilota task. " * 25
MORE_LINES_THAN_A_PIPE_HOLDS = 2 * PIPE_CAPACITY // len(OUTPUT_LINE) + 1


class LilotaTestCase(TestCase):
    DB_URL = DEFAULT_TEST_DB_URL

    @classmethod
    def get_session(cls):
        engine = create_engine(cls.DB_URL)
        Session = sessionmaker(bind=engine)
        return Session()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Apply the migrations
        upgrade_db(cls.DB_URL)

    def setUp(self):
        # Create SqlAlchemy engine and session
        with LilotaTestCase.get_session() as session:
            session.query(Task).delete()
            session.query(Node).delete()
            session.query(LogEntry).delete()
            session.query(NodeLeader).delete()
            session.commit()

    def test_init___worker_mode_with_zero_workers___should_raise(self):
        # Act & Assert
        with self.assertRaises(ValueError) as ctx:
            Lilota(
                db_url=self.DB_URL,
                mode=LilotaMode.WORKERS,
                script_path="dummy.py",
                number_of_workers=0,
            )

        self.assertEqual(
            str(ctx.exception), "number_of_workers must be > 0 in WORKER or ALL mode"
        )

    def test_init___all_mode_with_zero_workers___should_raise(self):
        # Act & Assert
        with self.assertRaises(ValueError) as ctx:
            Lilota(
                db_url=self.DB_URL,
                mode=LilotaMode.ALL,
                script_path="dummy.py",
                number_of_workers=0,
            )

        self.assertEqual(
            str(ctx.exception), "number_of_workers must be > 0 in WORKER or ALL mode"
        )

    def test_init___worker_mode_with_too_many_workers___should_raise(self):
        # Arrange
        from multiprocessing import cpu_count

        max_workers = cpu_count() * 5
        too_many = max_workers + 1

        # Act & Assert
        with self.assertRaises(ValueError) as ctx:
            Lilota(
                db_url=self.DB_URL,
                mode=LilotaMode.WORKERS,
                script_path="dummy.py",
                number_of_workers=too_many,
            )

        self.assertEqual(
            str(ctx.exception),
            f"number_of_workers cannot be greater than {max_workers}",
        )

    def test_init___all_mode_with_too_many_workers___should_raise(self):
        # Arrange
        from multiprocessing import cpu_count

        max_workers = cpu_count() * 5
        too_many = max_workers + 1

        # Act & Assert
        with self.assertRaises(ValueError) as ctx:
            Lilota(
                db_url=self.DB_URL,
                mode=LilotaMode.ALL,
                script_path="dummy.py",
                number_of_workers=too_many,
            )

        self.assertEqual(
            str(ctx.exception),
            f"number_of_workers cannot be greater than {max_workers}",
        )

    def test_init___scheduler_mode_ignores_worker_validation___should_not_raise(self):
        # This should NOT raise even with invalid worker count
        lilota = Lilota(
            db_url=self.DB_URL,
            mode=LilotaMode.SCHEDULER,
            script_path=None,
            number_of_workers=0,  # allowed in scheduler-only mode
        )

        self.assertIsNotNone(lilota)

    def test_stop___start_and_directly_stop___should_shutdown_all_processes(self):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=8,
        )

        try:
            lilota.start()
            self.assertTrue(lilota._is_started)
            self.assertEqual(len(lilota._process_manager._processes), 8)
            for mp in lilota._process_manager._processes:
                self.assertIsNone(mp.proc.poll())
        except Exception:
            lilota.stop()

        # Act
        lilota.stop()

        # Assert
        self.assertFalse(lilota._is_started)
        self.assertEqual(len(lilota._process_manager._processes), 0)

    def test_schedule___add_1_hello_world_task___should_execute_task(self):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "hello_world_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("hello_world")
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep(seconds=1)
            task = lilota.get_task_by_id(id)
            self.assertEqual(task.status, TaskStatus.COMPLETED)
            self.assertIsNone(task.error)
            self.assertEqual(task.progress_percentage, 100)
            self.assertIsNone(task.input)
            self.assertIsNone(task.output)
        finally:
            lilota.stop()

    def test_schedule___add_1_task_with_only_input_model___should_execute_task(self):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("only_input_model", AddInput(a=1, b=2))
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep()
            task = lilota.get_task_by_id(id)
            self.assertEqual(task.status, TaskStatus.COMPLETED)
            self.assertIsNone(task.error)
            self.assertEqual(task.progress_percentage, 100)
            self.assertIsNotNone(task.input)
            self.assertIsNone(task.output)
        finally:
            lilota.stop()

    def test_schedule___add_1_task_with_only_output_model___should_execute_task(self):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("only_output_model")
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep(seconds=1)
            task = lilota.get_task_by_id(id)
            self.assertEqual(task.status, TaskStatus.COMPLETED)
            self.assertIsNone(task.error)
            self.assertEqual(task.progress_percentage, 100)
            self.assertIsNone(task.input)
            self.assertIsNotNone(task.output)
            result = task.output["sum"]
            self.assertEqual(3, result)
        finally:
            lilota.stop()

    def test_schedule___add_1_task_with_only_taskprogress___should_execute_task(self):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "taskprogress_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("only_taskprogress")
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep(3)
            task: Task = lilota.get_task_by_id(id)
            self.assertEqual(task.progress_percentage, 50)
        finally:
            lilota.stop()

    def test_schedule___add_1_task_using_input_and_output_model___should_calculate_the_result(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("add", AddInput(a=1, b=2))
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep()
            task = lilota.get_task_by_id(id)
            self.assertEqual(task.status, TaskStatus.COMPLETED)
            self.assertIsNone(task.error)
            self.assertEqual(task.progress_percentage, 100)
            number1 = task.input["a"]
            number2 = task.input["b"]
            result = task.output["sum"]
            self.assertEqual(number1 + number2, result)
        finally:
            lilota.stop()

    def test_schedule___with_taskprogress_object_passed___should_set_progress_correctly(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "taskprogress_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("add_with_taskprogress", AddInputDataclass(a=1, b=2))
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep()
            task: Task = lilota.get_task_by_id(id)
            self.assertEqual(task.progress_percentage, 50)
        finally:
            lilota.stop()

    def test_schedule___with_logger_object_passed___should_log_correctly(
        self,
    ):
        # Arrange
        log_store: LogStore = LogStore(self.DB_URL)
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "logger_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            lilota.schedule("only_logger")
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep(2)
            log_entries: list[LogEntry] = log_store.get_log_entries()
            self.assertEqual(len(log_entries), 1)
            self.assertEqual("Message from only_logger task", log_entries[0].message)
        finally:
            lilota.stop()

    def test_schedule___add_1_task_using_dataclasses___should_calculate_the_result(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("add_with_dataclasses", AddInputDataclass(a=1, b=2))
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep()
            task = lilota.get_task_by_id(id)
            self.assertEqual(task.status, TaskStatus.COMPLETED)
            self.assertIsNone(task.error)
            self.assertEqual(task.progress_percentage, 100)
            number1 = task.input["a"]
            number2 = task.input["b"]
            result = task.output["sum"]
            self.assertEqual(number1 + number2, result)
        finally:
            lilota.stop()

    def test_schedule___add_1_task_using_dict___should_calculate_the_result(self):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("add_with_dict", {"a": 1, "b": 2})
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep(seconds=1)
            task = lilota.get_task_by_id(id)
            self.assertEqual(task.status, TaskStatus.COMPLETED)
            self.assertIsNone(task.error)
            self.assertEqual(task.progress_percentage, 100)
            number1 = task.input["a"]
            number2 = task.input["b"]
            result = task.output["sum"]
            self.assertEqual(number1 + number2, result)
        finally:
            lilota.stop()

    def test_schedule___add_1_task_using_invalid_model___should_calculate_the_result(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()
        id = None

        # Act & Assert
        try:
            id = lilota.schedule("add_with_invalid_model", AddInputInvalid(a=1, b=2))
            self.fail("A TypeError was expected")
        except TypeError as ex:
            self.assertEqual(
                str(ex),
                "Unsupported type: AddInputInvalid. Expected ModelProtocol, dataclass, or dict.",
            )
            self.assertIsNone(id)
        finally:
            lilota.stop()

    def test_schedule___add_500_tasks___should_calculate_the_results(self):
        # Arrange
        ids = []
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=20,
        )
        lilota.start()

        # Act
        try:
            for i in range(1, 501):
                id = lilota.schedule("add", AddInput(a=i, b=i))
                ids.append(id)
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.wait(lilota)
            self.assertEqual(len(ids), 500)

            index = 0
            for id in ids:
                index = index + 1
                task = lilota.get_task_by_id(id)
                number1 = task.input["a"]
                number2 = task.input["b"]
                if task.output is None:
                    self.fail()
                result = task.output["sum"]
                self.assertEqual(number1 + number2, result)
                self.assertEqual(task.status, TaskStatus.COMPLETED)
                self.assertIsNone(task.error)
        finally:
            lilota.stop()

    def test_schedule___start_lilota_twice_and_add_5_tasks___should_calculate_the_results(
        self,
    ):
        # Arrange
        ids = []
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()
        lilota.stop()
        lilota.start()

        # Act
        try:
            for i in range(1, 6):
                id = lilota.schedule("add", AddInput(a=i, b=i))
                ids.append(id)
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.wait(lilota)
            self.assertEqual(len(ids), 5)

            index = 0
            for id in ids:
                index = index + 1
                task = lilota.get_task_by_id(id)
                number1 = task.input["a"]
                number2 = task.input["b"]
                if task.output is None:
                    self.fail()
                result = task.output["sum"]
                self.assertEqual(number1 + number2, result)
                self.assertEqual(task.status, TaskStatus.COMPLETED)
                self.assertIsNone(task.error)
        finally:
            lilota.stop()

    def test_schedule___add_1_task_but_function_raises_exception___should_log_exception(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        # Act
        try:
            id = lilota.schedule("add_with_exception", AddInput(a=1, b=2))
        except Exception:
            lilota.stop()

        # Assert
        try:
            self.sleep(3)
            task = lilota.get_task_by_id(id)
            self.assertEqual(task.status, TaskStatus.FAILED)
            self.assertIsNotNone(task.error)
            self.assertEqual(task.error["type"], "Exception")
            self.assertEqual(task.error["message"], "Error")
            self.assertEqual(task.progress_percentage, 100)
            self.assertIsNone(task.output)
        finally:
            lilota.stop()

    def test_logging___when_starting_lilota_with_two_workers___should_log_correctly(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=2,
            logging_level=logging.DEBUG,
        )
        log_store: LogStore = LogStore(self.DB_URL)

        # Act
        try:
            lilota.start()
        except Exception:
            lilota.stop()

        # Assert
        leadership_acquired = False

        try:
            self.sleep(seconds=1)
            scheduler_node = lilota._scheduler.get_node()
            worker_nodes = [
                n for n in lilota.get_all_nodes() if n.type == NodeType.WORKER
            ]
            worker_node_1: Node = worker_nodes[0]
            worker_node_2: Node = worker_nodes[1]

            log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(
                scheduler_node.id
            )
            self.assertEqual(len(log_entries), 1)
            self.assertEqual(log_entries[0].message, "Node started")

            log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(
                worker_node_1.id
            )
            self.assertEqual("Node started", log_entries[0].message)
            if len(log_entries) == 2:
                self.assertEqual(
                    f"Leadership acquired first time (node id: {worker_node_1.id})",
                    log_entries[1].message,
                )
                leadership_acquired = True

            log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(
                worker_node_2.id
            )
            self.assertEqual("Node started", log_entries[0].message)
            if len(log_entries) == 2:
                self.assertEqual(
                    f"Leadership acquired first time (node id: {worker_node_2.id})",
                    log_entries[1].message,
                )
                leadership_acquired = True

            self.assertTrue(leadership_acquired)
        finally:
            lilota.stop()

    def test_timeout___when_having_an_infinite_loop___should_stop_worker(self):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "infinite_loop_test_script.py"
            ),
            number_of_workers=1,
            logging_level=logging.DEBUG,
        )
        log_store: LogStore = LogStore(
            db_url=self.DB_URL,
        )
        lilota.start()
        process: ManagedProcess = None
        process_id: UUID = None

        # Act
        try:
            process = lilota._process_manager._processes[0]
            process_id = process.id
            self.assertIsNotNone(process)
            self.assertIsNone(process.proc.poll())
            task_id = lilota.schedule("infinite_loop")
        except Exception:
            lilota.stop()

        # Assert (debugging not possible)
        self.sleep(3)

        try:
            new_process = lilota._process_manager._processes[0]
            self.assertNotEqual(new_process.id, process_id)
            self.assertIsNone(new_process.proc.poll())
            worker: LilotaWorker = [
                n for n in lilota.get_all_nodes() if n.type == NodeType.WORKER
            ][0]
            self.assertIsNotNone(worker)
            log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(
                worker.id
            )
            self.assertEqual(len(log_entries), 1)
            self.assertEqual(
                log_entries[0].message,
                f"The process will be stopped because the task 'infinite_loop' ({task_id}) has expired",
            )
            self.assertEqual(log_entries[0].level, "ERROR")
        finally:
            lilota.stop()

    def test_exception_in_script___when_directly_failing___should_stop_worker(self):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "exception_test_script.py"
            ),
            number_of_workers=1,
            logging_level=logging.DEBUG,
        )
        self.assertIsNone(lilota.error)

        # Act
        with redirect_stderr(io.StringIO()) as worker_stderr:
            lilota.start()
            lilota.wait_for_failure()

        # Assert
        try:
            self.assertTrue(lilota.has_failed())
            self.assertIsNotNone(lilota.error)
            self.assertIn("Exception: Error in script", lilota.error)
            self.assertIn("Exception: Error in script", worker_stderr.getvalue())
            self.assertEqual(len(lilota._process_manager._processes), 0)
        finally:
            lilota.stop()

    def test_worker_not_started___when_script_does_not_start_worker___should_stop_worker(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "worker_not_started_test_script.py"
            ),
            number_of_workers=1,
            process_manager_interval=0.1,
            logging_level=logging.DEBUG,
        )
        self.assertIsNone(lilota.error)

        # Act
        lilota.start()
        lilota.wait_for_failure(timeout=5)

        # Assert
        try:
            self.assertTrue(lilota.has_failed())
            self.assertIn("worker_not_started_test_script.py", lilota.error)
            self.assertIn("worker.start()", lilota.error)
            self.assertEqual(len(lilota._process_manager._processes), 0)
        finally:
            lilota.stop()

    def test_schedule___task_writes_much_output_to_stdout___should_complete_task(self):
        # Arrange
        with self.worker_stdout_in_file() as stdout_path:
            lilota = Lilota(
                db_url=self.DB_URL,
                script_path=str(
                    Path(__file__).resolve().parent
                    / "scripts"
                    / "output_test_script.py"
                ),
                number_of_workers=1,
            )
            lilota.start()

            try:
                # Act
                id = lilota.schedule(
                    "write_to_stdout",
                    {"line": OUTPUT_LINE, "count": MORE_LINES_THAN_A_PIPE_HOLDS},
                )
                task = self.wait_for_task(lilota, id, [TaskStatus.COMPLETED])
            finally:
                lilota.stop()

            output = stdout_path.read_text()

        # Assert
        self.assertEqual(
            task.status,
            TaskStatus.COMPLETED,
            "The worker hangs because nobody reads what it writes to stdout",
        )
        self.assertEqual(output.count(OUTPUT_LINE), MORE_LINES_THAN_A_PIPE_HOLDS)

    def test_schedule___task_writes_one_line_to_stdout___should_show_line_while_worker_is_running(
        self,
    ):
        # Arrange
        line = "One line written by a lilota task"

        with self.worker_stdout_in_file() as stdout_path:
            lilota = Lilota(
                db_url=self.DB_URL,
                script_path=str(
                    Path(__file__).resolve().parent
                    / "scripts"
                    / "output_test_script.py"
                ),
                number_of_workers=1,
            )
            lilota.start()

            try:
                # Act
                id = lilota.schedule("write_to_stdout", {"line": line, "count": 1})
                task = self.wait_for_task(lilota, id, [TaskStatus.COMPLETED])

                # The worker is still running. The line must not wait in a buffer
                # of the worker until that is full or the worker is stopped.
                output = self.wait_for_text_in_file(stdout_path, line)
            finally:
                lilota.stop()

        # Assert
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertIn(line, output)

    def test_schedule___task_writes_much_output_to_stderr___should_complete_task(self):
        # Arrange
        with redirect_stderr(io.StringIO()) as worker_stderr:
            lilota = Lilota(
                db_url=self.DB_URL,
                script_path=str(
                    Path(__file__).resolve().parent
                    / "scripts"
                    / "output_test_script.py"
                ),
                number_of_workers=1,
                process_manager_interval=0.1,
            )
            lilota.start()

            try:
                # Act
                id = lilota.schedule(
                    "write_to_stderr",
                    {"line": OUTPUT_LINE, "count": MORE_LINES_THAN_A_PIPE_HOLDS},
                )
                task = self.wait_for_task(lilota, id, [TaskStatus.COMPLETED])

                # Give the process manager the time to react to the output
                self.sleep(0.5)
                has_failed = lilota.has_failed()
                running_processes = [
                    process
                    for process in lilota._process_manager._processes
                    if process.proc.poll() is None
                ]
            finally:
                lilota.stop()

        # Assert
        self.assertEqual(
            task.status,
            TaskStatus.COMPLETED,
            "The worker hangs because nobody reads what it writes to stderr",
        )
        self.assertFalse(has_failed, "Writing to stderr is not an error")
        self.assertEqual(len(running_processes), 1)
        self.assertEqual(
            worker_stderr.getvalue().count(OUTPUT_LINE), MORE_LINES_THAN_A_PIPE_HOLDS
        )

    def test_schedule___task_raises_exception___should_keep_workers_running_and_execute_next_task(
        self,
    ):
        # Arrange
        number_of_workers = 2
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "lilota_test_script.py"
            ),
            number_of_workers=number_of_workers,
            process_manager_interval=0.1,
        )
        lilota.start()

        try:
            # Act
            failed_id = lilota.schedule("add_with_exception", AddInput(a=1, b=2))
            failed_task = self.wait_for_task(lilota, failed_id, [TaskStatus.FAILED])
            self.assertEqual(failed_task.status, TaskStatus.FAILED)

            # Give the process manager the time to react to the task that raised
            self.sleep(2)

            # Assert
            self.assertFalse(
                lilota.has_failed(),
                "A task that raises an exception must not be a fatal error for lilota",
            )
            running_processes = [
                process
                for process in lilota._process_manager._processes
                if process.proc.poll() is None
            ]
            self.assertEqual(len(running_processes), number_of_workers)

            id = lilota.schedule("add", AddInput(a=1, b=2))
            task = self.wait_for_task(lilota, id, [TaskStatus.COMPLETED])
            self.assertEqual(task.status, TaskStatus.COMPLETED)
            self.assertEqual(task.output, {"sum": 3})
        finally:
            lilota.stop()

    def test_schedule___task_raises_exception_and_has_attempts_left___should_retry_task(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "retry_test_script.py"
            ),
            number_of_workers=1,
            process_manager_interval=0.1,
        )
        lilota.start()

        try:
            # Act
            id = lilota.schedule("always_failing")

            # Assert
            retry_task = self.wait_for_retry_of_task(id)
            self.assertIsNotNone(
                retry_task,
                "The task failed and has attempts left, but it was not retried",
            )
            self.assertEqual(retry_task.name, "always_failing")
            self.assertEqual(retry_task.attempts, 2)
            self.assertEqual(retry_task.max_attempts, 3)
        finally:
            lilota.stop()

    def test_schedule___task_raises_exception_and_has_no_attempts_left___should_not_retry_task(
        self,
    ):
        # Arrange
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent / "scripts" / "retry_test_script.py"
            ),
            number_of_workers=1,
            process_manager_interval=0.1,
        )
        lilota.start()

        try:
            # Act
            id = lilota.schedule("always_failing_with_default_max_attempts")
            task = self.wait_for_task(lilota, id, [TaskStatus.FAILED])

            # Give the worker the time to retry the task. It looks for tasks to retry
            # with every heartbeat, which is every 0.5 seconds in this script.
            self.sleep(2)

            # Assert
            self.assertEqual(task.status, TaskStatus.FAILED)
            self.assertEqual(task.attempts, 1)
            self.assertEqual(task.max_attempts, 1)

            with LilotaTestCase.get_session() as session:
                tasks = session.query(Task).all()
                self.assertEqual(
                    len(tasks),
                    1,
                    "A task with max_attempts=1 must be executed only once",
                )
                self.assertIsNone(tasks[0].retried_at)
        finally:
            lilota.stop()

    def test_timeout___when_worker_has_written_to_stderr___should_restart_worker(self):
        # Arrange
        with redirect_stderr(io.StringIO()):
            lilota = Lilota(
                db_url=self.DB_URL,
                script_path=str(
                    Path(__file__).resolve().parent
                    / "scripts"
                    / "killed_worker_test_script.py"
                ),
                number_of_workers=1,
                process_manager_interval=0.1,
            )
            lilota.start()

            try:
                process_id = lilota._process_manager._processes[0].id
                warning_id = lilota.schedule("write_warning")
                warning_task = self.wait_for_task(
                    lilota, warning_id, [TaskStatus.COMPLETED]
                )

                # Act: the worker stops itself because the task expires
                lilota.schedule("infinite_loop")
                is_restarted = self.wait_for_restart_of_worker(lilota, process_id)
                error = lilota.error

                next_id = lilota.schedule("write_warning")
                next_task = self.wait_for_task(lilota, next_id, [TaskStatus.COMPLETED])
            finally:
                lilota.stop()

        # Assert
        self.assertEqual(warning_task.status, TaskStatus.COMPLETED)
        self.assertIsNone(
            error,
            "A worker that is stopped because of an expired task must be restarted, "
            "whatever it has written to stderr before",
        )
        self.assertTrue(is_restarted)
        self.assertEqual(next_task.status, TaskStatus.COMPLETED)

    def test_kill___when_worker_has_written_to_stderr___should_restart_worker(self):
        # Arrange
        with redirect_stderr(io.StringIO()):
            lilota = Lilota(
                db_url=self.DB_URL,
                script_path=str(
                    Path(__file__).resolve().parent
                    / "scripts"
                    / "killed_worker_test_script.py"
                ),
                number_of_workers=1,
                process_manager_interval=0.1,
            )
            lilota.start()

            try:
                process = lilota._process_manager._processes[0]
                warning_id = lilota.schedule("write_warning")
                warning_task = self.wait_for_task(
                    lilota, warning_id, [TaskStatus.COMPLETED]
                )

                # Act: the worker is killed from the outside
                os.kill(process.proc.pid, signal.SIGKILL)
                is_restarted = self.wait_for_restart_of_worker(lilota, process.id)
                error = lilota.error

                next_id = lilota.schedule("write_warning")
                next_task = self.wait_for_task(lilota, next_id, [TaskStatus.COMPLETED])
            finally:
                lilota.stop()

        # Assert
        self.assertEqual(warning_task.status, TaskStatus.COMPLETED)
        self.assertIsNone(
            error,
            "A worker that is killed must be restarted, whatever it has written to "
            "stderr before",
        )
        self.assertTrue(is_restarted)
        self.assertEqual(next_task.status, TaskStatus.COMPLETED)

    def test_kill___when_application_is_killed___should_stop_workers(self):
        # Arrange
        with self.application("hello_world_test_script.py") as (
            application,
            worker_pids,
        ):
            # Act: the application has no chance to stop its workers
            application.kill()
            application.wait()
            running_worker_pids = self.wait_for_end_of_processes(worker_pids)

        # Assert
        self.assertEqual(
            running_worker_pids,
            [],
            "Workers must end when the application that started them was killed",
        )

    def test_terminate___when_application_is_terminated___should_stop_workers(self):
        # Arrange
        with self.application("hello_world_test_script.py") as (
            application,
            worker_pids,
        ):
            # Act: the application does not handle SIGTERM, so it ends at once and
            # does not stop its workers either
            application.terminate()
            application.wait()
            running_worker_pids = self.wait_for_end_of_processes(worker_pids)

        # Assert
        self.assertEqual(
            running_worker_pids,
            [],
            "Workers must end when the application that started them was terminated",
        )

    def test_kill___when_application_is_killed___should_stop_subprocess_of_task(self):
        # Arrange
        scheduler = Lilota(db_url=self.DB_URL, mode=LilotaMode.SCHEDULER)
        scheduler.start()

        try:
            with self.application("subprocess_test_script.py") as (
                application,
                _,
            ):
                task_id = scheduler.schedule("start_subprocess")
                task = self.wait_for_task(scheduler, task_id, [TaskStatus.COMPLETED])
                self.assertEqual(task.status, TaskStatus.COMPLETED)
                subprocess_pid = task.output["pid"]
                is_started = self.is_process_running(subprocess_pid)

                # Act
                application.kill()
                application.wait()
                running_pids = self.wait_for_end_of_processes([subprocess_pid])
        finally:
            scheduler.stop()

        # Assert
        self.assertTrue(is_started)
        self.assertEqual(
            running_pids,
            [],
            "A process started by a task must end with the worker, when the "
            "application that started the worker was killed",
        )

    def test_start___when_worker_script_is_started_without_lilota___should_keep_running_after_its_parent_ended(
        self,
    ):
        # Arrange: a process starts the worker script on its own and ends at once
        worker_script = (
            Path(__file__).resolve().parent / "scripts" / "hello_world_test_script.py"
        )
        parent = subprocess.run(
            [
                sys.executable,
                "-c",
                "import subprocess, sys\n"
                "worker = subprocess.Popen(\n"
                "    [sys.executable, '-u', sys.argv[1]],\n"
                "    stdout=subprocess.DEVNULL,\n"
                "    start_new_session=True,\n"
                ")\n"
                "print(worker.pid)",
                str(worker_script),
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        worker_pid = int(parent.stdout)

        try:
            is_started = self.wait_for_worker_nodes(1)

            # Act: more time than a worker needs to notice that its parent is gone
            self.sleep(3)
            is_running = self.is_process_running(worker_pid)
        finally:
            self.kill_process_group(worker_pid)

        # Assert
        self.assertTrue(is_started)
        self.assertTrue(
            is_running,
            "A worker that was not started by lilota must keep running when the "
            "process that started it has ended",
        )

    def test_dead_worker___when_task_was_taken_and_not_started___should_execute_task(
        self,
    ):
        # Arrange: a worker has taken a task and has died before it started the task
        dead_worker_id = self.create_dead_worker()
        store = TaskStore(self.DB_URL, None)
        task_id = store.create_task("with_timeout")
        store.get_next_task(dead_worker_id)
        task_of_dead_worker = store.get_task_by_id(task_id)

        # Act: a worker that is alive
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "dead_worker_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        try:
            completed_task = self.wait_for_completed_task("with_timeout")
            task = store.get_task_by_id(task_id)
        finally:
            lilota.stop()

        # Assert
        self.assertEqual(task_of_dead_worker.status, TaskStatus.SCHEDULED)
        self.assertEqual(task_of_dead_worker.locked_by, dead_worker_id)
        self.assertIsNotNone(
            completed_task,
            "A task that a dead worker has taken must be executed by another worker. "
            f"The task still has the status '{task.status}'",
        )

    def test_dead_worker___when_task_without_timeout_was_running___should_execute_task(
        self,
    ):
        # Arrange: a worker has died while it was executing a task that has no timeout
        dead_worker_id = self.create_dead_worker()
        store = TaskStore(self.DB_URL, None)
        task_id = store.create_task("without_timeout")
        store.get_next_task(dead_worker_id)
        store.start_task(task_id, max_attempts=1, timeout=None)
        task_of_dead_worker = store.get_task_by_id(task_id)

        # Act: a worker that is alive
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "dead_worker_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        try:
            completed_task = self.wait_for_completed_task("without_timeout")
            task = store.get_task_by_id(task_id)
        finally:
            lilota.stop()

        # Assert
        self.assertEqual(task_of_dead_worker.status, TaskStatus.RUNNING)
        self.assertIsNone(task_of_dead_worker.expires_at)
        self.assertIsNotNone(
            completed_task,
            "A task that a dead worker was executing must be executed by another "
            f"worker. The task still has the status '{task.status}'",
        )

    def test_dead_worker___when_task_with_timeout_was_running___should_execute_task(
        self,
    ):
        # Arrange: a worker has died while it was executing a task. The task has the
        # default of one attempt, and its timeout of 5 minutes has not passed.
        dead_worker_id = self.create_dead_worker()
        store = TaskStore(self.DB_URL, None)
        task_id = store.create_task("with_timeout")
        store.get_next_task(dead_worker_id)
        store.start_task(task_id, max_attempts=1, timeout=timedelta(minutes=5))
        task_of_dead_worker = store.get_task_by_id(task_id)

        # Act: a worker that is alive
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "dead_worker_test_script.py"
            ),
            number_of_workers=1,
        )
        lilota.start()

        try:
            completed_task = self.wait_for_completed_task("with_timeout")
            task = store.get_task_by_id(task_id)
        finally:
            lilota.stop()

        # Assert
        self.assertEqual(task_of_dead_worker.status, TaskStatus.RUNNING)
        self.assertIsNotNone(task_of_dead_worker.expires_at)
        self.assertIsNotNone(
            completed_task,
            "A task that a dead worker was executing must be executed by another "
            "worker, without waiting for its timeout and even if it has no attempts "
            f"left. The task still has the status '{task.status}'",
        )

    def sleep(self, seconds: float = 0.3):
        time.sleep(seconds)

    def wait_for_restart_of_worker(
        self, lilota: Lilota, process_id: UUID, timeout: float = 10
    ) -> bool:
        """Wait until the worker process was replaced by a new one that is running.

        Returns False if lilota fails instead, or after the timeout.
        """
        deadline = time.monotonic() + timeout

        while True:
            if lilota.has_failed() or time.monotonic() >= deadline:
                return False

            processes = list(lilota._process_manager._processes)
            if (
                len(processes) == 1
                and processes[0].id != process_id
                and processes[0].proc.poll() is None
            ):
                return True

            self.sleep(0.1)

    def wait_for_task(
        self, lilota: Lilota, id: UUID, statuses: list[TaskStatus], timeout: float = 10
    ) -> Task:
        """Wait until the task has one of the statuses and return it.

        After the timeout the task is returned as it is, so the caller can assert on
        the status it actually has.
        """
        deadline = time.monotonic() + timeout

        while True:
            task = lilota.get_task_by_id(id)
            if task.status in statuses or time.monotonic() >= deadline:
                return task
            self.sleep(0.1)

    @contextmanager
    def worker_stdout_in_file(self):
        """Let worker processes write their stdout into a file, and yield its path.

        A worker writes to the stdout of the process that started it, which is this
        test run. The stdout is replaced on the level of the file descriptor, because
        that is what a worker process inherits. contextlib.redirect_stdout would only
        replace sys.stdout of this process.

        Workers have to be started inside the block. The file is deleted when the
        block ends.
        """
        stdout_file_descriptor = 1

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout.txt"
            sys.stdout.flush()
            original_stdout = os.dup(stdout_file_descriptor)

            with open(path, "w") as file:
                os.dup2(file.fileno(), stdout_file_descriptor)

            try:
                yield path
            finally:
                sys.stdout.flush()
                os.dup2(original_stdout, stdout_file_descriptor)
                os.close(original_stdout)

    def wait_for_text_in_file(self, path: Path, text: str, timeout: float = 5) -> str:
        """Wait until the file contains the text and return the content of the file.

        After the timeout the content is returned as it is.
        """
        deadline = time.monotonic() + timeout

        while True:
            content = path.read_text()
            if text in content or time.monotonic() >= deadline:
                return content
            self.sleep(0.1)

    def wait_for_retry_of_task(self, id: UUID, timeout: float = 10) -> Task | None:
        """Wait until the task was retried and return the retry, or None after the timeout."""
        deadline = time.monotonic() + timeout

        while True:
            with LilotaTestCase.get_session() as session:
                retry_task = (
                    session.query(Task).filter(Task.previous_task_id == id).first()
                )
            if retry_task is not None or time.monotonic() >= deadline:
                return retry_task
            self.sleep(0.1)

    def create_dead_worker(self) -> UUID:
        """Create the node of a worker that has died, and return its ID.

        The process of such a worker is gone, so its node was not seen for a long
        time. Its status is still "running": nobody was there to change it.
        """
        with LilotaTestCase.get_session() as session:
            node = Node(
                type=NodeType.WORKER,
                status=NodeStatus.RUNNING,
                last_seen_at=datetime.now(timezone.utc) - timedelta(hours=1),
            )
            session.add(node)
            session.commit()
            return node.id

    def wait_for_completed_task(self, name: str, timeout: float = 15) -> Task | None:
        """Wait until a task with the name is completed and return it, or None after the timeout.

        The completed task is the task that was scheduled or a retry of it. The
        timeout is long enough for a retry, which starts after 5 seconds.
        """
        deadline = time.monotonic() + timeout

        while True:
            with LilotaTestCase.get_session() as session:
                completed_task = (
                    session.query(Task)
                    .filter(Task.name == name, Task.status == TaskStatus.COMPLETED)
                    .first()
                )
            if completed_task is not None or time.monotonic() >= deadline:
                return completed_task
            self.sleep(0.1)

    @contextmanager
    def application(self, worker_script: str):
        """Run an application that starts lilota with 2 workers, in a process of its own.

        Yields the process of the application and the process IDs of its workers, as
        soon as the workers have started. The application never calls lilota.stop().

        When the block ends, the application is killed, and so are the workers that
        are still there. A failing test therefore leaves no processes behind.
        """
        scripts = Path(__file__).resolve().parent / "scripts"
        application = subprocess.Popen(
            [
                sys.executable,
                "-u",
                str(scripts / "application_test_script.py"),
                str(scripts / worker_script),
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        worker_pids: list[int] = []

        try:
            # The first line the application writes are the process IDs of its workers
            if select.select([application.stdout], [], [], 10)[0]:
                worker_pids = [
                    int(pid) for pid in application.stdout.readline().split()
                ]
            self.assertEqual(len(worker_pids), 2, "The application did not start")
            self.assertTrue(self.wait_for_worker_nodes(2), "The workers did not start")

            yield application, worker_pids
        finally:
            # The application first: as long as it runs, it replaces a killed worker
            application.kill()
            application.wait()
            application.stdout.close()
            for pid in worker_pids:
                self.kill_process_group(pid)

    def wait_for_worker_nodes(self, count: int, timeout: float = 10) -> bool:
        """Wait until the number of workers has started. Returns False after the timeout."""
        deadline = time.monotonic() + timeout

        while True:
            with LilotaTestCase.get_session() as session:
                started = (
                    session.query(Node).filter(Node.type == NodeType.WORKER).count()
                )
            if started == count or time.monotonic() >= deadline:
                return started == count
            self.sleep(0.1)

    def wait_for_end_of_processes(
        self, pids: list[int], timeout: float = 10
    ) -> list[int]:
        """Wait until none of the processes is running anymore.

        Returns the IDs of the processes that are still running after the timeout.
        """
        deadline = time.monotonic() + timeout

        while True:
            running_pids = [pid for pid in pids if self.is_process_running(pid)]
            if not running_pids or time.monotonic() >= deadline:
                return running_pids
            self.sleep(0.1)

    def is_process_running(self, pid: int) -> bool:
        # ps instead of os.kill(pid, 0): a process that has ended keeps its process ID
        # until its parent has collected it. It would be taken for a running process.
        state = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True
        ).stdout.strip()
        return state != "" and not state.startswith("Z")

    def kill_process_group(self, pid: int) -> None:
        """Kill the process and the processes it has started, if they are still there.

        Works for a process that leads its own process group, as a worker does.
        """
        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def wait(self, lilota: Lilota) -> None:
        max = 60
        index = 0

        while True:
            has_unfinished_tasks = lilota._scheduler.has_unfinished_tasks()
            if has_unfinished_tasks:
                self.sleep(1)
                index = index + 1
                if index == max:
                    raise Exception("Should finish earlier")
            else:
                break

            if lilota.has_failed():
                raise Exception(lilota.error)


class WorkerErrorOutputTestCase(TestCase):
    def read_error_output(self, code: str) -> tuple[str, str]:
        """Run the code in a Python process and read its stderr with a WorkerErrorOutput.

        Returns what the WorkerErrorOutput kept and what it passed on to sys.stderr.
        """
        proc = subprocess.Popen(
            [sys.executable, "-c", code], stderr=subprocess.PIPE, text=True
        )

        with redirect_stderr(io.StringIO()) as passed_on:
            error_output = WorkerErrorOutput(proc)
            error_output.start()
            proc.wait()
            text = error_output.text()

        return text, passed_on.getvalue()

    def test_text___process_writes_nothing___should_return_empty_string(self):
        # Act
        text, passed_on = self.read_error_output("pass")

        # Assert
        self.assertEqual(text, "")
        self.assertEqual(passed_on, "")

    def test_text___process_fails_with_exception___should_return_traceback(self):
        # Act
        text, passed_on = self.read_error_output("raise Exception('Error in script')")

        # Assert
        self.assertTrue(text.startswith("Traceback (most recent call last):"))
        self.assertTrue(text.endswith("Exception: Error in script\n"))
        self.assertEqual(passed_on, text)

    def test_text___process_writes_more_lines_than_are_kept___should_return_last_lines(
        self,
    ):
        # Arrange
        number_of_lines = WorkerErrorOutput.MAX_LINES + 50

        # Act
        text, passed_on = self.read_error_output(
            "import sys\n"
            f"for i in range({number_of_lines}): print('line', i, file=sys.stderr)"
        )

        # Assert
        lines = text.splitlines()
        self.assertEqual(len(lines), WorkerErrorOutput.MAX_LINES)
        self.assertEqual(lines[0], "line 50")
        self.assertEqual(lines[-1], f"line {number_of_lines - 1}")
        self.assertEqual(len(passed_on.splitlines()), number_of_lines)

    def test_text___process_writes_one_endless_line___should_keep_limited_text(self):
        # Arrange
        maximum = WorkerErrorOutput.MAX_LINES * WorkerErrorOutput.MAX_LINE_LENGTH
        length = maximum + 10 * WorkerErrorOutput.MAX_LINE_LENGTH

        # Act
        text, passed_on = self.read_error_output(
            f"import sys\nsys.stderr.write('x' * {length})"
        )

        # Assert
        self.assertEqual(len(text), maximum)
        self.assertEqual(len(passed_on), length)


if __name__ == "__main__":
    main()
