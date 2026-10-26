"""Tasks registered with keep_input=False: their input is removed from the database as
soon as the task is finished for good, and not before."""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import TestCase, main
from uuid import uuid4

from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker

from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.core import Lilota
from lilota.db.alembic import upgrade_db
from lilota.models import LogEntry, Node, NodeLeader, Task, TaskProgress, TaskStatus
from lilota.stores import TaskStore
from lilota.worker import LilotaWorker

SECRET = {"link": "https://example.com/reset#token=secret"}


class KeepInputTestCase(TestCase):
    DB_URL = DEFAULT_TEST_DB_URL

    @classmethod
    def get_session(cls):
        engine = create_engine(cls.DB_URL)
        Session = sessionmaker(bind=engine)
        return Session()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        upgrade_db(cls.DB_URL)

    def setUp(self):
        with self.get_session() as session:
            session.query(Task).delete()
            session.query(Node).delete()
            session.query(LogEntry).delete()
            session.query(NodeLeader).delete()
            session.commit()

        self.store = TaskStore(self.DB_URL, None)

    def started_task(self, max_attempts: int = 1, keep_input: bool = False):
        """A task with SECRET as its input, taken and started by a worker."""
        task_id = self.store.create_task("test", SECRET)
        self.store.get_next_task(uuid4())
        self.store.start_task(task_id, max_attempts=max_attempts, keep_input=keep_input)
        return task_id

    def fail_task(self, task_id, can_be_retried: bool = True):
        self.store.end_task_failure(
            task_id,
            {"message": "Error"},
            TaskProgress(task_id, None),
            can_be_retried=can_be_retried,
        )

    def make_overdue(self, task_id):
        with self.get_session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
            )
            session.commit()

    def all_tasks(self) -> list[Task]:
        with self.get_session() as session:
            return session.query(Task).order_by(Task.attempts).all()

    # --- registration ----------------------------------------------------------------

    def test_task___keep_input_false___should_be_registered(self):
        # Arrange
        worker = LilotaWorker(db_url=self.DB_URL)

        # Act
        @worker.task(keep_input=False)
        def secret(data: dict) -> None:
            pass

        @worker.task
        def public(data: dict) -> None:
            pass

        # Assert
        self.assertFalse(worker._registry["secret"].keep_input)
        self.assertTrue(worker._registry["public"].keep_input)

    def test_start_task___should_write_keep_input_to_the_task(self):
        # Act
        secret_id = self.started_task(keep_input=False)
        public_id = self.store.create_task("test", SECRET)
        self.store.get_next_task(uuid4())
        self.store.start_task(public_id, max_attempts=1)

        # Assert
        self.assertFalse(self.store.get_task_by_id(secret_id).keep_input)
        self.assertTrue(self.store.get_task_by_id(public_id).keep_input)

    # --- completed -------------------------------------------------------------------

    def test_end_task_success___keep_input_false___should_remove_input(self):
        # Arrange
        task_id = self.started_task()

        # Act
        self.store.end_task_success(
            task_id, {"sent": True}, TaskProgress(task_id, None)
        )

        # Assert
        task = self.store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertIsNone(task.input)
        self.assertEqual(task.output, {"sent": True})

        # SQL NULL, not the JSON value null: otherwise the row would be found and
        # rewritten again every time
        with self.get_session() as session:
            self.assertEqual(
                session.query(Task).filter(Task.input.is_(None)).count(), 1
            )

    def test_end_task_success___keep_input_true___should_keep_input(self):
        # Arrange
        task_id = self.started_task(keep_input=True)

        # Act
        self.store.end_task_success(task_id, None, TaskProgress(task_id, None))

        # Assert
        self.assertEqual(self.store.get_task_by_id(task_id).input, SECRET)

    # --- failed ----------------------------------------------------------------------

    def test_end_task_failure___with_attempts_left___should_keep_input_until_retried(
        self,
    ):
        # Arrange
        task_id = self.started_task(max_attempts=2)

        # Act
        self.fail_task(task_id)

        # Assert: the retry still has to copy it
        self.assertEqual(self.store.get_task_by_id(task_id).input, SECRET)

    def test_retry_tasks___keep_input_false___should_move_input_to_the_retry(self):
        # Arrange
        task_id = self.started_task(max_attempts=2)
        self.fail_task(task_id)

        # Act
        retried = self.store.retry_tasks()

        # Assert
        self.assertEqual(retried, 1)
        original, retry = self.all_tasks()
        self.assertEqual(original.id, task_id)
        self.assertIsNone(original.input)
        self.assertEqual(retry.input, SECRET)
        self.assertFalse(retry.keep_input)

    def test_retry_tasks___keep_input_true___should_keep_input_in_both(self):
        # Arrange
        task_id = self.started_task(max_attempts=2, keep_input=True)
        self.fail_task(task_id)

        # Act
        self.store.retry_tasks()

        # Assert
        original, retry = self.all_tasks()
        self.assertEqual(original.input, SECRET)
        self.assertEqual(retry.input, SECRET)

    def test_end_task_failure___with_no_attempts_left___should_remove_input(self):
        # Arrange
        task_id = self.started_task(max_attempts=1)

        # Act
        self.fail_task(task_id)

        # Assert
        task = self.store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertIsNone(task.input)
        self.assertEqual(task.error, {"message": "Error"})

    def test_end_task_failure___that_cannot_be_retried___should_remove_input(self):
        # Arrange: attempts left, but the task must not be executed again
        task_id = self.started_task(max_attempts=3)

        # Act
        self.fail_task(task_id, can_be_retried=False)

        # Assert
        self.assertIsNone(self.store.get_task_by_id(task_id).input)
        self.assertEqual(self.store.retry_tasks(), 0)

    # --- expired ---------------------------------------------------------------------

    def test_expire_overdue_tasks___with_no_attempts_left___should_remove_input(self):
        # Arrange
        task_id = self.started_task(max_attempts=1)
        self.make_overdue(task_id)

        # Act
        self.store.expire_overdue_tasks()

        # Assert
        task = self.store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.EXPIRED)
        self.assertIsNone(task.input)

    def test_expire_overdue_tasks___with_attempts_left___should_keep_input(self):
        # Arrange
        task_id = self.started_task(max_attempts=2)
        self.make_overdue(task_id)

        # Act
        self.store.expire_overdue_tasks()

        # Assert
        self.assertEqual(self.store.get_task_by_id(task_id).input, SECRET)

    def test_retry_tasks___overdue_running_task___should_move_input_to_the_retry(self):
        # Arrange
        task_id = self.started_task(max_attempts=2)
        self.make_overdue(task_id)

        # Act
        self.store.retry_tasks()

        # Assert
        original, retry = self.all_tasks()
        self.assertIsNone(original.input)
        self.assertEqual(retry.input, SECRET)

    # --- with a worker ---------------------------------------------------------------

    def start_lilota(self) -> Lilota:
        lilota = Lilota(
            db_url=self.DB_URL,
            script_path=str(
                Path(__file__).resolve().parent
                / "scripts"
                / "keep_input_test_script.py"
            ),
            number_of_workers=1,
            process_manager_interval=0.1,
        )
        lilota.start()
        return lilota

    def wait_until(self, condition, timeout: float = 20.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = condition()
            if result:
                return result
            time.sleep(0.1)
        self.fail("Timed out waiting")

    def test_worker___completed_tasks___should_remove_only_the_secret_input(self):
        # Arrange
        lilota = self.start_lilota()

        try:
            # Act
            secret_id = lilota.schedule("send_secret", SECRET)
            public_id = lilota.schedule("send_public", SECRET)

            secret, public = self.wait_until(
                lambda: (
                    (
                        lilota.get_task_by_id(secret_id),
                        lilota.get_task_by_id(public_id),
                    )
                    if lilota.get_task_by_id(secret_id).status == TaskStatus.COMPLETED
                    and lilota.get_task_by_id(public_id).status == TaskStatus.COMPLETED
                    else None
                )
            )

            # Assert
            self.assertIsNone(secret.input)
            self.assertEqual(public.input, SECRET)
        finally:
            lilota.stop()

    def test_worker___task_that_fails_every_time___should_leave_no_input_behind(self):
        # Arrange
        lilota = self.start_lilota()

        try:
            # Act: two attempts, both fail
            lilota.schedule("fail_secret", SECRET)

            tasks = self.wait_until(
                lambda: (
                    tasks
                    if len(tasks := self.all_tasks()) == 2
                    and all(task.status == TaskStatus.FAILED for task in tasks)
                    else None
                )
            )

            # Assert
            self.assertEqual([task.attempts for task in tasks], [1, 2])
            self.assertEqual([task.input for task in tasks], [None, None])
        finally:
            lilota.stop()


if __name__ == "__main__":
    main()
