import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from datetime import datetime, timezone, timedelta
import logging
from uuid import UUID, uuid4
from unittest import TestCase, main
from unittest.mock import patch
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker
from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.models import Node, NodeStatus, NodeType, Task, TaskProgress, TaskStatus
from lilota.db.alembic import upgrade_db
from lilota.stores import TaskStore


class SqlAlchemyTaskStoreTestCase(TestCase):
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
        upgrade_db(SqlAlchemyTaskStoreTestCase.DB_URL)

    def setUp(self):
        self.delete_all_tasks()
        self.delete_all_nodes()

    def test_get_next_task___with_no_tasks___should_return_none(self):
        # Arrange
        worker_id = uuid4()
        logger = logging.getLogger("test_logger")
        self.delete_all_tasks()
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        task: Task = store.get_next_task(worker_id)

        # Assert
        self.assertIsNone(task)

    def test_get_next_task___with_one_created_task___should_return_task(self):
        # Arrange
        worker_id = uuid4()
        logger = logging.getLogger("test_logger")
        task_id: Task = self.create_task(
            Task(
                name="test",
                status=TaskStatus.CREATED,
                run_at=datetime.now(timezone.utc),
                locked_by=None,
                locked_at=None,
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        next_task: Task = store.get_next_task(worker_id)

        # Assert
        self.assertIsNotNone(next_task)
        self.assertEqual(next_task.id, task_id)
        self.assertEqual(next_task.status, TaskStatus.SCHEDULED)
        self.assertIsNotNone(next_task.locked_at)
        self.assertEqual(next_task.locked_by, worker_id)

    def test_get_next_task___with_one_task_that_is_not_created___should_return_none(
        self,
    ):
        # Arrange
        worker_id = uuid4()
        logger = logging.getLogger("test_logger")

        statuses_to_test = [
            TaskStatus.SCHEDULED,
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        ]

        for status in statuses_to_test:
            with self.subTest(status=status):
                self.create_task(
                    Task(name="test", status=status, run_at=datetime.now(timezone.utc))
                )
                store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

                # Act
                next_task: Task = store.get_next_task(worker_id)

                # Assert
                self.assertIsNone(next_task)

    def test_get_next_task___with_two_created_tasks___should_return_task_with_smallest_run_at(
        self,
    ):
        # Arrange
        worker_id = uuid4()
        logger = logging.getLogger("test_logger")
        self.create_task(
            Task(
                name="test",
                status=TaskStatus.CREATED,
                run_at=datetime.now(timezone.utc) + timedelta(minutes=5),
            )
        )
        task2_id: Task = self.create_task(
            Task(
                name="test",
                status=TaskStatus.CREATED,
                run_at=datetime.now(timezone.utc),
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        next_task: Task = store.get_next_task(worker_id)

        # Assert
        self.assertIsNotNone(next_task)
        self.assertEqual(next_task.id, task2_id)

    def test_expire_overdue_tasks___with_one_running_and_expired_task___should_set_status_expired(
        self,
    ):
        # Arrange
        logger = logging.getLogger("test_logger")
        task_id: Task = self.create_task(
            Task(
                name="test",
                status=TaskStatus.RUNNING,
                run_at=datetime(2026, 3, 20, 0, 0, 0),
                expires_at=datetime(2026, 3, 20, 1, 0, 0),
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        store.expire_overdue_tasks()

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.EXPIRED)

    def test_expire_overdue_tasks___with_one_running_and_not_expired_task___should_not_set_status_to_expired(
        self,
    ):
        # Arrange
        logger = logging.getLogger("test_logger")
        task_id: Task = self.create_task(
            Task(
                name="test",
                status=TaskStatus.RUNNING,
                run_at=datetime(2026, 3, 20, 0, 0, 0),
                expires_at=None,
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        store.expire_overdue_tasks()

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.RUNNING)

    def test_expire_overdue_tasks___with_one_expired_task___should_do_nothing(self):
        # Arrange
        logger = logging.getLogger("test_logger")
        task_id: Task = self.create_task(
            Task(
                name="test",
                status=TaskStatus.EXPIRED,
                run_at=datetime(2026, 3, 20, 0, 0, 0),
                expires_at=datetime(2026, 3, 20, 1, 0, 0),
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        store.expire_overdue_tasks()

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.EXPIRED)

    def test_start_task___with_no_timeout___should_not_set_expires_at_and_timeout(self):
        # Arrange
        logger = logging.getLogger("test_logger")
        task_id: Task = self.create_task(
            Task(
                name="test",
                status=TaskStatus.CREATED,
                run_at=datetime.now(timezone.utc) + timedelta(minutes=5),
                timeout=None,
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        task: Task = store.start_task(task_id, 1)

        # Assert
        self.assertGreater(task.pid, 0)
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.progress_percentage, 0)
        self.assertIsNone(task.timeout)
        self.assertIsNotNone(task.start_date_time)
        self.assertIsNone(task.expires_at)
        self.assertIsNone(task.end_date_time)

    def test_start_task___with_timeout_of_5_minutes___should_set_expires_at_correctly(
        self,
    ):
        # Arrange
        logger = logging.getLogger("test_logger")
        timeout_in_sec = 300
        task_id: Task = self.create_task(
            Task(
                name="test",
                status=TaskStatus.CREATED,
                run_at=datetime.now(timezone.utc) + timedelta(minutes=5),
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        task: Task = store.start_task(task_id, 1, timedelta(seconds=timeout_in_sec))

        # Assert
        self.assertGreater(task.pid, 0)
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.progress_percentage, 0)
        self.assertEqual(task.timeout, timeout_in_sec)
        self.assertIsNotNone(task.start_date_time)
        self.assertEqual(
            task.expires_at, task.start_date_time + timedelta(seconds=task.timeout)
        )
        self.assertIsNone(task.end_date_time)

    def test_start_task___with_no_timeout___should_not_set_timeout(self):
        # Arrange
        logger = logging.getLogger("test_logger")
        task_id: Task = self.create_task(
            Task(
                name="test",
                status=TaskStatus.CREATED,
                run_at=datetime.now(timezone.utc) + timedelta(minutes=5),
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger)

        # Act
        task: Task = store.start_task(task_id, 1, None)

        # Assert
        self.assertGreater(task.pid, 0)
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.progress_percentage, 0)
        self.assertIsNone(task.timeout)
        self.assertIsNotNone(task.start_date_time)
        self.assertIsNone(task.expires_at)
        self.assertIsNone(task.end_date_time)

    def test_retry_tasks___with_no_tasks___should_return_zero(self):
        # Arrange
        logger = logging.getLogger("test_logger")
        store = TaskStore(self.DB_URL, logger)

        # Act
        result = store.retry_tasks()

        # Assert
        self.assertEqual(result, 0)

    def test_retry_tasks___with_failed_task___should_create_retry_task(self):
        # Arrange
        logger = logging.getLogger("test_logger")

        task_id = self.create_task(
            Task(
                name="test",
                status=TaskStatus.FAILED,
                run_at=datetime.now(timezone.utc),
                attempts=1,
                max_attempts=3,
            )
        )

        store = TaskStore(self.DB_URL, logger)

        # Act
        result = store.retry_tasks()

        # Assert
        self.assertEqual(result, 1)

        with self.get_session() as session:
            tasks = session.query(Task).all()
            self.assertEqual(len(tasks), 2)
            original = next(t for t in tasks if t.id == task_id)
            retry = next(t for t in tasks if t.id != task_id)
            self.assertIsNotNone(original.retried_at)
            self.assertEqual(retry.previous_task_id, task_id)
            self.assertEqual(retry.attempts, 2)
            self.assertEqual(retry.status, TaskStatus.CREATED)

    def test_retry_tasks___with_expired_running_task___should_retry(self):
        # Arrange
        logger = logging.getLogger("test_logger")
        past = datetime.now(timezone.utc) - timedelta(hours=1)

        self.create_task(
            Task(
                name="test",
                status=TaskStatus.RUNNING,
                run_at=past,
                attempts=1,
                max_attempts=3,
                expires_at=past,
            )
        )

        store = TaskStore(self.DB_URL, logger)

        # Act
        result = store.retry_tasks()

        # Assert
        self.assertEqual(result, 1)

    def test_retry_tasks___with_max_attempts_reached___should_not_retry(self):
        # Arrange
        logger = logging.getLogger("test_logger")

        self.create_task(
            Task(
                name="test",
                status=TaskStatus.FAILED,
                run_at=datetime.now(timezone.utc),
                attempts=3,
                max_attempts=3,
            )
        )

        store = TaskStore(self.DB_URL, logger)

        # Act
        result = store.retry_tasks()

        # Assert
        self.assertEqual(result, 0)

    def test_retry_tasks___with_already_retried_task___should_skip(self):
        # Arrange
        logger = logging.getLogger("test_logger")

        task = Task(
            name="test",
            status=TaskStatus.FAILED,
            run_at=datetime.now(timezone.utc),
            attempts=1,
            max_attempts=3,
            retried_at=datetime.now(timezone.utc),
        )

        self.create_task(task)
        store = TaskStore(self.DB_URL, logger)

        # Act
        result = store.retry_tasks()

        # Assert
        self.assertEqual(result, 0)

    def test_retry_tasks___should_apply_backoff_delay(self):
        # Arrange
        logger = logging.getLogger("test_logger")
        attempts = 2

        task_id = self.create_task(
            Task(
                name="test",
                status=TaskStatus.FAILED,
                run_at=datetime.now(timezone.utc),
                attempts=attempts,
                max_attempts=5,
            )
        )

        store = TaskStore(self.DB_URL, logger)

        # Act
        before = datetime.now(timezone.utc)
        store.retry_tasks()
        after = datetime.now(timezone.utc)

        # Assert
        with self.get_session() as session:
            retry_task = (
                session.query(Task).filter(Task.previous_task_id == task_id).one()
            )
            expected_delay = timedelta(seconds=5 * attempts)
            self.assertTrue(
                before + expected_delay <= retry_task.run_at <= after + expected_delay
            )

    def test_create_task___should_be_the_first_attempt(self):
        # Arrange
        logger = logging.getLogger("test_logger")
        store = TaskStore(self.DB_URL, logger)

        # Act
        task_id = store.create_task("test")

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(task.attempts, 1)
        self.assertEqual(task.max_attempts, 1)

    def test_retry_tasks___with_task_that_always_fails___should_execute_task_max_attempts_times(
        self,
    ):
        for max_attempts in [1, 2, 3]:
            with self.subTest(max_attempts=max_attempts):
                # Arrange
                self.delete_all_tasks()
                logger = logging.getLogger("test_logger")
                store = TaskStore(self.DB_URL, logger)
                store.create_task("test")
                executions = 0

                # Act: execute every task that is waiting, the way a worker does, let
                # it fail and retry, until no task is waiting anymore
                while True:
                    with self.get_session() as session:
                        waiting_tasks = (
                            session.query(Task)
                            .filter(Task.status == TaskStatus.CREATED)
                            .all()
                        )

                    if not waiting_tasks:
                        break

                    for task in waiting_tasks:
                        store.start_task(task.id, max_attempts)
                        store.end_task_failure(
                            task.id,
                            {"message": "Error"},
                            TaskProgress(task.id, store.set_progress),
                        )
                        executions += 1

                    store.retry_tasks()

                # Assert
                self.assertEqual(executions, max_attempts)

    def test_retry_tasks___should_respect_batch_size(self):
        # Arrange
        logger = logging.getLogger("test_logger")

        for _ in range(5):
            self.create_task(
                Task(
                    name="test",
                    status=TaskStatus.FAILED,
                    run_at=datetime.now(timezone.utc),
                    attempts=1,
                    max_attempts=3,
                )
            )

        store = TaskStore(self.DB_URL, logger)

        # Act
        result = store.retry_tasks(batch_size=2)

        # Assert
        self.assertEqual(result, 2)
        with self.get_session() as session:
            retry_tasks = (
                session.query(Task).filter(Task.previous_task_id.isnot(None)).all()
            )
            self.assertEqual(len(retry_tasks), 2)

    def test_end_task_failure___with_task_that_can_be_retried___should_retry_task(
        self,
    ):
        # Arrange: a task that has two of its three attempts left
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = store.create_task("test")
        store.get_next_task(uuid4())
        store.start_task(task_id, max_attempts=3)

        # Act
        store.end_task_failure(
            task_id, {"message": "Error"}, TaskProgress(task_id, None)
        )
        retried = store.retry_tasks()

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(retried, 1)

    def test_end_task_failure___with_task_that_cannot_be_retried___should_not_retry_task(
        self,
    ):
        # Arrange: a task that has two of its three attempts left
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = store.create_task("test")
        store.get_next_task(uuid4())
        store.start_task(task_id, max_attempts=3)

        # Act
        store.end_task_failure(
            task_id,
            {"message": "Error"},
            TaskProgress(task_id, None),
            can_be_retried=False,
        )
        retried = store.retry_tasks()

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.error, {"message": "Error"})
        self.assertEqual(retried, 0)

    def test_release_tasks_of_dead_nodes___with_task_of_dead_node___should_set_status_created(
        self,
    ):
        # Arrange: a worker has started a task and was not seen for an hour
        now = datetime.now(timezone.utc)
        dead_node_id = self.create_node(last_seen_at=now - timedelta(hours=1))
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = store.create_task("test")
        store.get_next_task(dead_node_id)
        store.start_task(task_id, max_attempts=1, timeout=timedelta(minutes=5))
        store.set_progress(task_id, 40)

        # Act
        released = store.release_tasks_of_dead_nodes(
            cutoff=now - timedelta(seconds=20), exclude_node_id=uuid4()
        )

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(released, 1)
        self.assertEqual(task.status, TaskStatus.CREATED)
        self.assertIsNone(task.locked_by)
        self.assertIsNone(task.locked_at)
        self.assertIsNone(task.start_date_time)
        self.assertIsNone(task.expires_at)
        self.assertEqual(task.progress_percentage, 0)
        self.assertEqual(task.attempts, 1)

    def test_release_tasks_of_dead_nodes___with_task_of_node_that_does_not_exist___should_set_status_created(
        self,
    ):
        # Arrange: the task is locked by a node of which there is no row
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = store.create_task("test")
        store.get_next_task(uuid4())

        # Act
        released = store.release_tasks_of_dead_nodes(
            cutoff=datetime.now(timezone.utc) - timedelta(seconds=20),
            exclude_node_id=uuid4(),
        )

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(released, 1)
        self.assertEqual(task.status, TaskStatus.CREATED)
        self.assertIsNone(task.locked_by)

    def test_release_tasks_of_dead_nodes___with_task_of_node_that_is_alive___should_not_change_task(
        self,
    ):
        # Arrange: the worker that is executing the task was seen a moment ago
        now = datetime.now(timezone.utc)
        node_id = self.create_node(last_seen_at=now)
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = store.create_task("test")
        store.get_next_task(node_id)
        store.start_task(task_id, max_attempts=1)

        # Act
        released = store.release_tasks_of_dead_nodes(
            cutoff=now - timedelta(seconds=20), exclude_node_id=uuid4()
        )

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(released, 0)
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.locked_by, node_id)

    def test_release_tasks_of_dead_nodes___with_task_of_excluded_node___should_not_change_task(
        self,
    ):
        # Arrange: the node that looks for dead nodes never takes itself for dead
        now = datetime.now(timezone.utc)
        node_id = self.create_node(last_seen_at=now - timedelta(hours=1))
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = store.create_task("test")
        store.get_next_task(node_id)
        store.start_task(task_id, max_attempts=1)

        # Act
        released = store.release_tasks_of_dead_nodes(
            cutoff=now - timedelta(seconds=20), exclude_node_id=node_id
        )

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(released, 0)
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.locked_by, node_id)

    def test_release_tasks_of_dead_nodes___with_task_that_has_run_into_its_timeout___should_not_change_task(
        self,
    ):
        # Arrange: a worker that stops itself because its task has expired leaves the
        # task like this. The task took too long. It must not be executed again
        # without a look at its attempts.
        now = datetime.now(timezone.utc)
        dead_node_id = self.create_node(last_seen_at=now - timedelta(hours=1))
        task_id = self.create_task(
            Task(
                name="test",
                status=TaskStatus.RUNNING,
                locked_by=dead_node_id,
                expires_at=now - timedelta(seconds=10),
            )
        )
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)

        # Act
        released = store.release_tasks_of_dead_nodes(
            cutoff=now - timedelta(seconds=20), exclude_node_id=uuid4()
        )

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(released, 0)
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.locked_by, dead_node_id)

    def test_end_task_success___task_was_expired_and_retried___should_not_complete_task(
        self,
    ):
        # Arrange: the task has run into its timeout. The leader has expired it and
        # has created its retry.
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = self.create_running_task(timeout_ends_in=timedelta(seconds=-1))
        store.expire_overdue_tasks()
        retried = store.retry_tasks()

        # Act: at that moment the task function returns on its worker
        store.end_task_success(task_id, {"sum": 3}, TaskProgress(task_id, None))

        # Assert: the retry executes the task again, so it is not completed as well
        task = store.get_task_by_id(task_id)
        self.assertEqual(retried, 1)
        self.assertEqual(task.status, TaskStatus.EXPIRED)
        self.assertIsNone(task.output)

    def test_end_task_failure___task_was_expired_and_retried___should_not_fail_task(
        self,
    ):
        # Arrange: the task has run into its timeout. The leader has expired it and
        # has created its retry.
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = self.create_running_task(timeout_ends_in=timedelta(seconds=-1))
        store.expire_overdue_tasks()
        retried = store.retry_tasks()

        # Act: at that moment the task function raises an exception on its worker
        store.end_task_failure(
            task_id, {"message": "Error"}, TaskProgress(task_id, None)
        )

        # Assert
        task = store.get_task_by_id(task_id)
        self.assertEqual(retried, 1)
        self.assertEqual(task.status, TaskStatus.EXPIRED)
        self.assertIsNone(task.error)

    def test_end_task_success___running_task_was_retried___should_not_complete_task(
        self,
    ):
        # Arrange: the task has run into its timeout. The leader has created its
        # retry, without expiring it before: retry_tasks also takes running tasks.
        store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = self.create_running_task(timeout_ends_in=timedelta(seconds=-1))
        retried = store.retry_tasks()

        # Act: at that moment the task function returns on its worker
        store.end_task_success(task_id, {"sum": 3}, TaskProgress(task_id, None))

        # Assert: the retry executes the task again, so it is not completed as well
        task = store.get_task_by_id(task_id)
        self.assertEqual(retried, 1)
        self.assertNotEqual(task.status, TaskStatus.COMPLETED)

    def test_retry_tasks___task_is_completed_while_retry_is_created___should_not_retry_task(
        self,
    ):
        # Arrange: the task has run into its timeout and is still running
        leader_store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        worker_store = TaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, None)
        task_id = self.create_running_task(timeout_ends_in=timedelta(seconds=-1))

        # The task function returns on its worker at the moment the leader has found
        # the task and is about to mark it as retried
        def complete_task_and_update(*args):
            with patch("lilota.stores.update", update):
                worker_store.end_task_success(
                    task_id, {"sum": 3}, TaskProgress(task_id, None)
                )
            return update(*args)

        # Act
        with patch("lilota.stores.update", complete_task_and_update):
            retried = leader_store.retry_tasks()

        # Assert: the task is completed, so it is not executed again
        task = leader_store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.output, {"sum": 3})
        self.assertEqual(retried, 0)

    def create_running_task(self, timeout_ends_in: timedelta) -> UUID:
        """Create a task that is being executed for the first time and may be executed twice.

        Its timeout ends after the given time. A negative time is a timeout that
        has passed.
        """
        now = datetime.now(timezone.utc)
        return self.create_task(
            Task(
                name="test",
                status=TaskStatus.RUNNING,
                run_at=now - timedelta(minutes=10),
                attempts=1,
                max_attempts=2,
                expires_at=now + timeout_ends_in,
            )
        )

    def delete_all_tasks(self):
        with SqlAlchemyTaskStoreTestCase.get_session() as session:
            session.query(Task).delete()
            session.commit()

    def create_task(self, task: Task) -> Task:
        with SqlAlchemyTaskStoreTestCase.get_session() as session:
            session.add(task)
            session.commit()
            return task.id

    def delete_all_nodes(self):
        with SqlAlchemyTaskStoreTestCase.get_session() as session:
            session.query(Node).delete()
            session.commit()

    def create_node(self, last_seen_at: datetime):
        with SqlAlchemyTaskStoreTestCase.get_session() as session:
            node = Node(
                type=NodeType.WORKER,
                status=NodeStatus.RUNNING,
                last_seen_at=last_seen_at,
            )
            session.add(node)
            session.commit()
            return node.id


if __name__ == "__main__":
    main()
