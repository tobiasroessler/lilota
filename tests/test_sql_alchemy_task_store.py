import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from alembic import command
from datetime import datetime, timezone, timedelta
import logging
from uuid import uuid4
from unittest import TestCase, main
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.models import Task, TaskStatus
from lilota.db.alembic import get_alembic_config
from lilota.stores import SqlAlchemyTaskStore



class SqlAlchemyTaskStoreTestCase(TestCase):

  DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test"


  @classmethod
  def get_session(cls):
    engine = create_engine(cls.DB_URL)
    Session = sessionmaker(bind=engine)
    return Session()
  

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    # Apply the migrations
    cfg = get_alembic_config(db_url=SqlAlchemyTaskStoreTestCase.DB_URL)
    try:
      command.upgrade(cfg, "head")
    except Exception as ex:
      raise Exception(f"Could not update the database: {str(ex)}")


  def setUp(self):
    self.delete_all_tasks()


  def test_get_next_task___with_no_tasks___should_return_none(self):
    # Arrange
    worker_id = uuid4()
    logger = logging.getLogger("test_logger")
    self.delete_all_tasks()
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

    # Act
    task: Task = store.get_next_task(worker_id)

    # Assert
    self.assertIsNone(task)


  def test_get_next_task___with_one_created_task___should_return_task(self):
    # Arrange
    worker_id = uuid4()
    logger = logging.getLogger("test_logger")
    task_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.CREATED,
      run_at=datetime.now(timezone.utc),
      locked_by=None,
      locked_at=None
    ))
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

    # Act
    next_task: Task = store.get_next_task(worker_id)

    # Assert
    self.assertIsNotNone(next_task)
    self.assertEqual(next_task.id, task_id)
    self.assertEqual(next_task.status, TaskStatus.SCHEDULED)
    self.assertIsNotNone(next_task.locked_at)
    self.assertEqual(next_task.locked_by, worker_id)


  def test_get_next_task___with_one_task_that_is_not_created___should_return_none(self):
    # Arrange
    worker_id = uuid4()
    logger = logging.getLogger("test_logger")

    statuses_to_test = [
      TaskStatus.SCHEDULED,
      TaskStatus.RUNNING,
      TaskStatus.COMPLETED,
      TaskStatus.FAILED,
      TaskStatus.CANCELLED
    ]

    for status in statuses_to_test:
      with self.subTest(status=status):

        task_id: Task = self.create_task(Task(
          name="test",
          status=status,
          run_at=datetime.now(timezone.utc)
        ))
        store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

        # Act
        next_task: Task = store.get_next_task(worker_id)

        # Assert
        self.assertIsNone(next_task)


  def test_get_next_task___with_two_created_tasks___should_return_task_with_smallest_run_at(self):
    # Arrange
    worker_id = uuid4()
    logger = logging.getLogger("test_logger")
    task1_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.CREATED,
      run_at=datetime.now(timezone.utc) + timedelta(minutes=5)
    ))
    task2_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.CREATED,
      run_at=datetime.now(timezone.utc)
    ))
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

    # Act
    next_task: Task = store.get_next_task(worker_id)

    # Assert
    self.assertIsNotNone(next_task)
    self.assertEqual(next_task.id, task2_id)


  def test_expire_overdue_tasks___with_one_running_and_expired_task___should_set_status_expired(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    task_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.RUNNING,
      run_at=datetime(2026, 3, 20, 0, 0, 0),
      expires_at=datetime(2026, 3, 20, 1, 0, 0)
    ))
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

    # Act
    store.expire_overdue_tasks()

    # Assert
    task = store.get_task_by_id(task_id)
    self.assertEqual(task.status, TaskStatus.EXPIRED)


  def test_expire_overdue_tasks___with_one_running_and_not_expired_task___should_not_set_status_to_expired(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    task_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.RUNNING,
      run_at=datetime(2026, 3, 20, 0, 0, 0),
      expires_at=None
    ))
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

    # Act
    store.expire_overdue_tasks()

    # Assert
    task = store.get_task_by_id(task_id)
    self.assertEqual(task.status, TaskStatus.RUNNING)


  def test_expire_overdue_tasks___with_one_expired_task___should_do_nothing(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    task_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.EXPIRED,
      run_at=datetime(2026, 3, 20, 0, 0, 0),
      expires_at=datetime(2026, 3, 20, 1, 0, 0)
    ))
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

    # Act
    store.expire_overdue_tasks()

    # Assert
    task = store.get_task_by_id(task_id)
    self.assertEqual(task.status, TaskStatus.EXPIRED)


  def test_start_task___with_no_timeout___should_not_set_expires_at_and_timeout(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    task_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.CREATED,
      run_at=datetime.now(timezone.utc) + timedelta(minutes=5),
      timeout=None
    ))
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

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


  def test_start_task___with_timeout_of_5_minutes___should_set_expires_at_correctly(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    timeout_in_sec = 300
    task_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.CREATED,
      run_at=datetime.now(timezone.utc) + timedelta(minutes=5)
    ))
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

    # Act
    task: Task = store.start_task(task_id, 1, timedelta(seconds=timeout_in_sec))

    # Assert
    self.assertGreater(task.pid, 0)
    self.assertEqual(task.status, TaskStatus.RUNNING)
    self.assertEqual(task.progress_percentage, 0)
    self.assertEqual(task.timeout, timeout_in_sec)
    self.assertIsNotNone(task.start_date_time)
    self.assertEqual(task.expires_at, task.start_date_time + timedelta(seconds=task.timeout))
    self.assertIsNone(task.end_date_time)


  def test_start_task___with_no_timeout___should_not_set_timeout(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    task_id: Task = self.create_task(Task(
      name="test",
      status=TaskStatus.CREATED,
      run_at=datetime.now(timezone.utc) + timedelta(minutes=5)
    ))
    store = SqlAlchemyTaskStore(SqlAlchemyTaskStoreTestCase.DB_URL, logger, False)

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
    store = SqlAlchemyTaskStore(self.DB_URL, logger, False)

    # Act
    result = store.retry_tasks()

    # Assert
    self.assertEqual(result, 0)


  def test_retry_tasks___with_failed_task___should_create_retry_task(self):
    # Arrange
    logger = logging.getLogger("test_logger")

    task_id = self.create_task(Task(
      name="test",
      status=TaskStatus.FAILED,
      run_at=datetime.now(timezone.utc),
      attempts=1,
      max_attempts=3
    ))

    store = SqlAlchemyTaskStore(self.DB_URL, logger, False)

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

    self.create_task(Task(
      name="test",
      status=TaskStatus.RUNNING,
      run_at=past,
      attempts=1,
      max_attempts=3,
      expires_at=past
    ))

    store = SqlAlchemyTaskStore(self.DB_URL, logger, False)

    # Act
    result = store.retry_tasks()

    # Assert
    self.assertEqual(result, 1)


  def test_retry_tasks___with_max_attempts_reached___should_not_retry(self):
    # Arrange
    logger = logging.getLogger("test_logger")

    self.create_task(Task(
      name="test",
      status=TaskStatus.FAILED,
      run_at=datetime.now(timezone.utc),
      attempts=3,
      max_attempts=3
    ))

    store = SqlAlchemyTaskStore(self.DB_URL, logger, False)

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
      retried_at=datetime.now(timezone.utc)
    )

    self.create_task(task)
    store = SqlAlchemyTaskStore(self.DB_URL, logger, False)

    # Act
    result = store.retry_tasks()

    # Assert
    self.assertEqual(result, 0)


  def test_retry_tasks___should_apply_backoff_delay(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    attempts = 2

    task_id = self.create_task(Task(
      name="test",
      status=TaskStatus.FAILED,
      run_at=datetime.now(timezone.utc),
      attempts=attempts,
      max_attempts=5
    ))

    store = SqlAlchemyTaskStore(self.DB_URL, logger, False)

    # Act
    before = datetime.now(timezone.utc)
    store.retry_tasks()
    after = datetime.now(timezone.utc)

    # Assert
    with self.get_session() as session:
      retry_task = session.query(Task).filter(Task.previous_task_id == task_id).one()
      expected_delay = timedelta(seconds=5 * attempts)
      self.assertTrue(
        before + expected_delay <= retry_task.run_at <= after + expected_delay
      )


  def test_retry_tasks___should_respect_batch_size(self):
    # Arrange
    logger = logging.getLogger("test_logger")

    for _ in range(5):
      self.create_task(Task(
        name="test",
        status=TaskStatus.FAILED,
        run_at=datetime.now(timezone.utc),
        attempts=1,
        max_attempts=3
      ))

    store = SqlAlchemyTaskStore(self.DB_URL, logger, False)

    # Act
    result = store.retry_tasks(batch_size=2)

    # Assert
    self.assertEqual(result, 2)
    with self.get_session() as session:
      retry_tasks = session.query(Task).filter(Task.previous_task_id.isnot(None)).all()
      self.assertEqual(len(retry_tasks), 2)


  def delete_all_tasks(self):
    with SqlAlchemyTaskStoreTestCase.get_session() as session:
      session.query(Task).delete()
      session.commit()


  def create_task(self, task: Task) -> Task:
    with SqlAlchemyTaskStoreTestCase.get_session() as session:
      session.add(task)
      session.commit()
      return task.id



if __name__ == '__main__':
  main()