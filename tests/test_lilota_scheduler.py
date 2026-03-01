import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from alembic import command
from dataclasses import dataclass
from unittest import TestCase, main
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.scheduler import LilotaScheduler
from lilota.models import Node, NodeLeader, NodeType, NodeStatus, Task, TaskStatus, LogEntry
from lilota.db.alembic import get_alembic_config
from lilota.stores import SqlAlchemyLogStore
import logging
import time


@dataclass
class AddInput():
    a: int
    b: int


@dataclass
class AddOutput():
  sum: int


def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


def hello_world():
  print("Hello Word")



class LilotaSchedulerTestCase(TestCase):

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
    cfg = get_alembic_config(db_url=LilotaSchedulerTestCase.DB_URL)
    try:
      command.upgrade(cfg, "head")
    except Exception as ex:
      raise Exception(f"Could not update the database: {str(ex)}")
    

  def setUp(self):
    # Create SqlAlchemy engine and session
    with LilotaSchedulerTestCase.get_session() as session:
      session.query(Task).delete()
      session.query(Node).delete()
      session.query(LogEntry).delete()
      session.query(NodeLeader).delete()
      session.commit()


  def test_start___should_create_node(self):
    # Arrange
    lilota = LilotaScheduler(LilotaSchedulerTestCase.DB_URL)

    # Act
    lilota.start()

    # Assert
    try:
      node: Node = lilota.get_node()
      self.assertEqual(node.id, lilota._node_id)
      self.assertEqual(node.type, NodeType.SCHEDULER)
      self.assertEqual(node.status, NodeStatus.RUNNING)
      self.assertIsNotNone(node.created_at)
      self.assertIsNotNone(node.last_seen_at)
    finally:
      lilota.stop()


  def test_start___with_heartbeat___should_start_the_heartbeat(self):
    # Arrange
    lilota = LilotaScheduler(LilotaSchedulerTestCase.DB_URL, node_heartbeat_interval=1.0)
    lilota.start()

    try:
      node: Node = lilota.get_node()
      first_seen = node.last_seen_at
      self.assertIsNotNone(first_seen)

      # Act
      time.sleep(2)

      # Assert
      node: Node = lilota.get_node()
      self.assertGreater(
        node.last_seen_at,
        first_seen,
        "Heartbeat should update last_seen_at"
      )
    finally:
      lilota.stop()


  def test_stop___with_heartbeat___should_stop_the_heartbeat(self):
    # Arrange
    lilota = LilotaScheduler(LilotaSchedulerTestCase.DB_URL, node_heartbeat_interval=1.0)
    lilota.start()

    try:
      node: Node = lilota.get_node()
      first_seen = node.last_seen_at
      self.assertIsNotNone(first_seen)
    finally:
      lilota.stop()

    # Act
    time.sleep(2)

    # Assert
    node: Node = lilota.get_node()
    self.assertEqual(
      node.last_seen_at,
      first_seen,
      "Heartbeat should not update last_seen_at"
    )


  def test_schedule___should_create_task(self):
    # Arrange
    lilota = LilotaScheduler(LilotaSchedulerTestCase.DB_URL)
    lilota.start()

    try:
      # Act
      task_id = lilota.schedule("add", AddInput(2, 3))

      # Assert
      task: Task = lilota.get_task_by_id(task_id)
      self.assertEqual(task.name, "add")
      self.assertEqual(task.status, TaskStatus.CREATED)
      self.assertIsNone(task.error)
      self.assertEqual(task.progress_percentage, 0)
      self.assertEqual(task.input['a'], 2)
      self.assertEqual(task.input['b'], 3)
      self.assertIsNone(task.output)
      self.assertIsNone(task.locked_at)
      self.assertIsNone(task.locked_by)
    finally:
      lilota.stop()


  def test_logging___when_starting_scheduler___should_log_correctly(self):
    # Arrange
    lilota = LilotaScheduler(LilotaSchedulerTestCase.DB_URL, logging_level=logging.DEBUG)
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaSchedulerTestCase.DB_URL)

    # Act
    lilota.start()

    # Assert
    try:
      node: Node = lilota.get_node()
    finally:
      lilota.stop()

    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node.id)
    self.assertEqual(len(log_entries), 2)
    self.assertEqual(log_entries[0].message, "Node started")
    self.assertEqual(log_entries[1].message, "Node stopped")


  def test_logging___when_starting_scheduler_stopping_and_starting_it_again___should_log_correctly(self):
    # Arrange
    lilota = LilotaScheduler(LilotaSchedulerTestCase.DB_URL, logging_level=logging.DEBUG)
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaSchedulerTestCase.DB_URL)
    lilota.start()
    lilota.stop()

    # Act
    lilota.start()

    # Assert
    try:
      node: Node = lilota.get_node()
    finally:
      lilota.stop()

    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node.id)
    self.assertEqual(len(log_entries), 4)
    self.assertEqual(log_entries[0].message, "Node started")
    self.assertEqual(log_entries[1].message, "Node stopped")
    self.assertEqual(log_entries[2].message, "Node started")
    self.assertEqual(log_entries[3].message, "Node stopped")


  def test_logging___when_starting_scheduler_twice___should_log_correctly(self):
    # Arrange
    lilota1 = LilotaScheduler(LilotaSchedulerTestCase.DB_URL, logging_level=logging.DEBUG)
    lilota2 = LilotaScheduler(LilotaSchedulerTestCase.DB_URL, logging_level=logging.DEBUG)
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaSchedulerTestCase.DB_URL)

    # Act
    lilota1.start()
    lilota2.start()

    # Assert
    try:
      node1: Node = lilota1.get_node()
      node2: Node = lilota2.get_node()
    finally:
      lilota1.stop()
      lilota2.stop()

    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node1.id)
    self.assertEqual(len(log_entries), 2)
    self.assertEqual(log_entries[0].message, "Node started")
    self.assertEqual(log_entries[1].message, "Node stopped")

    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node2.id)
    self.assertEqual(len(log_entries), 2)
    self.assertEqual(log_entries[0].message, "Node started")
    self.assertEqual(log_entries[1].message, "Node stopped")


if __name__ == '__main__':
  main()