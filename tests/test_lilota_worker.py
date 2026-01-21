import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from alembic import command
from dataclasses import dataclass
from unittest import TestCase, main
from typing import Any
from multiprocessing import cpu_count
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.core import LilotaWorker
from lilota.models import Node, NodeType, NodeStatus, Task, TaskStatus, TaskProgress
from lilota.db.alembic import get_alembic_config
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



class LilotaWorkerTestCase(TestCase):

  DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test"

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    # Apply the migrations
    cfg = get_alembic_config(db_url=LilotaWorkerTestCase.DB_URL)
    try:
      command.upgrade(cfg, "head")
    except Exception as ex:
      raise Exception(f"Could not update the database: {str(ex)}")
    
    # Create SQLAlchemy engine and session
    engine = create_engine(cls.DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Delete all tasks and nodes
    session.query(Task).delete()
    session.query(Node).delete()
    session.commit()
    session.close()


  def setUp(self):
    pass


  def test_start___should_create_node(self):
    # Act
    lilota = LilotaWorker(LilotaWorkerTestCase.DB_URL)
    lilota.start()

    # Assert
    try:
      node: Node = lilota.get_node()
      self.assertEqual(node.id, lilota._node_id)
      self.assertEqual(node.type, NodeType.WORKER)
      self.assertEqual(node.status, NodeStatus.RUNNING)
      self.assertIsNotNone(node.created_at)
      self.assertIsNotNone(node.last_seen_at)
    finally:
      lilota.stop()


  def test_start___with_heartbeat___should_start_the_heartbeat(self):
    # Arrange
    lilota = LilotaWorker(LilotaWorkerTestCase.DB_URL, heartbeat_interval_sec=1)
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
    lilota = LilotaWorker(LilotaWorkerTestCase.DB_URL, heartbeat_interval_sec=1)
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


if __name__ == '__main__':
  main()