import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from alembic import command
from dataclasses import dataclass
from unittest import TestCase, main
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.worker import LilotaWorker
from lilota.models import Node, NodeType, NodeStatus, NodeLeader, Task, LogEntry
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



class LilotaWorkerTestCase(TestCase):

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
    cfg = get_alembic_config(db_url=LilotaWorkerTestCase.DB_URL)
    try:
      command.upgrade(cfg, "head")
    except Exception as ex:
      raise Exception(f"Could not update the database: {str(ex)}")
    

  def setUp(self):
    # Create SqlAlchemy engine and session
    with LilotaWorkerTestCase.get_session() as session:
      # Delete all tasks and nodes
      session.query(Task).delete()
      session.query(Node).delete()
      session.query(LogEntry).delete()
      session.query(NodeLeader).delete()
      session.commit()


  def test_register___nothing_is_registered___should_not_have_any_registration(self):
    # Arrange & Act
    lilota = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True)

    # Assert
    self.assertEqual(len(lilota._registry), 0)


  def test_register___one_class_is_registered___should_have_one_registration(self):
    # Arrange
    lilota = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True)

    # Act
    lilota._register("add_task", AddInput)

    # Assert
    self.assertEqual(len(lilota._registry), 1)


  def test_register___task_is_already_registered___should_raise_exception(self):
    # Arrange
    lilota = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True)
    lilota._register("add_task", AddInput)

    # Act & Assert
    try:
      lilota._register("add_task", AddInput)
    except RuntimeError as err:
      self.assertEqual(str(err), "Task 'add_task' is already registered")


  def test_start___but_started_twice___should_raise_exception(self):
    # Arrange
    lilota = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)
    lilota.start()

    # Act & Assert
    with self.assertRaises(Exception) as context:
      lilota.start()
    try:
      self.assertEqual(str(context.exception), "The node is already started")
    finally:
      lilota.stop()


  def test_start___should_create_node(self):
    # Act
    worker = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True)
    worker.start()

    # Assert
    try:
      node: Node = worker.get_node()
      self.assertEqual(node.id, worker._node_id)
      self.assertEqual(node.type, NodeType.WORKER)
      self.assertEqual(node.status, NodeStatus.RUNNING)
      self.assertIsNotNone(node.created_at)
      self.assertIsNotNone(node.last_seen_at)
    finally:
      worker.stop()


  def test_start___with_heartbeat___should_start_the_heartbeat(self):
    # Arrange
    worker = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True, node_heartbeat_interval=1.0)
    worker.start()

    try:
      node: Node = worker.get_node()
      first_seen = node.last_seen_at
      self.assertIsNotNone(first_seen)

      # Act
      time.sleep(2)

      # Assert
      node: Node = worker.get_node()
      self.assertGreater(
        node.last_seen_at,
        first_seen,
        "Heartbeat should update last_seen_at"
      )
    finally:
      worker.stop()


  def test_stop___but_start_was_not_executed___should_raise_exception(self):
    # Arrange
    lilota = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)

    # Act & Assert
    with self.assertRaises(Exception) as context:
      lilota.stop()
    self.assertEqual(str(context.exception), "The node cannot be stopped because it was not started")


  def test_stop___with_heartbeat___should_stop_the_heartbeat(self):
    # Arrange
    worker = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True, node_heartbeat_interval=1.0)
    worker.start()

    try:
      node: Node = worker.get_node()
      first_seen = node.last_seen_at
      self.assertIsNotNone(first_seen)
    finally:
      worker.stop()

    # Act
    time.sleep(2)

    # Assert
    node: Node = worker.get_node()
    self.assertEqual(
      node.last_seen_at,
      first_seen,
      "Heartbeat should not update last_seen_at"
    )


  def test_logging___when_starting_scheduler___should_log_correctly(self):
    # Arrange
    worker = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True, logging_level=logging.DEBUG)
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaWorkerTestCase.DB_URL)

    # Act
    worker.start()

    # Assert
    try:
      node: Node = worker.get_node()
    finally:
      worker.stop()

    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node.id)
    self.assertEqual(len(log_entries), 3)
    self.assertEqual("Node started", log_entries[0].message)
    self.assertEqual(f"Leadership acquired first time (node id: {node.id})", log_entries[1].message)
    self.assertEqual("Node stopped", log_entries[2].message)


  def test_logging___when_starting_scheduler_twice___should_log_correctly(self):
    # Arrange
    worker1 = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True, logging_level=logging.DEBUG)
    worker2 = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True, logging_level=logging.DEBUG)
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaWorkerTestCase.DB_URL)

    # Act
    worker1.start()
    worker2.start()

    # Assert
    try:
      node1: Node = worker1.get_node()
      node2: Node = worker2.get_node()
    finally:
      worker1.stop()
      worker2.stop()

    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node1.id)
    self.assertEqual(len(log_entries), 3)
    self.assertEqual("Node started", log_entries[0].message)
    self.assertEqual(f"Leadership acquired first time (node id: {node1.id})", log_entries[1].message)
    self.assertEqual("Node stopped", log_entries[2].message)

    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node2.id)
    self.assertEqual(len(log_entries), 2)
    self.assertEqual("Node started", log_entries[0].message)
    self.assertEqual("Node stopped", log_entries[1].message)


  def test_leadership___with_one_worker___should_set_this_worker_as_leader(self):
    # Arrange
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaWorkerTestCase.DB_URL)

    with LilotaWorkerTestCase.get_session() as session:
      session.query(NodeLeader).delete()
      worker = LilotaWorker(
        LilotaWorkerTestCase.DB_URL, 
        run_in_thread=True,
        node_heartbeat_interval=1.0, 
        node_timeout_sec=20,
        logging_level=logging.DEBUG
      )
      node_leader = session.get(NodeLeader, 1)
      self.assertIsNone(node_leader)
      session.commit()
    
    # Act
    worker.start()
    time.sleep(1)
    worker._heartbeat.stop()

    # Assert
    with LilotaWorkerTestCase.get_session() as session:
      node_leader = session.get(NodeLeader, 1)
      self.assertIsNotNone(node_leader)
      self.assertEqual(node_leader.node_id, worker.get_node().id)

    node_id = worker.get_node().id
    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node_id)
    self.assertEqual(len(log_entries), 2)
    self.assertEqual("Node started", log_entries[0].message)
    self.assertEqual(f"Leadership acquired first time (node id: {node_id})", log_entries[1].message)
    worker.stop()


  def test_leadership___with_two_workers_and_first_one_expires___should_set_second_worker_as_leader(self):
    # Arrange
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaWorkerTestCase.DB_URL)

    with LilotaWorkerTestCase.get_session() as session:
      session.query(NodeLeader).delete()
      session.query(Node).delete()
      worker1 = LilotaWorker(
        LilotaWorkerTestCase.DB_URL, 
        run_in_thread=True,
        node_heartbeat_interval=1.0, 
        node_timeout_sec=2,
        logging_level=logging.DEBUG
      )
      node_leader = session.get(NodeLeader, 1)
      self.assertIsNone(node_leader)
      session.commit()
    
    worker1.start()
    time.sleep(1)
    worker1._heartbeat.stop()
    
    with LilotaWorkerTestCase.get_session() as session:
      node_leader = session.get(NodeLeader, 1)
      self.assertIsNotNone(node_leader)
      self.assertEqual(node_leader.node_id, worker1.get_node().id)

    # Act
    worker2 = LilotaWorker(
      LilotaWorkerTestCase.DB_URL, 
      run_in_thread=True,
      node_heartbeat_interval=1.0, 
      node_timeout_sec=2,
      logging_level=logging.DEBUG
    )
    worker2.start()
    time.sleep(2)
    worker2._heartbeat.stop()

    # Assert
    with LilotaWorkerTestCase.get_session() as session:
      node_leader = session.get(NodeLeader, 1)
      self.assertIsNotNone(node_leader)
      self.assertEqual(node_leader.node_id, worker2.get_node().id)

    worker1_id = worker1.get_node().id
    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(worker1_id)
    self.assertEqual(len(log_entries), 2)
    self.assertEqual("Node started", log_entries[0].message)
    self.assertEqual(f"Leadership acquired first time (node id: {worker1_id})", log_entries[1].message)
    self.assertFalse(any(entry.message == "Leadership acquired (takeover)" for entry in log_entries))
    
    worker2_id = worker2.get_node().id
    log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(worker2_id)
    self.assertEqual(len(log_entries), 3)
    self.assertEqual("Node started", log_entries[0].message)
    self.assertEqual(f"Leadership acquired (new node id: {worker2_id})", log_entries[1].message)
    self.assertEqual("Marked 1 stale node(s) as DEAD", log_entries[2].message)
    worker1.stop()
    worker2.stop()


  def test_leadership___with_two_workers_and_first_one_is_leader___should_not_change_leader(self):
    # Arrange
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaWorkerTestCase.DB_URL)

    with LilotaWorkerTestCase.get_session() as session:
      session.query(NodeLeader).delete()
      session.query(Node).delete()
      node_leader = session.get(NodeLeader, 1)
      self.assertIsNone(node_leader)
      session.commit()

    worker1 = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True, node_heartbeat_interval=1.0, node_timeout_sec=2)
    worker1.start()
    time.sleep(2)

    worker2 = LilotaWorker(LilotaWorkerTestCase.DB_URL, run_in_thread=True, node_heartbeat_interval=1.0, node_timeout_sec=2)
    worker2.start()
    time.sleep(2)

    with LilotaWorkerTestCase.get_session() as session:
      node_leader = session.get(NodeLeader, 1)
      self.assertEqual(node_leader.node_id, worker1.get_node().id)
      first_expiration_date = node_leader.lease_expires_at

    # Act
    time.sleep(2)

    # Assert
    with LilotaWorkerTestCase.get_session() as session:
      node_leader = session.get(NodeLeader, 1)
      self.assertEqual(node_leader.node_id, worker1.get_node().id)
      next_expiration_date = node_leader.lease_expires_at
      self.assertGreater(next_expiration_date, first_expiration_date)
    worker1.stop()
    worker2.stop()


if __name__ == '__main__':
  main()