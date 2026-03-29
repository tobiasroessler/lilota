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
from lilota.models import Node, NodeStatus, NodeType
from lilota.db.alembic import get_alembic_config
from lilota.stores import SqlAlchemyNodeStore



class SqlAlchemyNodeStoreTestCase(TestCase):

  DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test"


  @classmethod
  def get_session(cls):
    engine = create_engine(cls.DB_URL)
    Session = sessionmaker(bind=engine)
    return Session()


  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    cfg = get_alembic_config(db_url=cls.DB_URL)
    try:
      command.upgrade(cfg, "head")
    except Exception as ex:
      raise Exception(f"Could not update the database: {str(ex)}")


  def setUp(self):
    self.delete_all_nodes()


  def test_create_node___should_persist_node(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    node_id = store.create_node(NodeType.WORKER)

    # Assert
    node = self.get_node(node_id)
    self.assertIsNotNone(node)
    self.assertEqual(node.id, node_id)
    self.assertEqual(node.type, NodeType.WORKER)
    self.assertEqual(node.status, NodeStatus.IDLE)


  def test_get_all_nodes___with_no_nodes___should_return_empty_list(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    nodes = store.get_all_nodes()

    # Assert
    self.assertEqual(nodes, [])


  def test_get_all_nodes___with_nodes___should_return_all(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    id1 = self.create_node(Node(type=NodeType.WORKER, status=NodeStatus.IDLE))
    id2 = self.create_node(Node(type=NodeType.SCHEDULER, status=NodeStatus.IDLE))
    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    nodes = store.get_all_nodes()

    # Assert
    self.assertEqual(len(nodes), 2)
    self.assertSetEqual({n.id for n in nodes}, {id1, id2})


  def test_get_node_by_id___existing_node___should_return_node(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    node_id = self.create_node(Node(type=NodeType.WORKER, status=NodeStatus.IDLE))
    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    node = store.get_node_by_id(node_id)

    # Assert
    self.assertIsNotNone(node)
    self.assertEqual(node.id, node_id)


  def test_get_node_by_id___non_existing_node___should_return_none(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    node = store.get_node_by_id(uuid4())

    # Assert
    self.assertIsNone(node)


  def test_update_node_status___should_update_status(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    node_id = self.create_node(Node(type=NodeType.WORKER, status=NodeStatus.IDLE))
    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    store.update_node_status(node_id, NodeStatus.RUNNING)

    # Assert
    node = store.get_node_by_id(node_id)
    self.assertEqual(node.status, NodeStatus.RUNNING)


  def test_update_status_on_dead_nodes___should_mark_old_nodes_as_dead(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    old_time = datetime.now(timezone.utc) - timedelta(hours=2)
    recent_time = datetime.now(timezone.utc)

    node1_id = self.create_node(Node(
      type=NodeType.WORKER,
      status=NodeStatus.IDLE,
      last_seen_at=old_time
    ))

    node2_id = self.create_node(Node(
      type=NodeType.WORKER,
      status=NodeStatus.IDLE,
      last_seen_at=recent_time
    ))

    exclude_id = node2_id

    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    updated_count = store.update_status_on_dead_nodes(
      cutoff=datetime.now(timezone.utc) - timedelta(hours=1),
      exclude_node_id=exclude_id
    )

    # Assert
    node1 = store.get_node_by_id(node1_id)
    node2 = store.get_node_by_id(node2_id)
    self.assertEqual(updated_count, 1)
    self.assertEqual(node1.status, NodeStatus.DEAD)
    self.assertEqual(node2.status, NodeStatus.IDLE)


  def test_update_status_on_dead_nodes___should_not_update_already_dead(self):
    # Arrange
    logger = logging.getLogger("test_logger")
    old_time = datetime.now(timezone.utc) - timedelta(hours=2)

    node_id = self.create_node(Node(
      type=NodeType.WORKER,
      status=NodeStatus.DEAD,
      last_seen_at=old_time
    ))

    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    updated_count = store.update_status_on_dead_nodes(
      cutoff=datetime.now(timezone.utc) - timedelta(hours=1),
      exclude_node_id=uuid4()
    )

    # Assert
    node = store.get_node_by_id(node_id)
    self.assertEqual(updated_count, 0)
    self.assertEqual(node.status, NodeStatus.DEAD)


  def test_update_node_last_seen_at___should_update_timestamp(self):
    # Arrange
    logger = logging.getLogger("test_logger")

    node_id = self.create_node(Node(
      type=NodeType.WORKER,
      status=NodeStatus.IDLE,
      last_seen_at=None
    ))

    store = SqlAlchemyNodeStore(self.DB_URL, logger)

    # Act
    store.update_node_last_seen_at(node_id)

    # Assert
    node = store.get_node_by_id(node_id)
    self.assertIsNotNone(node.last_seen_at)
    self.assertLess(
      datetime.now(timezone.utc) - node.last_seen_at,
      timedelta(seconds=5)
    )


  def delete_all_nodes(self):
    with self.get_session() as session:
      session.query(Node).delete()
      session.commit()


  def create_node(self, node: Node):
    with self.get_session() as session:
      session.add(node)
      session.commit()
      return node.id


  def get_node(self, node_id):
    with self.get_session() as session:
      return session.get(Node, node_id)



if __name__ == '__main__':
  main()