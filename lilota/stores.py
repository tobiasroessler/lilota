from abc import ABC
import os
from .models import Node, NodeType, NodeStatus, NodeLeader, Task, TaskStatus, LogEntry
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from typing import Any
from lilota.utils import normalize_data
from uuid import UUID
import logging


class StoreBase(ABC):
  """Abstract base class for all database stores.

  Provides common initialization and session management.
  """

  def __init__(self, db_url: str, logger: logging.Logger):
    """
    Args:
        db_url (str): Database connection URL.
        logger (logging.Logger): Logger for store operations.
    """
    self._db_url = db_url
    self._logger = logger
    self._engine = None
    self._Session = None


  def _ensure_engine(self):
    if self._engine is None:
      self._engine = create_engine(self._db_url, future=True)
      self._Session = sessionmaker(
        bind=self._engine,
        expire_on_commit=False,
      )


  def _get_session(self):
    self._ensure_engine()
    return self._Session()



class SqlAlchemyNodeStore(StoreBase):
  """Database store for managing Lilota nodes."""

  def __init__(self, db_url: str, logger: logging.Logger):
    """
    Args:
        db_url (str): Database connection URL.
        logger (logging.Logger): Logger for store operations.
    """
    super().__init__(db_url, logger)


  def create_node(self, type: NodeType, status: NodeStatus = NodeStatus.STARTING) -> UUID:
    """Create a new node record in the database.

    Args:
        type (NodeType): Type of the node (scheduler or worker).
        status (NodeStatus): Initial lifecycle status.

    Returns:
        UUID: The unique identifier of the created node.
    """
    node = Node(
      type=type,
      status=status
    )
    
    with self._get_session() as session:
      with session.begin():
        session.add(node)

    return node.id
  

  def get_all_nodes(self):
    """Return all nodes in the database."""
    with self._get_session() as session:
      return session.query(Node).all()
      

  def get_node_by_id(self, id: UUID):
    """Return a node by its UUID.

    Args:
        id (UUID): Node identifier.

    Returns:
        Node | None: Node object if found, else None.
    """
    with self._get_session() as session:
      node = session.get(Node, id)
      if node is None:
        return None
      return node
  

  def update_node_status(self, id: UUID, status: NodeStatus):
    """Update the status of a node.

    Args:
        id (UUID): Node identifier.
        status (NodeStatus): New status.
    """
    with self._get_session() as session:
      with session.begin():
        stmt = (
          update(Node).where(Node.id == id).values(status = status)
        )
        session.execute(stmt)


  def update_nodes_status_on_dead_nodes(self, cutoff: datetime, exclude_node_id: UUID):
    """Mark nodes as DEAD if their last_seen_at is older than cutoff.

    Args:
        cutoff (datetime): Time threshold to consider a node dead.
        exclude_node_id (UUID): Node to exclude from the update.

    Returns:
        int: Number of nodes marked as DEAD.
    """
    with self._get_session() as session:
      with session.begin():
        result = session.execute(
          update(Node)
          .where(Node.last_seen_at < cutoff)
          .where(Node.id != exclude_node_id)
          .where(Node.status != NodeStatus.DEAD)
          .values(status=NodeStatus.DEAD)
        )
        return result.rowcount


  def update_node_last_seen_at(self, id: UUID):
    """Update the heartbeat timestamp of a node.

    Args:
        id (UUID): Node identifier.
    """
    with self._get_session() as session:
      with session.begin():
        session.execute(
          update(Node)
          .where(Node.id == id)
          .values(last_seen_at = datetime.now(timezone.utc))
        )



class SqlAlchemyTaskStore(StoreBase):
  """Database store for managing Lilota tasks."""

  def __init__(self, db_url: str, logger: logging.Logger, set_progress_manually: bool = False):
    """
    Args:
        db_url (str): Database connection URL.
        logger (logging.Logger): Logger for store operations.
        set_progress_manually (bool): Whether task progress must be updated manually.
    """
    super().__init__(db_url, logger)
    self._set_progress_manually = set_progress_manually


  def create_task(self, name: str, input: Any = None):
    """Create a new task record in the database.

    Args:
        name (str): Registered task name.
        input (Any, optional): Input data for the task.

    Returns:
        UUID: The unique identifier of the created task.
    """
    if not input is None:
      input = normalize_data(input)

    task = Task(
      name=name,
      input=input,
      status = TaskStatus.CREATED
    )
    
    with self._get_session() as session:
      with session.begin():
        session.add(task)

    return task.id
  

  def get_all_tasks(self):
    """Return all tasks ordered by ID."""
    with self._get_session() as session:
      return session.query(Task).order_by(Task.id).all()
      

  def get_unfinished_tasks(self) -> list[Task]:
    """Return all tasks that are not yet completed or failed."""
    with self._get_session() as session:
      return (
        session.query(Task)
          .filter(Task.status.in_([TaskStatus.CREATED, TaskStatus.SCHEDULED, TaskStatus.RUNNING]))
          .order_by(Task.id)
          .all()
      )
    

  def has_unfinished_tasks(self) -> bool:
    """Check if there are any unfinished tasks in the database."""
    with self._get_session() as session:
      return session.query(
        session.query(Task)
        .filter(
          Task.status.in_([
            TaskStatus.CREATED,
            TaskStatus.SCHEDULED,
            TaskStatus.RUNNING
          ])
        )
        .exists()
      ).scalar()


  def get_task_by_id(self, id: UUID) -> Task:
    """Return a task by its UUID."""
    with self._get_session() as session:
      task = session.get(Task, id)
      if task is None:
        return None
      return task
    

  def get_next_task(self, worker_id: UUID) -> Task:
    """Return the next available task for a worker and lock it.

    Args:
        worker_id (UUID): Worker node locking the task.

    Returns:
        Task | None: The next scheduled task, or None if no task is available.
    """
    with self._get_session() as session:
      with session.begin():
        task_id = session.execute(
          select(Task.id)
          .where(Task.status == TaskStatus.CREATED)
          .where(Task.run_at <= datetime.now(timezone.utc))
          .order_by(Task.run_at)
          .limit(1)
        ).scalar()

        if not task_id:
          return None
        
        result = session.execute(
          update(Task)
          .where(Task.id == task_id)
          .where(Task.status == TaskStatus.CREATED)
          .values(
            status=TaskStatus.SCHEDULED,
            locked_at=datetime.now(timezone.utc),
            locked_by=worker_id,
          )
        )

      if result.rowcount != 1:
        return None
      
      return session.get(Task, task_id)


  def start_task(self, id: UUID) -> Task:
    """Mark a task as RUNNING and initialize metadata.

    Args:
        id (UUID): Id of the task.

    Returns:
        Task | None: The started task, or None if no task is available.
    """
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.pid = os.getpid()
        task.status = TaskStatus.RUNNING
        task.progress_percentage = 0
        task.start_date_time = datetime.now(timezone.utc)
        task.end_date_time = None
        return task


  def set_progress(self, id: UUID, progress: int):
    """Update the progress percentage of a task.
    
    Args:
        id (UUID): Id of the task.
        progress (int): The progress of the task (0-100)
    """
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.progress_percentage = max(0, min(progress, 100))


  def end_task_success(self, id: UUID, output: Any):
    """Mark a task as successfully completed.

    Args:
        id (UUID): Id of the task.
        output (Any): Task result data.
    """
    if not output is None:
      output = normalize_data(output)

    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.output = output
        self._complete_progress(task, TaskStatus.COMPLETED)


  def end_task_failure(self, id: UUID, error: dict):
    """Mark a task as failed.

    Args:
        id (UUID): Id of the task.
        error (dict): Error information to store.
    """
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.error = error
        self._complete_progress(task, TaskStatus.FAILED)


  def delete_task_by_id(self, id: UUID):
    """Delete a task by its UUID.

    Returns:
        bool: True if deleted, False if task not found.
    """
    with self._get_session() as session:
      with session.begin():
        task = session.get(Task, id)
        if task is None:
          return False
        session.delete(task)
    return True
  

  def _complete_progress(self, task: Task, task_status: TaskStatus):
    if not self._set_progress_manually:
      task.progress_percentage = 100
    task.status = task_status
    task.end_date_time = datetime.now(timezone.utc)


  def _load_task(self, session, id: UUID) -> Task:
    task: Task = session.get(Task, id)
    if task is None:
      raise ValueError(f"Task {id} not found")
    return task



class SqlAlchemyLogStore():
  """Database store for logging entries into Lilota."""

  def __init__(self, db_url: str):
    """
    Args:
        db_url (str): Database connection URL.
    """
    self._db_url = db_url
    self._engine = None
    self._Session = None


  def _ensure_engine(self):
    if self._engine is None:
      self._engine = create_engine(self._db_url, pool_pre_ping=True)
      self._Session = sessionmaker(
        bind=self._engine,
        expire_on_commit=False,
      )


  def get_session(self):
    """Return a new SQLAlchemy session."""
    self._ensure_engine()
    return self._Session()
  

  def get_log_entries_by_node_id(self, node_id: UUID) -> list[LogEntry]:
    """Return all log entries associated with a given node."""
    with self.get_session() as session:
      return (
        session.query(LogEntry)
          .filter(LogEntry.node_id == node_id)
          .order_by(LogEntry.created_at)
          .all()
      )
  


class SqlAlchemyNodeLeaderStore(StoreBase):
  """Store managing leader election for worker nodes."""

  def __init__(self, db_url: str, logger: logging.Logger, node_timeout_sec: int):
    """
    Args:
        db_url (str): Database connection URL.
        logger (logging.Logger): Logger for store operations.
        node_timeout_sec (int): Leader lease timeout in seconds.
    """
    super().__init__(db_url, logger)
    self._node_timeout_sec: int = node_timeout_sec
  

  def try_acquire_leadership(self, node_id) -> bool:
    """Attempt to acquire leadership for the given node.

    Returns True if leadership is acquired, False otherwise.
    """
    now = datetime.now(timezone.utc)
    new_expiry = now + timedelta(seconds=self._node_timeout_sec)
    session = self._get_session()

    try:
      # Try to take over expired lease
      result = session.execute(
        update(NodeLeader)
        .where(
          NodeLeader.id == 1,
          NodeLeader.lease_expires_at < now,
        )
        .values(
          node_id=node_id,
          lease_expires_at=new_expiry,
        )
      )

      if result.rowcount == 1:
        session.commit()
        self._logger.debug(f"Leadership acquired (new node id: {node_id})")
        return True

      # Check if row exists
      exists = session.execute(
        select(NodeLeader.id).where(NodeLeader.id == 1)
      ).first()

      if exists:
        session.rollback()
        return False

      # No row → try to create it
      session.add(
        NodeLeader(
          id=1,
          node_id=node_id,
          lease_expires_at=new_expiry,
        )
      )
      session.commit()
      self._logger.debug(f"Leadership acquired first time (node id: {node_id})")
      return True
    except IntegrityError:
        # Someone else won the race to insert
        session.rollback()
        return False
    except Exception:
        session.rollback()
        self._logger.exception("Leader election failed")
        return False
    finally:
        session.close()

  
  def renew_leadership(self, node_id):
    """Renew leadership lease for the given node.

    Returns True if renewed successfully.
    """
    now = datetime.now(timezone.utc)
    new_expiry = now + timedelta(seconds=self._node_timeout_sec)

    with self._get_session() as session:
      result = session.execute(
        update(NodeLeader)
        .where(
          NodeLeader.id == 1,
          NodeLeader.node_id == node_id,
          NodeLeader.lease_expires_at >= now
        )
        .values(lease_expires_at=new_expiry)
      )

      session.commit()
      renewed = result.rowcount == 1
      if renewed:
        self._logger.debug(f"Leadership renewed (node id: {node_id})")
      return renewed
    

  def delete_leader_by_id(self, id: int):
    """Delete the leader record by ID.

    Returns True if deleted successfully, False otherwise.
    """
    with self._get_session() as session:
      with session.begin():
        leader = session.get(Task, id)
        if leader is None:
          return False
        session.delete(leader)
    return True