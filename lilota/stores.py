from abc import ABC
import os
from .models import Node, NodeType, NodeStatus, NodeLeader, Task, TaskStatus, LogEntry
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from typing import Any
from lilota.utils import exception_to_dict, normalize_data
from uuid import uuid4
import logging


class StoreBase(ABC):

  def __init__(self, db_url: str, logger: logging.Logger):
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

  def __init__(self, db_url: str, logger: logging.Logger):
    super().__init__(db_url, logger)


  def create_node(self, type: NodeType, status: NodeStatus = NodeStatus.STARTING) -> uuid4:
    node = Node(
      type=type,
      status=status
    )
    
    with self._get_session() as session:
      with session.begin():
        session.add(node)

    return node.id
  

  def get_all_nodes(self):
    with self._get_session() as session:
      return session.query(Node).all()
      

  def get_node_by_id(self, id: uuid4):
    with self._get_session() as session:
      node = session.get(Node, id)
      if node is None:
        return None
      return node
  

  def update_node_status(self, id: uuid4, status: NodeStatus):
    with self._get_session() as session:
      with session.begin():
        stmt = (
          update(Node).where(Node.id == id).values(status = status)
        )
        session.execute(stmt)


  def update_nodes_status_on_dead_nodes(self, cutoff: datetime, exclude_node_id: uuid4):
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


  def update_node_last_seen_at(self, id: uuid4):
    with self._get_session() as session:
      with session.begin():
        session.execute(
          update(Node)
          .where(Node.id == id)
          .values(last_seen_at = datetime.now(timezone.utc))
        )



class SqlAlchemyTaskStore(StoreBase):

  def __init__(self, db_url: str, logger: logging.Logger, set_progress_manually: bool = False):
    super().__init__(db_url, logger)
    self._set_progress_manually = set_progress_manually


  def create_task(self, name: str, input: Any = None):
    if not input is None:
      input = normalize_data(input)

    task = Task(
      name=name,
      input=input,
      status = TaskStatus.PENDING
    )
    
    with self._get_session() as session:
      with session.begin():
        session.add(task)

    return task.id
  

  def get_all_tasks(self):
    with self._get_session() as session:
      return session.query(Task).order_by(Task.id).all()
      

  def get_unfinished_tasks(self) -> list[Task]:
    with self._get_session() as session:
      return (
        session.query(Task)
          .filter(Task.status.in_([TaskStatus.PENDING,TaskStatus.RUNNING]))
          .order_by(Task.id)
          .all()
      )


  def get_task_by_id(self, id: uuid4) -> Task:
    with self._get_session() as session:
      task = session.get(Task, id)
      if task is None:
        return None
      return task
    

  def get_next_task(self, worker_id: uuid4) -> Task:
    with self._get_session() as session:
      task_id = session.execute(
        select(Task.id)
        .where(Task.status == "pending")
        .where(Task.run_at <= datetime.now(timezone.utc))
        .order_by(Task.run_at)
        .limit(1)
      ).scalar()

      if not task_id:
        return None
      
      result = session.execute(
        update(Task)
        .where(Task.id == task_id)
        .where(Task.status == "pending")
        .values(
          status="running",
          locked_at=datetime.now(timezone.utc),
          locked_by=worker_id,
        )
      )

      session.commit()

      if result.rowcount != 1:
        return None
      
      return session.get(Task, task_id)


  def start_task(self, id: uuid4) -> Task:
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.pid = os.getpid()
        task.status = TaskStatus.RUNNING
        task.progress_percentage = 0
        task.start_date_time = datetime.now(timezone.utc)
        task.end_date_time = None
        return task


  def set_progress(self, id: uuid4, progress: int):
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.progress_percentage = max(0, min(progress, 100))


  def end_task_success(self, id: uuid4, output: Any):
    if not output is None:
      output = normalize_data(output)

    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.output = output
        self._complete_progress(task, TaskStatus.COMPLETED)


  def end_task_failure(self, id: uuid4, ex: Exception):
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.exception = exception_to_dict(ex)
        self._complete_progress(task, TaskStatus.FAILED)


  def delete_task_by_id(self, id: uuid4):
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


  def _load_task(self, session, id: uuid4) -> Task:
    task: Task = session.get(Task, id)
    if task is None:
      raise ValueError(f"Task {id} not found")
    return task



class SqlAlchemyLogStore():

  def __init__(self, db_url: str):
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
    self._ensure_engine()
    return self._Session()
  

  def get_log_entries_by_node_id(self, node_id: uuid4) -> list[LogEntry]:
    with self.get_session() as session:
      return (
        session.query(LogEntry)
          .filter(LogEntry.node_id == node_id)
          .order_by(LogEntry.created_at)
          .all()
      )
  


class SqlAlchemyNodeLeaderStore(StoreBase):

  def __init__(self, db_url: str, logger: logging.Logger, node_timeout_sec: int):
    super().__init__(db_url, logger)
    self._node_timeout_sec: int = node_timeout_sec
  

  def try_acquire_leadership(self, node_id) -> bool:
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
        self._logger.info("Leadership acquired (takeover)")
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
      self._logger.info("Leadership acquired (first leader)")
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
    now = datetime.now(timezone.utc)
    new_expiry = now + timedelta(seconds=self._node_timeout_sec)

    with self._get_session() as session:
      result = session.execute(
        update(NodeLeader)
        .where(
          NodeLeader.id == 1,
          NodeLeader.node_id == node_id,
        )
        .values(lease_expires_at=new_expiry)
      )

      session.commit()
      return result.rowcount == 1
    

  def delete_leader_by_id(self, id: int):
    with self._get_session() as session:
      with session.begin():
        leader = session.get(Task, id)
        if leader is None:
          return False
        session.delete(leader)
    return True