from abc import ABC, abstractmethod
from .models import Task, TaskStatus
from datetime import datetime, UTC, timezone
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Any
from lilota.utils import exception_to_dict, normalize_data


class TaskStoreBase(ABC):

  @abstractmethod
  def create_task(self, name: str, input: Any):
    pass

  @abstractmethod
  def get_all_tasks(self) -> list[Task]:
    pass

  @abstractmethod
  def get_task_by_id(self, id):
    pass

  @abstractmethod
  def start_task(self, id: int):
    pass

  @abstractmethod
  def set_progress(self, id: int, progress: int):
    pass

  @abstractmethod
  def end_task_success(self, id: int, output: Any):
    pass

  @abstractmethod
  def end_task_failure(self, id: int, ex: Exception):
    pass



class SqlAlchemyTaskStore(TaskStoreBase):

  def __init__(self, db_url: str, set_progress_manually: bool = False):
    self._db_url = db_url
    self._set_progress_manually = set_progress_manually
    self._engine = None
    self._Session = None


  def create_task(self, name: str, input: Any):
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
      with session.begin():
        return session.query(Task).order_by(Task.id).all()


  def get_task_by_id(self, id: int):
    with self._get_session() as session:
      with session.begin():
        task = session.get(Task, id)
        if task is None:
          return None
        return task


  def start_task(self, id: int) -> Task:
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.start_date_time = datetime.now(timezone.utc)
        task.status = TaskStatus.RUNNING
        return task


  def set_progress(self, id: int, progress: int):
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.progress_percentage = max(0, min(progress, 100))


  def end_task_success(self, id: int, output: Any):
    output = normalize_data(output)

    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.output = output
        self._complete_progress(task, TaskStatus.COMPLETED)


  def end_task_failure(self, id: int, ex: Exception):
    with self._get_session() as session:
      with session.begin():
        task = self._load_task(session, id)
        task.exception = exception_to_dict(ex)
        self._complete_progress(task, TaskStatus.FAILED)


  def delete_task_by_id(self, id: int):
    with self._get_session() as session:
      with session.begin():
        task = session.get(Task, id)
        if task is None:
          return False
        session.delete(task)
    return True
  

  def _complete_progress(self, task: Task, tasl_status: TaskStatus):
    if not self._set_progress_manually:
      task.progress_percentage = 100
    task.status = tasl_status
    task.end_date_time = datetime.now(timezone.utc)


  def _get_engine(self):
    if self._engine is None:
      self._engine = create_engine(self._db_url, future=True)
      self._Session = sessionmaker(
        bind=self._engine,
        expire_on_commit=False,
      )


  def _get_session(self):
    self._get_engine()
    return self._Session()


  def _load_task(self, session, id: int) -> Task:
    task: Task = session.get(Task, id)
    if task is None:
      raise ValueError(f"Task {id} not found")
    return task
