from abc import ABC, abstractmethod
from .models import Task
from datetime import datetime, UTC, timezone
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Any


class TaskStoreBase(ABC):

  @abstractmethod
  def insert(self, name: str, description: str, input: Any):
    pass

  @abstractmethod
  def get_all_tasks(self) -> list[Task]:
    pass

  @abstractmethod
  def get_by_id(self, id):
    pass

  @abstractmethod
  def set_start(self, id: int):
    pass

  @abstractmethod
  def set_progress(self, id: int, progress: int):
    pass

  @abstractmethod
  def set_output(self, id: int, output: Any):
    pass

  @abstractmethod
  def set_end(self, id: int):
    pass



class SqlAlchemyTaskStore(TaskStoreBase):

  def __init__(self, db_url: str):
    self._db_url = db_url
    self._engine = None
    self._Session = None


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


  def insert(self, name: str, description: str, input: Any):
    try:
      with self._get_session() as session:
        # TODO: Here we should also support dataclasses
        if isinstance(input, BaseModel):
          input = input.model_dump()

        task = Task(
          name=name,
          description=description,
          input=input,
        )
        
        session.add(task)
        session.commit()
        return task.id
    except Exception as ex:
      pass


  def get_all_tasks(self):
    with self._get_session() as session:
      tasks = session.query(Task).order_by(Task.id).all()
      return [t for t in tasks]


  def get_by_id(self, id: int):
    with self._get_session() as session:
      task = session.get(Task, id)
      if task is None:
        return None
      return task


  def set_start(self, id: int):
    with self._get_session() as session:
      task = session.get(Task, id)
      if task is None:
        raise ValueError(f"Task {id} not found")

      task.start_date_time = datetime.now(timezone.utc)
      session.commit()


  def set_progress(self, id: int, progress: int):
    with self._get_session() as session:
      task = session.get(Task, id)
      if task is None:
        raise ValueError(f"Task {id} not found")

      task.progress_percentage = max(0, min(progress, 100))
      session.commit()


  def set_output(self, id: int, output: Any):
    with self._get_session() as session:
      if isinstance(output, BaseModel):
        output = output.model_dump()

      task = session.get(Task, id)
      if task is None:
        raise ValueError(f"Task {id} not found")

      task.output = output
      session.commit()


  def set_end(self, id: int):
    with self._get_session() as session:
      task = session.get(Task, id)
      if task is None:
        raise ValueError(f"Task {id} not found")

      task.end_date_time = datetime.now(timezone.utc)
      task.progress_percentage = 100
      session.commit()
      
