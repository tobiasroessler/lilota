from abc import ABC, abstractmethod
from .models import TaskInfo
from multiprocessing import Lock
from multiprocessing.managers import BaseManager
from datetime import datetime, UTC
import logging
from logging.handlers import QueueHandler


class StoreManager(BaseManager):
  pass


class TaskStoreBase(ABC):

  @abstractmethod
  def insert(self, task_info: TaskInfo):
    pass

  @abstractmethod
  def get_all_tasks(self) -> list[TaskInfo]:
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
  def set_output(self, id: int, output: dict):
    pass

  @abstractmethod
  def set_end(self, id: int):
    pass


class MemoryTaskStore(TaskStoreBase):

  id = 0

  def __init__(self):
    self._tasks: list[TaskInfo] = []
    self._done_tasks: list[TaskInfo] = []
    self._lock = Lock()


  def insert(self, name, description, input):
    self._lock.acquire()
    try:
      task_info = TaskInfo(MemoryTaskStore.id, name, description, input)
      MemoryTaskStore.id += 1
      self._tasks.append(task_info)
      return task_info.id
    finally:
      self._lock.release()


  def get_all_tasks(self) -> list[TaskInfo]:
    self._lock.acquire()
    try:
      return self._tasks + self._done_tasks
    except Exception as ex:
      return None
    finally:
      self._lock.release()


  def get_by_id(self, id):
    self._lock.acquire()
    try:
      for task in self._tasks:
        if task.id == id:
          return task
    except Exception as ex:
      return None
    finally:
      self._lock.release()


  def set_start(self, id: int):
    self._lock.acquire()
    try:
      found_task = None

      for task in self._tasks:
        if task.id == id:
          found_task = task
          break 

      if not found_task:
        raise Exception(f"The task with the id '{id}' does not exist")
      
      found_task.start_date_time = datetime.now(UTC)
    finally:
      self._lock.release()

  
  def set_progress(self, id: int, progress: int):
    self._lock.acquire()
    try:
      found_task = None

      for task in self._tasks:
        if task.id == id:
          found_task = task
          break 

      if not found_task:
        raise Exception(f"The task with the id '{id}' does not exist")
      
      found_task.progress_percentage = progress
    finally:
      self._lock.release()


  def set_output(self, id: int, output: dict):
    self._lock.acquire()
    try:
      found_task = None

      for task in self._tasks:
        if task.id == id:
          found_task = task
          break 

      if not found_task:
        raise Exception(f"The task with the id '{id}' does not exist")
      
      found_task.output = output
    finally:
      self._lock.release()


  def set_end(self, id: int):
    self._lock.acquire()
    try:
      found_task = None
      index = -1

      for i in range(len(self._tasks)):
        task = self._tasks[i]
        if task.id == id:
          found_task = task
          index = i
          break 

      if not found_task:
        raise Exception(f"The task with the id '{id}' does not exist")
      
      found_task.end_date_time = datetime.now(UTC)
      found_task.progress_percentage = 100

      del self._tasks[index]
      self._done_tasks.append(found_task)
    finally:
      self._lock.release()
