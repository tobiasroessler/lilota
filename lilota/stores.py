from abc import ABC, abstractmethod
from .models import TaskInfo
from multiprocessing import Lock
from multiprocessing.managers import BaseManager


class StoreManager(BaseManager):
  pass


class TaskStoreBase(ABC):

  @abstractmethod
  def insert(self, task_info: TaskInfo):
    pass

  @abstractmethod
  def get_all_tasks(self):
    pass

  @abstractmethod
  def get_by_id(self, id):
    pass

  @abstractmethod
  def update(self, task_info: TaskInfo):
    pass


class MemoryTaskStore(TaskStoreBase):

  def __init__(self):
    self._tasks: list[TaskInfo] = []
    self._lock = Lock()


  def insert(self, name, description, input):
    self._lock.acquire()
    try:
      task_info = TaskInfo(name, description, input)
      self._tasks.append(task_info)
      return task_info.id
    finally:
      self._lock.release()


  def get_all_tasks(self):
    self._lock.acquire()
    try:
      return self._tasks
    except Exception as ex:
      # TODO: Add logging
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
      # TODO: Add logging
      return None
    finally:
      self._lock.release()

  
  def update(self, task_info: TaskInfo):
    self._lock.acquire()
    try:
      found_task = None

      for task in self._tasks:
        if task.id == task_info.id:
          found_task = task
          break 

      if not found_task:
        raise Exception(f"The task with the id '{task_info.id}' does not exist")
      
      found_task.name = task_info.name
      found_task.progress_percentage = task_info.progress_percentage

      self._tasks.append(task_info)
    except Exception as ex:
      # TODO: Add logging
      pass
    finally:
      self._lock.release()
