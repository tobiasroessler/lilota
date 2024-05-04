from uuid import UUID, uuid4
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(init=False)
class TaskInfo():
  name: str
  description: str
  input: dict
  id: uuid4
  pid: int = 0
  progress_percentage: float = 0.0
  output: dict

  def __init__(self, name, description, input):
    self.id = uuid4()
    self.name = name
    self.description = description
    self.input = input


class TaskBase(ABC):

  def __init__(self, task_info: TaskInfo):
    self.task_info = task_info

  @abstractmethod
  def run(self):
    pass
