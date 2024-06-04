from dataclasses import dataclass
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
import logging


@dataclass(init=False)
class TaskInfo():
  name: str
  description: str
  id: int = 0
  pid: int = 0
  progress_percentage: int = 0
  start_date_time: datetime
  end_date_time: datetime
  input: dict
  output: dict

  def __init__(self, id, name, description, input):
    self.id = id
    self.name = name
    self.description = description
    self.input = input


class TaskBase(ABC):

  def __init__(self, task_info: TaskInfo, set_progress, set_output, logger: logging.Logger):
    self.task_info = task_info
    self.set_progress = set_progress
    self.set_output = set_output
    self.logger = logger

  @abstractmethod
  def run(self):
    pass
