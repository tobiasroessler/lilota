from typing import Any, Callable, Type, TypeVar, Optional, Any, Protocol, runtime_checkable
from sqlalchemy import String, DateTime, JSON, Enum as SqlEnum
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from datetime import datetime, timezone
from dataclasses import is_dataclass, asdict
from enum import Enum


Base = declarative_base()


class TaskStatus(str, Enum):
  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"
  CANCELLED = "cancelled"



class TaskProgress:

  def __init__(self, task_id: int, set_progress: Callable[[int, int], None]):
    self.task_id = task_id
    self.set_progress = set_progress

  def set(self, progress: int):
    self.set_progress(self.task_id, progress)



T = TypeVar("T", bound="ModelProtocol")

@runtime_checkable
class ModelProtocol(Protocol):

  def as_dict(self) -> dict[str, Any]:
    pass



class RegisteredTask:

  def __init__(self, func: Callable, input_model: Optional[Type], output_model: Optional[Type], task_progress: Optional[TaskProgress]):
    self.func = func
    self.input_model = input_model
    self.output_model = output_model
    self.task_progress = task_progress


  def __call__(self, raw_input: Any, task_progress: TaskProgress):
    # Deserialize input
    input_value = self._deserialize_input(raw_input)

    # Execute the function
    if task_progress is None:
      if input_value is None:
        result = self.func()
      else:
        result = self.func(input_value)
    else:
      if input_value is None:
        result = self.func(task_progress)
      else:
        result = self.func(input_value, task_progress)

    # Serialize output to JSON-safe dict
    return self._serialize_output(result)


  def _deserialize_input(self, raw_input: Any):
    if not self.input_model:
      return raw_input
    if isinstance(self.input_model, ModelProtocol):
      return self.input_model(**raw_input)
    if is_dataclass(self.input_model):
      return self.input_model(**raw_input)
    if isinstance(raw_input, dict):
      return raw_input
    if isinstance(raw_input, self.input_model):
      return raw_input
    raise TypeError(f"Unsupported input_model type: {self.input_model}")


  def _serialize_output(self, output: Any):
    if not self.output_model:
      return output
    if isinstance(output, dict):
      return output
    if isinstance(output, ModelProtocol):
      return output.as_dict()
    if isinstance(output, self.output_model):
      if is_dataclass(output):
        return asdict(output)
    if is_dataclass(self.output_model):
      return self.output_model(**output)
    return output



class Task(Base):
  __tablename__ = "task"
  id: Mapped[int] = mapped_column(primary_key=True)
  name: Mapped[str] = mapped_column(String, nullable=False)
  pid: Mapped[int] = mapped_column(default=0)
  status: Mapped[TaskStatus] = mapped_column(
    SqlEnum(TaskStatus, name="status"),
    default=TaskStatus.PENDING,
    nullable=False
  )
  progress_percentage: Mapped[int] = mapped_column(default=0)
  start_date_time: Mapped[datetime] = mapped_column(
    DateTime, default=lambda: datetime.now(timezone.utc)
  )
  end_date_time: Mapped[datetime | None] = mapped_column(DateTime)
  input: Mapped[Any | None] = mapped_column(JSON)
  output: Mapped[Any | None] = mapped_column(JSON)
  exception: Mapped[Any | None] = mapped_column(JSON)

  def __repr__(self):
    return f"<TaskInfo(id={self.id}, name={self.name}, progress={self.progress_percentage}%)>"
