from typing import Any, Callable, Type, TypeVar, Optional, Any, Protocol, runtime_checkable
from sqlalchemy import String, DateTime, JSON, CheckConstraint
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from datetime import datetime, timezone
from dataclasses import is_dataclass, asdict
from enum import StrEnum
from uuid import UUID, uuid4


Base = declarative_base()


class TaskStatus(StrEnum):
  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"
  CANCELLED = "cancelled"



class NodeStatus(StrEnum):
  IDLE = "idle"
  STARTING = "starting"
  RUNNING = "running"
  STOPPING = "stopping"
  STOPPED = "stopped"
  CRASHED = "crashed"



class NodeType(StrEnum):
  SCHEDULER = "scheduler"
  WORKER = "worker"



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
  


class Node(Base):
  __tablename__ = "lilota_node"
  id: Mapped[UUID] = mapped_column(
    primary_key=True,
    default=uuid4
  )
  name: Mapped[str | None] = mapped_column(String(255), nullable=True)
  type: Mapped[str] = mapped_column(
    String(32),
    nullable=False
  )
  status: Mapped[str] = mapped_column(
    String(32),
    nullable=False
  )
  created_at: Mapped[datetime] = mapped_column(
    DateTime, default=lambda: datetime.now(timezone.utc),
    nullable=False
  )
  last_seen_at: Mapped[datetime] = mapped_column(
    DateTime, default=lambda: datetime.now(timezone.utc),
    nullable=False
  )

  __table_args__ = (
    CheckConstraint(
        "type IN ('scheduler', 'worker')",
        name="lilota_note_type_check"
    ),
    CheckConstraint(
        "status IN ('idle', 'starting', 'running', 'stopping', 'stopped', 'crashed')",
        name="lilota_note_status_check"
    )
  )


class Task(Base):
  __tablename__ = "lilota_task"
  id: Mapped[int] = mapped_column(primary_key=True)
  name: Mapped[str] = mapped_column(String, nullable=False)
  pid: Mapped[int] = mapped_column(default=0, nullable=False)
  status: Mapped[str] = mapped_column(
    String(32),
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
  locked_by: Mapped[UUID | None] = mapped_column(default=None)
  locked_at: Mapped[datetime | None] = mapped_column(nullable=True, default=None)

  __table_args__ = (
    CheckConstraint(
        "status IN ('pending', 'running', 'completed', 'failed', 'cancelled')",
        name="lilota_task_status_check"
    ),
  )

  def __repr__(self):
    return f"<TaskInfo(id={self.id}, name={self.name}, progress={self.progress_percentage}%)>"
