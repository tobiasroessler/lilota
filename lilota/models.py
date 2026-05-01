from typing import Any, Callable, Type, TypeVar, Optional, Protocol, runtime_checkable
from sqlalchemy import Integer, String, Text, DateTime, JSON, CheckConstraint, Index
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from datetime import datetime, timezone, timedelta
from dataclasses import is_dataclass, asdict
from enum import StrEnum
from uuid import UUID, uuid4


Base = declarative_base()


class TaskStatus(StrEnum):
  """Enumeration of possible task states."""
  CREATED = "created"
  SCHEDULED = "scheduled"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"
  EXPIRED = "expired"
  CANCELLED = "cancelled"



class NodeStatus(StrEnum):
  """Enumeration describing the lifecycle state of a node."""
  IDLE = "idle"
  STARTING = "starting"
  RUNNING = "running"
  STOPPING = "stopping"
  STOPPED = "stopped"
  CRASHED = "crashed"
  DEAD ="dead"



class NodeType(StrEnum):
  """Enumeration of supported node types."""
  SCHEDULER = "scheduler"
  WORKER = "worker"



class TaskProgress:
  """Helper object used to update task progress.

  This object is passed to task functions when progress tracking is
  enabled. It allows the task to report its current progress to the
  task store.

  Args:
    task_id (int): Identifier of the task.
    set_progress (Callable[[int, int], None]): Function used to persist
      progress updates.
  """

  def __init__(self, task_id: int, set_progress: Callable[[int, int], None]):
    self.task_id = task_id
    self.set_progress = set_progress

  def set(self, progress: int):
    """Update the progress of the task.

    Args:
      progress (int): Progress value (typically 0–100).
    """
    self.set_progress(self.task_id, progress)



T = TypeVar("T", bound="ModelProtocol")

@runtime_checkable
class ModelProtocol(Protocol):
  """Protocol for models that can be serialized to dictionaries."""

  def as_dict(self) -> dict[str, Any]:
    """Return a dictionary representation of the model."""
    pass



class RegisteredTask:
  """Wrapper representing a registered task function.

  This class stores metadata about the task function such as input
  and output models and optionally a progress tracker.

  It is responsible for:
  - deserializing the task input
  - executing the task function
  - serializing the output

  Args:
    func (Callable): Task function to execute.
    input_model (Optional[Type]): Optional input model used to
      deserialize input payloads.
    output_model (Optional[Type]): Optional output model used to
      serialize the task result.
    task_progress (Optional[TaskProgress]): Optional progress helper.
    timeout (Optional[timedelta]): Optional timeout that can be set for a task.
    max_attempts (int): Upper limit on how many times the task may be executed.
  """

  def __init__(self, func: Callable, input_model: Optional[Type], output_model: Optional[Type], task_progress: Optional[TaskProgress], timeout: Optional[timedelta], max_attempts: int):
    self.func = func
    self.input_model = input_model
    self.output_model = output_model
    self.task_progress = task_progress
    self.timeout = timeout
    self.max_attempts = max_attempts


  def __call__(self, raw_input: Any, task_progress: TaskProgress):
    """Execute the registered task.

    Args:
      raw_input (Any): Raw input payload stored in the database.
      task_progress (TaskProgress): Progress tracking object.

    Returns:
      Any: Serialized task result.
    """

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
  """Database model representing a Lilota node.

  A node represents a running component of the system such as a
  scheduler or a worker.
  """

  __tablename__ = "lilota_node"
  id: Mapped[UUID] = mapped_column(
    primary_key=True,
    default=uuid4
  )
  name: Mapped[str | None] = mapped_column(String(255), nullable=True)
  type: Mapped[str] = mapped_column(String(32), nullable=False)
  status: Mapped[str] = mapped_column(String(32), nullable=False)
  created_at: Mapped[datetime] = mapped_column(
    DateTime(timezone=True), 
    default=lambda: datetime.now(timezone.utc),
    nullable=False
  )
  last_seen_at: Mapped[datetime] = mapped_column(
    DateTime(timezone=True), 
    default=lambda: datetime.now(timezone.utc),
    nullable=False
  )

  __table_args__ = (
    CheckConstraint(
        "type IN ('scheduler', 'worker')",
        name="lilota_note_type_check"
    ),
    CheckConstraint(
        "status IN ('idle', 'starting', 'running', 'stopping', 'stopped', 'crashed', 'dead')",
        name="lilota_note_status_check"
    )
  )



class Task(Base):
  """Database model representing a scheduled task.

  Tasks are created by the scheduler and executed by worker nodes.
  The model stores the execution state, input/output data, and
  runtime metadata.
  """

  __tablename__ = "lilota_task"
  id: Mapped[UUID] = mapped_column(
    primary_key=True,
    default=uuid4
  )
  name: Mapped[str] = mapped_column(String, nullable=False)
  pid: Mapped[int] = mapped_column(nullable=False, default=0)
  status: Mapped[str] = mapped_column(
    String(32),
    nullable=False,
    index=False
  )
  run_at: Mapped[datetime] = mapped_column(
    DateTime(timezone=True), 
    nullable=False,
    default=lambda: datetime.now(timezone.utc)
  )
  attempts: Mapped[int] = mapped_column(nullable=False, default=0)
  max_attempts: Mapped[int] = mapped_column(nullable=False, default=1)
  retried_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None, index=True)
  previous_task_id: Mapped[UUID] = mapped_column(nullable=True, default=None)
  timeout: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
  expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
  progress_percentage: Mapped[int] = mapped_column(default=0)
  start_date_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
  end_date_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
  input: Mapped[Any | None] = mapped_column(JSON)
  output: Mapped[Any | None] = mapped_column(JSON)
  error: Mapped[Any | None] = mapped_column(JSON)
  locked_by: Mapped[UUID | None] = mapped_column(default=None)
  locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
  version: Mapped[int] = mapped_column(default=0, nullable=False)

  __table_args__ = (
    CheckConstraint(
      "status IN ('created', 'scheduled', 'running', 'completed', 'failed', 'expired', 'cancelled')",
      name="lilota_task_status_check"
    ),
    Index("idx_get_next_task", "status", "run_at"),
    Index("idx_retryable_tasks", "retried_at", "status", "run_at"),
  )

  def __repr__(self):
    return f"<TaskInfo(id={self.id}, name={self.name}, progress={self.progress_percentage}%)>"
  


class LogEntry(Base):
  """Database model storing log messages generated by Lilota."""

  __tablename__ = "lilota_log"
  id: Mapped[int] = mapped_column(primary_key=True)
  created_at: Mapped[datetime] = mapped_column(
    DateTime(timezone=True),
    nullable=False,
  )
  level: Mapped[str] = mapped_column(String(20), nullable=False)
  logger: Mapped[str] = mapped_column(String(255), nullable=False)
  message: Mapped[str] = mapped_column(Text, nullable=False)
  process: Mapped[str | None] = mapped_column(String(64), default=None)
  thread: Mapped[str | None] = mapped_column(String(64), default=None)
  node_id: Mapped[UUID | None] = mapped_column(default=None)
  task_id: Mapped[UUID | None] = mapped_column(default=None)



class NodeLeader(Base):
  """Database model representing the worker node currently acting as leader.

  The leader is responsible for cluster-level maintenance tasks such as
  cleaning up stale nodes.
  """
  
  __tablename__ = "lilota_node_leader"
  id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
  node_id: Mapped[UUID] = mapped_column(nullable=False)
  lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)