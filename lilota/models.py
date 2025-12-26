from typing import Any
from sqlalchemy import String, DateTime, JSON, Enum as SqlEnum
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from datetime import datetime, timezone
from enum import Enum


Base = declarative_base()


class TaskStatus(str, Enum):
  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"
  CANCELLED = "cancelled"


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
