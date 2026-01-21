import logging
from logging.handlers import QueueListener, QueueHandler
from multiprocessing import Queue
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Literal
from .models import LogEntry
from .stores import SqlAlchemyLogStore


LOGGING_FORMATTER_DEFAULT = "%(asctime)s [PID %(process)d]: [%(levelname)s] - %(message)s"


class SqlAlchemyHandler(logging.Handler):

  def __init__(self, log_store: SqlAlchemyLogStore):
    super().__init__()
    self.log_store: SqlAlchemyLogStore = log_store


  def emit(self, record: logging.LogRecord) -> None:
    with self.log_store.get_session() as session:
      entry = LogEntry(
        level=record.levelname,
        logger=record.name,
        message=self.format(record),
        process=record.processName,
        thread=record.threadName,
        node_id=getattr(record, "node_id", None),
        task_id=getattr(record, "task_id", None),
      )
      session.add(entry)
      session.commit()



class ContextLogger(logging.LoggerAdapter):

  def process(self, msg, kwargs):
    kwargs.setdefault("extra", {})
    kwargs["extra"].setdefault("node_id", self.extra.get("node_id"))
    kwargs["extra"].setdefault("task_id", self.extra.get("task_id"))
    return msg, kwargs



class LilotaLoggingFilter(logging.Filter):

  def filter(self, record: logging.LogRecord) -> bool:
    if record.name.startswith("alembic."):
      return record.levelno >= logging.WARNING
    return True
    


def configure_logging_listener(db_url: str, logging_queue: Queue, logging_level: Literal[30]) -> QueueListener:
  db_handler = SqlAlchemyHandler(SqlAlchemyLogStore(db_url))
  db_handler.setLevel(logging_level)
  db_handler.setFormatter(logging.Formatter("%(message)s"))
  db_handler.addFilter(LilotaLoggingFilter())
  listener = QueueListener(logging_queue, db_handler)
  listener.start()
  return listener


def configure_logging(logging_queue: Queue, logging_level: int) -> None:
  root = logging.getLogger()
  root.setLevel(logging_level)
  root.handlers.clear()
  root.addHandler(QueueHandler(logging_queue))


def configure_logging_with_dbhandler(db_url: str, logging_level: int) -> None:
  logger = logging.getLogger()
  logger.setLevel(logging_level)
  db_handler = SqlAlchemyHandler(SqlAlchemyLogStore(db_url))
  db_handler.setLevel(logging_level)
  db_handler.setFormatter(logging.Formatter("%(message)s"))
  db_handler.addFilter(LilotaLoggingFilter())
  logger.addHandler(db_handler)
  return logger


def create_context_logger(base_logger: logging.Logger, **kwargs):
  extra = {}

  if "node_id" in kwargs:
    extra["node_id"] = kwargs["node_id"]

  if "task_id" in kwargs:
    extra["task_id"] = kwargs["task_id"]

  return ContextLogger(base_logger, extra,)