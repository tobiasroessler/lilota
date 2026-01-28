import logging
from logging.handlers import QueueListener, QueueHandler
from datetime import datetime
from multiprocessing import Queue
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Literal, Optional
from .models import LogEntry
from .stores import SqlAlchemyLogStore


LILOTA_LOGGER_NAME = "lilota"
LOGGING_FORMATTER_DEFAULT = "%(asctime)s [PID %(process)d]: [%(levelname)s] - %(message)s"


class SqlAlchemyHandler(logging.Handler):

  def __init__(self, log_store: SqlAlchemyLogStore):
    super().__init__()
    self.log_store: SqlAlchemyLogStore = log_store


  def emit(self, record: logging.LogRecord) -> None:
    with self.log_store.get_session() as session:
      entry = LogEntry(
        created_at=datetime.fromtimestamp(record.created),
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
  


class LoggingRuntime:

  _instance: Optional["LoggingRuntime"] = None

  def __init__(self, db_url: str, logging_level: int):
    if LoggingRuntime._instance is not None:
      return

    self.queue = Queue()
    db_handler = SqlAlchemyHandler(SqlAlchemyLogStore(db_url))
    db_handler.setLevel(logging_level)
    db_handler.setFormatter(logging.Formatter("%(message)s"))
    db_handler.addFilter(LilotaLoggingFilter())

    # TODO: With this logger nothing is logged. Why?
    # logger = logging.getLogger(LILOTA_LOGGER_NAME)
    # logger.setLevel(logging_level)
    # logger.addHandler(QueueHandler(self.queue))
    # logger.propagate = False

    self.listener = QueueListener(self.queue, db_handler)
    self.listener.start()

    LoggingRuntime._instance = self


  @classmethod
  def get(cls) -> "LoggingRuntime":
    if cls._instance is None:
      raise RuntimeError("LoggingRuntime not initialized")
    return cls._instance


  def stop(self):
    if self.listener:
      self.listener.stop()
      self.listener = None



def configure_logging(logging_queue: Queue, logging_level: int) -> None:
  logger = logging.getLogger(LILOTA_LOGGER_NAME)
  logger.setLevel(logging_level)

  if not any(isinstance(h, QueueHandler) for h in logger.handlers):
    logger.addHandler(QueueHandler(logging_queue))

  logger.propagate = False
  return logger


def create_context_logger(base_logger: logging.Logger, **kwargs):
  extra = {}

  if "node_id" in kwargs:
    extra["node_id"] = kwargs["node_id"]

  if "task_id" in kwargs:
    extra["task_id"] = kwargs["task_id"]

  return ContextLogger(base_logger, extra,)
