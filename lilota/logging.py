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
  _refcount: int = 0


  def __new__(cls, db_url: str, logging_level: int):
    if cls._instance is None:
      cls._instance = super().__new__(cls)
      cls._instance._initialized = False
    return cls._instance


  def __init__(self, db_url: str, logging_level: int):
    if self._initialized:
      return

    self.queue = Queue()
    self.db_handler = SqlAlchemyHandler(SqlAlchemyLogStore(db_url))
    self.db_handler.setLevel(logging_level)
    self.db_handler.setFormatter(logging.Formatter("%(message)s"))
    self.db_handler.addFilter(LilotaLoggingFilter())
    self.listener: Optional[QueueListener] = None

    self._initialized = True


  def start(self):
    if self.listener is None:
      self.listener = QueueListener(self.queue, self.db_handler)
      self.listener.start()

    LoggingRuntime._refcount += 1


  def stop(self):
    if LoggingRuntime._refcount > 0:
      LoggingRuntime._refcount -= 1

    if LoggingRuntime._refcount == 0 and self.listener is not None:
      self.listener.stop()
      self.listener = None



def configure_logging(logging_queue: Queue, logging_level: int) -> logging.Logger:
  logger = logging.getLogger(f"{LILOTA_LOGGER_NAME}.{id(logging_queue)}")
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