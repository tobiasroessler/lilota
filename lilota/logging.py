import logging
from datetime import datetime
from .models import LogEntry
from .stores import SqlAlchemyLogStore
from typing import Optional


LILOTA_LOGGER_NAME = "lilota"


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

  def __new__(cls, db_url: str, logging_level: int):
    if cls._instance is None:
      cls._instance = super().__new__(cls)
      cls._instance._initialized = False
    return cls._instance


  def __init__(self, db_url: str, logging_level: int):
    if self._initialized:
      return

    self.db_handler = SqlAlchemyHandler(SqlAlchemyLogStore(db_url))
    self.db_handler.setLevel(logging_level)
    self.db_handler.setFormatter(logging.Formatter("%(message)s"))
    self.db_handler.addFilter(LilotaLoggingFilter())
    self._initialized = True



def configure_logging(db_url: str, logging_level: int) -> logging.Logger:
  logger = logging.getLogger(f"{LILOTA_LOGGER_NAME}")
  logger.setLevel(logging_level)
  if not any(isinstance(h, SqlAlchemyHandler) for h in logger.handlers):
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