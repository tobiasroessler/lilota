import logging
from datetime import datetime
from .models import LogEntry
from .stores import LogStore


class SqlAlchemyHandler(logging.Handler):
    """Logging handler that stores log records in the database.

    This handler writes log messages to the ``lilota_log`` table using
    the provided :class:`SqlAlchemyLogStore`. Each log record is converted
    into a :class:`LogEntry` model instance.
    """

    def __init__(self, log_store: LogStore):
        """Initialize the logging handler.

        Args:
          log_store (SqlAlchemyLogStore): Store used to persist log entries.
        """
        super().__init__()
        self.log_store: LogStore = log_store

    def emit(self, record: logging.LogRecord) -> None:
        """Persist a log record in the database.

        Args:
          record (logging.LogRecord): Log record to store.
        """
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
    """Logger adapter that automatically injects contextual metadata.

    This adapter attaches additional context such as ``node_id`` and
    ``task_id`` to log records so that log entries can be associated
    with specific nodes or tasks.
    """

    def process(self, msg, kwargs):
        """Inject contextual metadata into the log record.

        Args:
          msg (str): Log message.
          kwargs (dict): Keyword arguments passed to the logger.

        Returns:
          tuple: Processed ``(msg, kwargs)`` pair with injected metadata.
        """
        kwargs.setdefault("extra", {})
        kwargs["extra"].setdefault("node_id", self.extra.get("node_id"))
        kwargs["extra"].setdefault("task_id", self.extra.get("task_id"))
        return msg, kwargs


class LilotaLoggingFilter(logging.Filter):
    """Logging filter used to suppress noisy third-party logs.

    Currently filters Alembic logs so that only warnings and errors
    are recorded.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Determine whether a log record should be processed.

        Args:
          record (logging.LogRecord): Log record being evaluated.

        Returns:
          bool: ``True`` if the record should be logged, otherwise ``False``.
        """
        if record.name.startswith("alembic."):
            return record.levelno >= logging.WARNING
        return True


def configure_logging(
    db_url: str, logger_name: str, logging_level: int
) -> logging.Logger:
    """Configure a Lilota logger that writes log messages to the database.

    This function creates and configures a logger with the given name. The
    logger uses :class:`SqlAlchemyHandler` to persist log records in the
    database through :class:`SqlAlchemyLogStore`. Any existing handlers
    attached to the logger are removed before configuration.

    Args:
      db_url (str):
        Database connection URL used by the log store.

      logger_name (str):
        Name of the logger to configure.

      logging_level (int):
        Logging level to apply to both the logger and its handler.

    Returns:
      logging.Logger:
        Configured logger instance that writes log messages to the database.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging_level)
    logger.handlers.clear()
    db_handler = SqlAlchemyHandler(LogStore(db_url))
    db_handler.setLevel(logging_level)
    db_handler.setFormatter(logging.Formatter("%(message)s"))
    db_handler.addFilter(LilotaLoggingFilter())
    logger.addHandler(db_handler)
    return logger


def create_context_logger(base_logger: logging.Logger, **kwargs):
    """Create a context-aware logger.

    The returned logger automatically attaches contextual metadata
    (such as ``node_id`` and ``task_id``) to all emitted log records.

    Args:
      base_logger (logging.Logger): Base logger instance.
      **kwargs: Optional context values (e.g., ``node_id``, ``task_id``).

    Returns:
      ContextLogger: Logger adapter with contextual metadata.
    """
    extra = {}

    if "node_id" in kwargs:
        extra["node_id"] = kwargs["node_id"]

    if "task_id" in kwargs:
        extra["task_id"] = kwargs["task_id"]

    return ContextLogger(
        base_logger,
        extra,
    )
