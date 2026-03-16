from abc import ABC, abstractmethod
from lilota.models import NodeType, NodeStatus
from lilota.stores import SqlAlchemyNodeStore, SqlAlchemyTaskStore
from lilota.heartbeat import Heartbeat, HeartbeatTask
from lilota.db.alembic import upgrade_db
from lilota.logging import configure_logging, create_context_logger
import logging
from uuid import UUID



class NodeHeartbeatTask(HeartbeatTask):
  """Heartbeat task used by Lilota nodes.

  This task periodically updates the ``last_seen_at`` timestamp of the
  associated node in the database to indicate that the node is still alive.
  """

  def __init__(self, interval: float, node_id: str, node_store: SqlAlchemyNodeStore, logger: logging.Logger):
    """Initialize the heartbeat task.

    Args:
      interval (float): Interval in seconds between heartbeat executions.
      node_id (str): Unique identifier of the node.
      node_store (SqlAlchemyNodeStore): Store used for node operations.
      logger (logging.Logger): Logger instance used for reporting errors.
    """
    super().__init__(interval)
    self._node_id = node_id
    self._node_store = node_store
    self._logger = logger


  def execute(self):
    """Execute the heartbeat update.

    Updates the ``last_seen_at`` field of the node in the database.
    Any errors are logged without interrupting the heartbeat loop.
    """
    try:
      self._node_store.update_node_last_seen_at(self._node_id)
    except Exception:
      self._logger.exception(f"Heartbeat update failed for node_id '{self._node_id}'")



class LilotaNode(ABC):
  """Abstract base class for all Lilota nodes.

  A node represents a running component in the Lilota system, such as a
  scheduler or a worker. This class handles common functionality including:

  - database initialization and migrations
  - node lifecycle management
  - node status updates
  - heartbeat management
  - access to node and task stores
  """

  def __init__(
    self,
    *,
    db_url: str,
    node_type: NodeType,
    node_heartbeat_interval: float,
    node_timeout_sec: int,
    logger_name: str,
    logging_level):
    """Initialize a Lilota node.

    Args:
      db_url (str): Database connection URL.
      node_type (NodeType): Type of node (`scheduler` or `worker`).
      node_heartbeat_interval (float): Interval in seconds between node
        heartbeat updates.
      node_timeout_sec (int): Time in seconds after which a node is
        considered inactive if no heartbeat is received.
      logger_name (str): Name of the logger used by the node.
      logging_level (int): Logging level used for the node logger.
    """

    self._db_url = db_url
    self._node_type = node_type
    self._node_heartbeat_interval = node_heartbeat_interval
    self._node_heartbeat_join_timeout_in_sec = 60
    self._node_timeout_sec = node_timeout_sec
    self._node_id = None
    self._node_store = None
    self._task_store = None
    self._heartbeat: Heartbeat = None
    self._logging_level = logging_level
    self._is_started = False

    # Upgrade the database
    upgrade_db(self._db_url)

    # Setup logging
    self._logger = configure_logging(self._db_url, logger_name, logging_level)


  def start(self):
    """Start the node.

    Initializes the node in the database, sets its status to ``RUNNING``,
    and triggers the node-specific startup logic implemented in
    ``_on_started()``.

    Raises:
      Exception: If the node is already started.
    """

    # Check if the node is already started
    if self._is_started:
      raise Exception("The node is already started")

    if not self._node_id:
      # Create stores
      self._node_store = SqlAlchemyNodeStore(self._db_url, self._logger)
      self._task_store = SqlAlchemyTaskStore(self._db_url, self._logger, self._should_set_progress_manually())

      # Create node with status STARTING
      self._node_id = self._node_store.create_node(self._node_type, NodeStatus.IDLE)

      # Create a context logger
      self._logger = create_context_logger(self._logger, node_id=self._node_id)

    # Change status to RUNNING
    self._node_store.update_node_status(self._node_id, NodeStatus.RUNNING)

    # Set the node as started
    self._is_started = True

    # On started
    self._on_started()


  def stop(self):
    """Stop the node.

    Updates the node status to ``STOPPING``, executes subclass-specific
    shutdown logic via ``_on_stop()``, and then sets the node status to
    ``IDLE``.

    Raises:
      Exception: If the node was not started.
    """

    # Check if the node was started
    if not self._is_started:
      raise Exception("The node cannot be stopped because it was not started")

    # Change status to STOPPING
    self._node_store.update_node_status(self._node_id, NodeStatus.STOPPING)

    # Stop additional stuff
    self._on_stop()

    # Change status to IDLE
    self._node_store.update_node_status(self._node_id, NodeStatus.IDLE)

    # Log Node stopped message
    self._logger.debug("Node stopped")

    # Set the node as not started
    self._is_started = False


  def get_nodes(self):
    """Return all nodes currently registered in the system.

    Returns:
      list: A list of node records.
    """
    return self._node_store.get_all_nodes()
  

  def get_node(self):
    """Return the current node record.

    Returns:
      Any | None: The node record if the node has been created,
      otherwise ``None``.
    """
    return self._node_store.get_node_by_id(self._node_id) if self._node_id else None
  

  def get_task_by_id(self, id: UUID):
    """Retrieve a task by its identifier.

    Args:
      id (UUID): Unique identifier of the task.

    Returns:
      Any: Task record associated with the given ID.
    """
    return self._task_store.get_task_by_id(id)
  

  def delete_task_by_id(self, id: UUID):
    """Delete a task from the system.

    Args:
      id (UUID): Unique identifier of the task.

    Returns:
      bool: True if the task was deleted, otherwise False.
    """
    self._logger.debug(f"Delete task with id {id}")
    success = self._task_store.delete_task_by_id(id)
    if success:
      self._logger.debug(f"Task deleted!")
    else:
      self._logger.debug(f"Task not deleted!")
    return success
  

  def _stop_node_heartbeat(self):
    # Stop heartbeat thread
    if self._heartbeat:
      self._heartbeat.stop_and_join(timeout=self._node_heartbeat_join_timeout_in_sec)
      self._heartbeat = None


  @abstractmethod
  def _on_started(self):
    """Hook executed after the node has successfully started.

    Subclasses should implement this method to start any required
    background threads, heartbeats, or task loops.
    """
    pass


  @abstractmethod
  def _on_stop(self):
    """Hook executed when the node is stopping.

    Subclasses should implement this method to clean up resources
    such as threads or background workers.
    """
    pass


  @abstractmethod
  def _should_set_progress_manually(self) -> bool:
    """Determine whether task progress should be set manually.

    Returns:
      bool: True if users must manually update the task progress, otherwise False.
    """
    pass
