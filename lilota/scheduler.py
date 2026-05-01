from typing import Any
from lilota.node import LilotaNode, NodeHeartbeatTask
from lilota.models import NodeType
from lilota.heartbeat import Heartbeat
import logging


class LilotaScheduler(LilotaNode):
    """Scheduler node responsible for creating and managing tasks.

    The scheduler registers itself as a scheduler node in the system and
    periodically sends heartbeats to indicate it is alive. Its main role
    is to create tasks and store them in the task store for workers to
    execute.
    """

    LOGGER_NAME = "lilota.scheduler"

    def __init__(
        self,
        db_url: str,
        node_heartbeat_interval: float = 5.0,
        node_timeout_sec: int = 20,
        logging_level=logging.INFO,
        **kwargs,
    ):
        """Initialize the scheduler node.

        Args:
          db_url (str): Database connection URL.
          node_heartbeat_interval (float, optional): Interval in seconds between
            node heartbeats. Defaults to 5.0.
          node_timeout_sec (int, optional): Time in seconds before a node is
            considered inactive. Defaults to 20.
          logging_level (int, optional): Logging level used by the scheduler.
            Defaults to logging.INFO.
          **kwargs: Additional keyword arguments passed to the parent
            ``LilotaNode`` initializer.
        """
        super().__init__(
            db_url=db_url,
            node_type=NodeType.SCHEDULER,
            node_heartbeat_interval=node_heartbeat_interval,
            node_timeout_sec=node_timeout_sec,
            logger_name=self.LOGGER_NAME,
            logging_level=logging_level,
            **kwargs,
        )

    def _on_started(self):
        # Start heartbeat thread
        heartbeat_task = NodeHeartbeatTask(
            self._node_heartbeat_interval,
            None,
            self._node_id,
            self._node_store,
            self._logger,
        )
        self._heartbeat = Heartbeat(
            f"scheduler_heartbeat_{self._node_id}", heartbeat_task, self._logger
        )
        self._heartbeat.start()

        # Log Node started message
        self._logger.debug("Node started")

    def _on_stop(self):
        # Stop heartbeat thread
        self._stop_node_heartbeat()

    def _should_set_progress_manually(self):
        return False

    def schedule(self, name: str, input: Any = None) -> int:
        """Create and store a new task.

        Args:
          name (str): Name of the registered task.
          input (Any, optional): Input payload for the task. Defaults to None.

        Returns:
          int: Identifier of the created task.
        """
        self._logger.debug(f"Create task (name: '{name}', input: {input})")
        return self._task_store.create_task(name, input)

    def has_unfinished_tasks(self):
        """Check whether there are unfinished tasks in the system.

        Returns:
          bool: True if unfinished tasks exist, otherwise False.
        """
        return self._task_store.has_unfinished_tasks()
