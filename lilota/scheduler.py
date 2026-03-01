from typing import Any
from lilota.node import LilotaNode, NodeHeartbeatTask
from lilota.models import NodeType
from lilota.heartbeat import Heartbeat
import logging



class LilotaScheduler(LilotaNode):

  LOGGER_NAME = "lilota.scheduler"

  def __init__(self, db_url: str, node_heartbeat_interval: float = 5.0, node_timeout_sec: int = 20, logging_level=logging.INFO, **kwargs):
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
      self._node_id, 
      self._node_store, 
      self._logger
    )
    self._heartbeat = Heartbeat(f"scheduler_heartbeat_{self._node_id}", heartbeat_task, self._logger)
    self._heartbeat.start()

    # Log Node started message
    self._logger.debug("Node started")


  def _on_stop(self):
    # Stop heartbeat thread
    self._stop_node_heartbeat()


  def _should_set_progress_manually(self):
    return False


  def schedule(self, name: str, input: Any = None) -> int:
    # Save the task infos in the store
    self._logger.debug(f"Create task (name: '{name}', input: {input})")
    return self._task_store.create_task(name, input)
