from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
import threading
import logging
from .stores import SqlAlchemyNodeStore, SqlAlchemyNodeLeaderStore


class HeartbeatThreadBase(threading.Thread, ABC):

  def __init__(
    self,
    node_id: str,
    logger: logging.Logger,
    interval_seconds: int,
    node_timeout_sec: int,
    node_store: SqlAlchemyNodeStore,
    name: str
  ):
    super().__init__(name=name, daemon=True)
    self._node_id = node_id
    self._interval_seconds = interval_seconds
    self._node_timeout_sec = node_timeout_sec
    self._stop_event = threading.Event()
    self._logger = logger
    self._node_store: SqlAlchemyNodeStore = node_store


  def run(self) -> None:
    while not self._stop_event.is_set():
      # Update own heartbeat
      try:
        self._node_store.update_node_last_seen_at(self._node_id)
      except Exception as ex:
        self._logger.exception(f"Heartbeat update failed for node_id '{self._node_id}': {str(ex)}")

      # Try to set leader and the leader should trigger a cleanup
      try:
        self.try_set_leader_and_cleanup()
      except Exception as ex:
        self._logger.exception("Leader election failed")

      # Wait
      self._stop_event.wait(self._interval_seconds)


  def stop(self) -> None:
    self._stop_event.set()


  @abstractmethod
  def try_set_leader_and_cleanup(self):
    pass



class SchedulerHeartbeatThread(HeartbeatThreadBase):
    
  def __init__(
    self,
    node_id: str,
    logger: logging.Logger,
    interval_seconds: int,
    node_timeout_sec: int,
    node_store: SqlAlchemyNodeStore
  ):
    super().__init__(
      node_id,
      logger, 
      interval_seconds,
      node_timeout_sec,
      node_store,
      f"scheduler_heartbeat_{node_id}"
    )


  def try_set_leader_and_cleanup(self):
    pass



class WorkerHeartbeatThread(HeartbeatThreadBase):
    
  def __init__(
    self,
    node_id: str,
    logger: logging.Logger,
    interval_seconds: int,
    node_timeout_sec: int,
    node_store: SqlAlchemyNodeStore,
    node_leader_store: SqlAlchemyNodeLeaderStore
  ):
    super().__init__(
      node_id, 
      logger, 
      interval_seconds,
      node_timeout_sec,
      node_store, 
      f"worker_heartbeat_{node_id}"
    )

    self._node_leader_store: SqlAlchemyNodeLeaderStore = node_leader_store
    self._is_leader = False


  def try_set_leader_and_cleanup(self):
    # Try to renew leadership
    if self._is_leader:
      self.is_leader = self._node_leader_store.renew_leadership(self._node_id)

      if not self._is_leader:
        self._logger.warning(f"Leadership lost")

    # Try to acquire leadership if not leader
    if not self._is_leader:
      self._is_leader = self._node_leader_store.try_acquire_leadership(self._node_id)

    # Leader-only work
    if self._is_leader:
      self._cleanup()

  
  def _cleanup(self) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._node_timeout_sec)

    try:
      cleaned = self._node_store.update_nodes_status_on_dead_nodes(cutoff, self._node_id)
      if cleaned > 0:
        self._logger.info(f"Marked {cleaned} stale node(s) as DEAD")
    except Exception:
      # Never let cleanup kill the heartbeat thread
      self._logger.exception("Node cleanup failed")