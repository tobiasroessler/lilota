from lilota.models import TaskProgress
from lilota.scheduler import LilotaScheduler
from lilota.worker import LilotaWorker
import logging
from typing import Callable, Type, Optional, Any
from uuid import UUID


class Lilota():

  def __init__(self, db_url: str, node_heartbeat_interval: float = 5, node_timeout_sec: int = 20, logging_level = logging.INFO, task_heartbeat_interval: float = 0.1, max_task_heartbeat_interval: float = 5.0, set_progress_manually: bool = False):
    self._db_url = db_url
    self._scheduler = LilotaScheduler(
      db_url=db_url,
      node_heartbeat_interval=node_heartbeat_interval,
      node_timeout_sec=node_timeout_sec,
      logging_level=logging_level
    )
    self._worker = LilotaWorker(
      db_url = db_url,
      run_in_thread=True,
      node_heartbeat_interval=node_heartbeat_interval,
      node_timeout_sec=node_timeout_sec,
      task_heartbeat_interval=task_heartbeat_interval,
      max_task_heartbeat_interval=max_task_heartbeat_interval,
      set_progress_manually=set_progress_manually,
      logging_level=logging_level
    )
    self._is_started = False


  def start(self):
    self._scheduler.start()
    self._worker.start()
    self._is_started = self._scheduler._is_started and self._worker._is_started


  def stop(self):
    self._scheduler.stop()
    self._worker.stop()
    self._is_started = self._scheduler._is_started and self._worker._is_started


  def schedule(self, name: str, input: Any = None) -> int:
    return self._scheduler.schedule(name, input)
  

  def get_task_by_id(self, id: UUID):
    return self._scheduler._task_store.get_task_by_id(id)
  

  def _register(
    self,
    name: str,
    func: Callable,
    *,
    input_model: Optional[Type[Any]] = None,
    output_model: Optional[Type[Any]] = None,
    task_progress: Optional[TaskProgress] = None
  ):
    self._worker._register(
      name=name, 
      func=func, 
      input_model=input_model, 
      output_model=output_model, 
      task_progress=task_progress
    )