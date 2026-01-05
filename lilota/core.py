from typing import Callable, Type, Optional, Dict, Any
from multiprocessing import cpu_count
from lilota.models import TaskProgress, RegisteredTask
from lilota.runner import TaskRunner
from lilota.db.alembic import upgrade_db


class Lilota:

  def __init__(self, db_url: str, number_of_processes = cpu_count(), set_progress_manually: bool = False):
    self._registry: Dict[str, RegisteredTask] = {}

    # Upgrade the database
    upgrade_db(db_url)

    # Initialize the task runner
    self._runner = TaskRunner(db_url, number_of_processes=number_of_processes, set_progress_manually=set_progress_manually)


  def _register(
    self,
    name: str,
    func: Callable,
    *,
    input_model: Optional[Type[Any]] = None,
    output_model: Optional[Type[Any]] = None,
    task_progress: Optional[TaskProgress] = None
  ):
    if name in self._registry:
      raise RuntimeError(f"Task {name!r} is already registered")
    
    if not task_progress is None and not isinstance(task_progress, type(TaskProgress)):
      raise TypeError("task_progress must be of type TaskProgress")

    # Register the task
    task = RegisteredTask(
      func=func,
      input_model=input_model,
      output_model=output_model,
      task_progress=task_progress
    )

    self._runner.register(name, task)
    self._registry[name] = task


  def register(
    self,
    name: str,
    *,
    input_model=None,
    output_model=None,
    task_progress=None
  ):
    def decorator(func):
      self._register(
        name=name,
        func=func,
        input_model=input_model,
        output_model=output_model,
        task_progress=task_progress
      )
      return func
    return decorator
  

  def start(self):
    self._runner.start()


  def schedule(self, name: str, input_model: Any = None) -> int:
    return self._runner.add(name, input_model)


  def stop(self):
    self._runner.stop()


  def get_all_tasks(self):
    return self._runner._store.get_all_tasks()
  

  def get_task_by_id(self, id: int):
    return self._runner._store.get_task_by_id(id)
  

  def delete_task_by_id(self, id: int):
    return self._runner._store.delete_task_by_id(id)
  

  def _get_input(self, raw_input, input_model) -> Any:
    if input_model:
      if isinstance(raw_input, input_model):
        # Return raw input
        return raw_input
      else:
        # Validate input
        return input_model.model_validate(raw_input)
    else:
      return raw_input
  

  def run(self, name: str, raw_input: Any) -> Any:
    if name not in self._registry:
      raise RuntimeError(f"Unknown task {name!r}")
    entry = self._registry[name]
    func = entry["func"]
    return func(raw_input)