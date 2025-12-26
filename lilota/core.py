from typing import Callable, Type, Optional, Dict, Any
from multiprocessing import cpu_count
from lilota.runner import TaskRunner
from lilota.db.alembic import upgrade_db
from dataclasses import is_dataclass, asdict
  

class RegisteredTask:

  def __init__(self, func: Callable, input_model: Optional[Type], output_model: Optional[Type]):
    self.func = func
    self.input_model = input_model
    self.output_model = output_model


  def __call__(self, raw_input: Any):
    # Deserialize input
    input_value = self._deserialize_input(raw_input)

    # Execute the function
    result = self.func(input_value)

    # Serialize output to JSON-safe dict
    return self._serialize_output(result)


  def _deserialize_input(self, raw_input: Any):
    if not self.input_model:
      return raw_input
    if isinstance(raw_input, self.input_model):
      return raw_input
    if hasattr(self.input_model, "model_validate"):  # Pydantic
      return self.input_model.model_validate(raw_input)
    if is_dataclass(self.input_model):
      return self.input_model(**raw_input)
    raise TypeError(f"Unsupported input_model type: {self.input_model}")


  def _serialize_output(self, output: Any):
    if not self.output_model:
      return output
    if isinstance(output, self.output_model):
      if hasattr(output, "model_dump"):  # Pydantic
        return output.model_dump()
      if is_dataclass(output):
        return asdict(output)
    if hasattr(self.output_model, "model_validate"):  # Pydantic
      return self.output_model.model_validate(output).model_dump()
    if is_dataclass(self.output_model):
      return self.output_model(**output)
    return output



class Lilota:

  def __init__(self, name: str, db_url: str, number_of_processes = cpu_count()):
    self._name = name
    self._registry: Dict[str, RegisteredTask] = {}

    # Upgrade the database
    upgrade_db(db_url)

    # Initialize the task runner
    self._runner = TaskRunner(db_url, number_of_processes=number_of_processes)


  def _register(
    self,
    name: str,
    func: Callable,
    *,
    input_model: Optional[Type[Any]] = None,
    output_model: Optional[Type[Any]] = None,
  ):
    if name in self._registry:
      raise RuntimeError(f"Task {name!r} is already registered")

    # Register the task
    task = RegisteredTask(
      func=func,
      input_model=input_model,
      output_model=output_model,
    )

    self._runner.register(name, task)
    self._registry[name] = task


  def register(
    self,
    name: str,
    *,
    input_model=None,
    output_model=None,
  ):
    def decorator(func):
      self._register(
        name=name,
        func=func,
        input_model=input_model,
        output_model=output_model,
      )
      return func
    return decorator
  

  def start(self):
    self._runner.start()


  def schedule(self, name: str, input_model: Any) -> int:
    return self._runner.add(name, input_model)


  def stop(self):
    self._runner.stop()


  def get_all_tasks(self):
    return self._runner._store.get_all_tasks()
  

  def get_task_by_id(self, id: int):
    return self._runner._store.get_task_by_id(id)
  

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