import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from alembic import command
from dataclasses import dataclass
from unittest import TestCase, main
from typing import Any
from multiprocessing import cpu_count
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.node import Lilota
from lilota.scheduler import LilotaScheduler
from lilota.worker import LilotaWorker
from lilota.models import Node, Task, TaskStatus, TaskProgress
from lilota.db.alembic import get_alembic_config
import time


class AddInput():
  def __init__(self, a: int, b: int) -> None:
    self.a = a
    self.b = b

  def as_dict(self) -> dict[str, Any]:
    return {
      "a": self.a,
      "b": self.b,
    }


class AddOutput():
  def __init__(self, sum: int) -> None:
    self.sum = sum

  def as_dict(self) -> dict[str, Any]:
    return {
      "sum": self.sum
    }


@dataclass
class AddInputDataclass():
    a: int
    b: int


@dataclass
class AddOutputDataclass():
  sum: int


class AddInputInvalid():
  a: int
  b: int

  def __init__(self, a: int, b: int):
    self.a = a
    self.b = b


class AddOutputInvalid():
  sum: int

  def __init__(self, sum: int):
    self.sum = sum


def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


def add_with_exception(data: AddInput) -> AddOutput:
  raise Exception("Error")


def add_with_dataclasses(data: AddInputDataclass) -> AddOutputDataclass:
  return AddOutputDataclass(sum=data.a + data.b)


def add_with_dict(data: dict[str, int]) -> dict[str, int]:
  return {
    "sum": data["a"] + data["b"]
  }


def add_with_taskprogress(data: AddInput, task_progress: TaskProgress):
  task_progress.set(50)


def hello_world():
  print("Hello Word")


def only_input_model(data: AddInput) -> None:
  print("Hello World")


def only_output_model() -> AddOutput:
  return AddOutput(sum=3)


def only_taskprogress(task_progress: TaskProgress) -> None:
  task_progress.set(50)


EXTERNAL_STATE = {"counter": 0}

def task_mutates_external_state() -> dict[str, int]:
  EXTERNAL_STATE["counter"] += 1
  return EXTERNAL_STATE



class LilotaTestCase(TestCase):

  DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test"


  @classmethod
  def get_session(cls):
    engine = create_engine(cls.DB_URL)
    Session = sessionmaker(bind=engine)
    return Session()
  

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    # Apply the migrations
    cfg = get_alembic_config(db_url=LilotaTestCase.DB_URL)
    try:
      command.upgrade(cfg, "head")
    except Exception as ex:
      raise Exception(f"Could not update the database: {str(ex)}")
    
    # Create SqlAlchemy engine and session
    with LilotaTestCase.get_session() as session:
      session.query(Task).delete()
      session.query(Node).delete()
      session.commit()


  def setUp(self):
    pass


  def test_stop___start_and_directly_stop___should_shutdown_all_processes(self):
    # Arrange
    lilota = LilotaWorker(LilotaTestCase.DB_URL)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)
    lilota.start()
    self.assertTrue(lilota._runner._is_started)
    self.assertEqual(len(lilota._runner._processes), cpu_count())
    self.assertIsNotNone(lilota._runner._logging_queue)
    self.assertIsNotNone(lilota._runner._store)

    # Act
    lilota.stop()

    # Assert
    self.assertFalse(lilota._runner._is_started)
    self.assertEqual(len(lilota._runner._processes), 0)
    self.assertIsNotNone(lilota._runner._logging_queue)
    self.assertIsNotNone(lilota._runner._store)


  def test___add_1_hello_world_task___should_execute_task(self):
    # Arrange
    worker = LilotaWorker(LilotaTestCase.DB_URL, number_of_processes=1, max_task_heartbeat_interval=0.1)
    worker._register(name="hello_world", func=hello_world)
    worker.start()
    scheduler = LilotaScheduler(LilotaTestCase.DB_URL)
    scheduler.start()

    # Act
    id = scheduler.schedule("hello_world")

    # Assert
    scheduler.stop()
    time.sleep(2)
    try:
      import signal, faulthandler
      faulthandler.dump_traceback_later(5, repeat=True)
      worker.stop()
    except Exception as ex:
      print("Exception")
    
    print("AFTER STOP")
    task = worker.get_task_by_id(id)
    self.assertEqual(task.status, TaskStatus.COMPLETED)
    self.assertIsNone(task.exception)
    self.assertEqual(task.progress_percentage, 100)
    self.assertIsNone(task.input)
    self.assertIsNone(task.output)


  def test___add_1_task_with_only_input_model___should_execute_task(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="only_input_model", func=only_input_model, input_model=AddInput)
    lilota.start()

    # Act
    id = lilota.schedule("only_input_model", AddInput(a=1, b=2))

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(id)
    self.assertEqual(task.status, TaskStatus.COMPLETED)
    self.assertIsNone(task.exception)
    self.assertEqual(task.progress_percentage, 100)
    self.assertIsNotNone(task.input)
    self.assertIsNone(task.output)


  def test___add_1_task_with_only_output_model___should_execute_task(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="only_output_model", func=only_output_model, output_model=AddOutput)
    lilota.start()

    # Act
    id = lilota.schedule("only_output_model")

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(id)
    self.assertEqual(task.status, TaskStatus.COMPLETED)
    self.assertIsNone(task.exception)
    self.assertEqual(task.progress_percentage, 100)
    self.assertIsNone(task.input)
    self.assertIsNotNone(task.output)
    result = task.output['sum']
    self.assertEqual(3, result)


  def test___add_1_task_with_only_taskprogress___should_execute_task(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1, set_progress_manually=True)
    lilota._register(name="only_taskprogress", func=only_taskprogress, task_progress=TaskProgress)
    lilota.start()

    # Act
    id = lilota.schedule("only_taskprogress")

    # Assert
    lilota.stop()
    task: Task = lilota.get_task_by_id(id)
    self.assertEqual(task.progress_percentage, 50)


  def test_schedule___add_1_task_using_model_protocol___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)
    lilota.start()

    # Act
    id = lilota.schedule("add", AddInput(a=1, b=2))

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(id)
    self.assertEqual(task.status, TaskStatus.COMPLETED)
    self.assertIsNone(task.exception)
    self.assertEqual(task.progress_percentage, 100)
    number1 = task.input['a']
    number2 = task.input['b']
    result = task.output['sum']
    self.assertEqual(number1 + number2, result)


  def test_schedule___with_taskprogress_object_passed___should_have_registered_task(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1, set_progress_manually=True)
    lilota._register(name="add_with_taskprogress", func=add_with_taskprogress, input_model=AddInput, output_model=AddOutput, task_progress=TaskProgress)
    lilota.start()

    # Act
    id = lilota.schedule("add_with_taskprogress", AddInputDataclass(a=1, b=2))

    # Assert
    lilota.stop()
    task: Task = lilota.get_task_by_id(id)
    self.assertEqual(task.progress_percentage, 50)


  def test_schedule___add_1_task_using_dataclasses___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add_with_dataclasses", func=add_with_dataclasses, input_model=AddInputDataclass, output_model=AddOutputDataclass)
    lilota.start()

    # Act
    id = lilota.schedule("add_with_dataclasses", AddInputDataclass(a=1, b=2))

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(id)
    self.assertEqual(task.status, TaskStatus.COMPLETED)
    self.assertIsNone(task.exception)
    self.assertEqual(task.progress_percentage, 100)
    number1 = task.input['a']
    number2 = task.input['b']
    result = task.output['sum']
    self.assertEqual(number1 + number2, result)


  def test_schedule___add_1_task_using_dict___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add_with_dict", func=add_with_dict, input_model=dict[str, int], output_model=dict[str, int])
    lilota.start()

    # Act
    id = lilota.schedule("add_with_dict", { "a": 1, "b": 2 })

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(id)
    self.assertEqual(task.status, TaskStatus.COMPLETED)
    self.assertIsNone(task.exception)
    self.assertEqual(task.progress_percentage, 100)
    number1 = task.input['a']
    number2 = task.input['b']
    result = task.output['sum']
    self.assertEqual(number1 + number2, result)


  def test_schedule___add_1_task_using_invalid_model___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add_with_invalid_model", func=add_with_dataclasses, input_model=AddInputInvalid, output_model=AddOutputInvalid)
    lilota.start()
    id = None

    # Act & Assert
    try:
      id = lilota.schedule("add_with_invalid_model", AddInputInvalid(a=1, b=2))
    except TypeError as ex:
      self.assertEqual(str(ex), "Unsupported type: AddInputInvalid. Expected ModelProtocol, dataclass, or dict.")
      self.assertIsNone(id)
    finally:
      lilota.stop()


  def test_schedule___add_1000_tasks_using_one_process___should_calculate_the_results(self):
    # Arrange
    ids = []
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)  
    lilota.start()

    # Act
    for i in range(1, 1001):
      id = lilota.schedule("add", AddInput(a=i, b=i))
      ids.append(id)

    # Assert
    lilota.stop()
    self.assertEqual(len(ids), 1000)

    for id in ids:
      task = lilota.get_task_by_id(id)
      number1 = task.input['a']
      number2 = task.input['b']
      result = task.output['sum']
      self.assertEqual(number1 + number2, result)
      self.assertEqual(task.status, TaskStatus.COMPLETED)
      self.assertIsNone(task.exception)


  def test_schedule___add_5000_tasks_using_multiple_processes___should_calculate_the_results(self):
    # Arrange
    ids = []
    lilota = Lilota(LilotaTestCase.DB_URL)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)  
    lilota.start()

    # Act
    for i in range(1, 5001):
      id = lilota.schedule("add", AddInput(a=i, b=i))
      ids.append(id)

    # Assert
    lilota.stop()
    self.assertEqual(len(ids), 5000)

    for id in ids:
      task = lilota.get_task_by_id(id)
      number1 = task.input['a']
      number2 = task.input['b']
      result = task.output['sum']
      self.assertEqual(number1 + number2, result)
      self.assertEqual(task.status, TaskStatus.COMPLETED)
      self.assertIsNone(task.exception)


  def test_schedule___add_1_task_but_function_raises_exception___should_log_exception(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add_with_exception", func=add_with_exception, input_model=AddInput, output_model=AddOutput)
    lilota.start()

    # Act
    id = lilota.schedule("add_with_exception", AddInput(a=1, b=2))

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(id)
    self.assertEqual(task.status, TaskStatus.FAILED)
    self.assertIsNotNone(task.exception)
    self.assertEqual(task.exception["type"], "Exception")
    self.assertEqual(task.exception["message"], "Error")
    self.assertEqual(task.progress_percentage, 100)
    self.assertIsNone(task.output)


  def test_schedule___delete_task_by_id___should_delete_task(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add_with_exception", func=add_with_exception, input_model=AddInput, output_model=AddOutput)
    lilota.start()

    # Act
    id = lilota.schedule("add_with_exception", AddInput(a=1, b=2))

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(id)
    self.assertIsNotNone(task)
    deleted = lilota.delete_task_by_id(id)
    self.assertTrue(deleted)
    deleted = lilota.delete_task_by_id(id)
    self.assertFalse(deleted)


  def test_schedule___task_uses_external_mutable_state___state_is_not_shared(self):
    """
    External mutable state is copied into worker processes.
    Mutations inside tasks do NOT affect parent process state.
    """

    # Arrange
    self.assertEqual(EXTERNAL_STATE["counter"], 0)

    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(
      name="task_mutates_external_state",
      func=task_mutates_external_state,
      output_model=dict[str, int]
    )
    lilota.start()

    # Act
    task_id = lilota.schedule("task_mutates_external_state")

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(task_id)
    self.assertEqual(task.status, TaskStatus.COMPLETED)
    self.assertEqual(task.output["counter"], 1)
    # 🔑 Critical assertion:
    # Parent process state was NOT modified
    self.assertEqual(EXTERNAL_STATE["counter"], 0)


  def test_start___with_unfinished_task___should_schedule_the_task_again(self):
    # Arrange
    engine = create_engine(LilotaTestCase.DB_URL)
    Session = sessionmaker(bind=engine)
    with Session() as session:
      task = Task(
        name="add",
        status=TaskStatus.RUNNING,
        input={"a": 4, "b": 5},
        output=None,
        exception=None,
        progress_percentage=50
      )
      session.add(task)
      session.commit()
      task_id = task.id
    engine.dispose()

    lilota = Lilota(LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)
    task = lilota.get_task_by_id(task_id)
    self.assertEqual(task.status, TaskStatus.RUNNING)
    self.assertEqual(task.progress_percentage, 50)

    # Act
    lilota.start()

    # Assert
    lilota.stop()
    task = lilota.get_task_by_id(task_id)
    self.assertEqual(task.status, TaskStatus.COMPLETED)
    self.assertIsNone(task.exception)
    self.assertEqual(task.progress_percentage, 100)
    number1 = task.input['a']
    number2 = task.input['b']
    result = task.output['sum']
    self.assertEqual(number1 + number2, result)
    lilota.delete_task_by_id(task_id)


if __name__ == '__main__':
  main()