import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from alembic import command
from dataclasses import dataclass
from unittest import TestCase, main
from multiprocessing import cpu_count
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.core import Lilota
from lilota.models import Task, TaskStatus, TaskProgress
from lilota.db.alembic import get_alembic_config


class AddInput(BaseModel):
    a: int
    b: int


class AddOutput(BaseModel):
  sum: int


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


def add_with_taskprogress(data: AddInput, task_progress: TaskProgress):
  task_progress.set(50)


class LilotaTestCase(TestCase):

  NAME = "My Server"
  DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test"

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    # Apply the migrations
    cfg = get_alembic_config(db_url=LilotaTestCase.DB_URL)
    try:
      command.upgrade(cfg, "head")
    except Exception as ex:
      raise Exception(f"Could not update the database: {str(ex)}")
    
    # Create SQLAlchemy engine and session
    engine = create_engine(cls.DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Delete all tasks
    session.query(Task).delete()
    session.commit()
    session.close()


  def setUp(self):
    pass


  def test_register___nothing_is_registered___should_not_have_any_registration(self):
    # Arrange & Act
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)

    # Assert
    self.assertEqual(len(lilota._runner._registrations), 0)


  def test_register___one_class_is_registered___should_have_one_registration(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)

    # Act
    lilota._register("add_task", AddInput)

    # Assert
    self.assertEqual(len(lilota._runner._registrations), 1)


  def test_register___task_is_already_registered___should_raise_exception(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register("add_task", AddInput)

    # Act & Assert
    try:
      lilota._register("add_task", AddInput)
    except RuntimeError as err:
      self.assertEqual(str(err), "Task 'add_task' is already registered")


  def test_start___number_of_processes_is_not_set___should_use_cpu_count(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)

    # Act
    lilota.start()

    # Assert
    try:
      self.assertEqual(lilota._runner._number_of_processes, cpu_count())
    finally:
      lilota.stop()


  def test_start___number_of_processes_is_set_to_one___should_use_one(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)

    # Act
    lilota.start()

    # Assert
    try:
      self.assertEqual(lilota._runner._number_of_processes, 1)
    finally:
      lilota.stop()


  def test_start___number_of_processes_is_greater_than_cpu_count___should_use_cpu_count(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1000)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)

    # Act
    lilota.start()

    # Assert
    try:
      self.assertEqual(lilota._runner._number_of_processes, cpu_count())
    finally:
      lilota.stop()


  def test_start___but_start_twice___should_raise_exception(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)
    lilota.start()

    # Act & Assert
    with self.assertRaises(Exception) as context:
      lilota.start()
    try:
      self.assertEqual(str(context.exception), "The task runner is already started")
    finally:
      lilota.stop()


  def test_stop___but_start_was_not_executed___should_raise_exception(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)

    # Act & Assert
    with self.assertRaises(Exception) as context:
      lilota.stop()
    self.assertEqual(str(context.exception), "The task runner cannot be stopped because it was not started")


  def test_stop___start_and_directly_stop___should_shutdown_all_processes(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)
    lilota.start()
    self.assertTrue(lilota._runner._is_started)
    self.assertEqual(len(lilota._runner._processes), cpu_count())
    self.assertIsNotNone(lilota._runner._logging_thread)
    self.assertIsNotNone(lilota._runner._store)

    # Act
    lilota.stop()

    # Assert
    self.assertFalse(lilota._runner._is_started)
    self.assertEqual(len(lilota._runner._processes), 0)
    self.assertIsNone(lilota._runner._logging_thread)
    self.assertIsNotNone(lilota._runner._store)


  def test_add___but_task_runner_is_not_started___should_raise_exception(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)

    # Act & Assert
    with self.assertRaises(Exception) as context:
      id = lilota.schedule("add", AddInput(a=1, b=2))
      self.assertIsNone(id)
    self.assertEqual(str(context.exception), "The task runner must be started first")


  def test_add___add_1_task_using_pydantic___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
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


  def test_add___with_taskprogress_object_passed___should_have_registered_task(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add_with_taskprogress", func=add_with_taskprogress, input_model=AddInput, output_model=AddOutput, task_progress=TaskProgress)
    lilota.start()

    # Act
    id = lilota.schedule("add_with_taskprogress", AddInputDataclass(a=1, b=2))

    # Assert
    lilota.stop()


  def test_add___add_1_task_using_dataclasses___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
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


  def test_add___add_1_task_using_invalid_model___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add_with_invalid_model", func=add_with_dataclasses, input_model=AddInputInvalid, output_model=AddOutputInvalid)
    lilota.start()
    id = None

    # Act & Assert
    try:
      id = lilota.schedule("add_with_invalid_model", AddInputInvalid(a=1, b=2))
    except TypeError as ex:
      self.assertEqual(str(ex), "Unsupported type: AddInputInvalid. Expected BaseModel, dataclass, or dict.")
      self.assertIsNone(id)
    finally:
      lilota.stop()


  def test_add___add_5000_tasks___should_calculate_the_results(self):
    # Arrange
    ids = []
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
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


  def test_add___add_1_task_but_function_raises_exception___should_log_exception(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
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


  def test_add___delete_task_by_id___should_delete_task(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
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
  

if __name__ == '__main__':
  main()