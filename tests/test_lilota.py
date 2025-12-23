import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from alembic import command
from unittest import TestCase, main
from multiprocessing import cpu_count
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.core import Lilota
from lilota.models import Task
from lilota.db.alembic import get_alembic_config


class AddInput(BaseModel):
    a: int
    b: int


class AddOutput(BaseModel):
  sum: int


def add(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


def add_with_exception(data: AddInput) -> AddOutput:
  return AddOutput(sum=data.a + data.b)


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
    lilota._register(name="add", func=add_with_exception, input_model=AddInput, output_model=AddOutput)
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
    lilota._register(name="add", func=add_with_exception, input_model=AddInput, output_model=AddOutput)

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
    lilota._register(name="add", func=add_with_exception, input_model=AddInput, output_model=AddOutput)

    # Act & Assert
    with self.assertRaises(Exception) as context:
      lilota.schedule("add", AddInput(a=1, b=2))
    self.assertEqual(str(context.exception), "The task runner must be started first")


  def test_add___add_1_task___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)
    lilota.start()

    # Act
    lilota.schedule("add", AddInput(a=1, b=2))

    # Assert
    lilota.stop()
    tasks = lilota.get_all_tasks()
    self.assertIsNotNone(tasks)
    self.assertEqual(len(tasks), 1)
    task = tasks[0]
    self.assertEqual(task.progress_percentage, 100)
    number1 = task.input['a']
    number2 = task.input['b']
    result = task.output['sum']
    self.assertEqual(number1 + number2, result)


  def test_add___add_5000_tasks___should_calculate_the_results(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add", func=add, input_model=AddInput, output_model=AddOutput)  
    lilota.start()

    # Act
    for i in range(1, 5001):
      lilota.schedule("add", AddInput(a=i, b=i))

    # Assert
    lilota.stop()
    tasks = lilota.get_all_tasks()
    self.assertIsNotNone(tasks)
    self.assertGreaterEqual(len(tasks), 5000)

    for task in tasks:
      number1 = task.input['a']
      number2 = task.input['b']
      result = task.output['sum']
      self.assertEqual(number1 + number2, result)


  def test_logging___exception_task___should_raise_exception(self):
    # Arrange
    lilota = Lilota(LilotaTestCase.NAME, LilotaTestCase.DB_URL, number_of_processes=1)
    lilota._register(name="add_with_exception", func=add_with_exception, input_model=AddInput, output_model=AddOutput)  
    lilota.start()

    # Act
    lilota.schedule("add_with_exception", None)

    # Assert
    lilota.stop()
    tasks = lilota.get_all_tasks()
    self.assertIsNotNone(tasks)
  

if __name__ == '__main__':
  main()