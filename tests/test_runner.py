from unittest import TestCase
from multiprocessing import cpu_count
from lilota.runner import TaskRunner
from lilota.models import TaskBase, TaskInfo
from lilota.stores import MemoryTaskStore, StoreManager
import logging
import time


class AdditionTask(TaskBase):
  
  def run(self):
    output = {
      "result": self.task_info.input["number1"] + self.task_info.input["number2"]
    }
    self.set_output(self.task_info.id, output)


class ExceptionTask(TaskBase):

  def run(self):
    raise Exception("Boom")


class TaskRunnerTestCase(TestCase):

  @classmethod
  def setUpTestData(cls):
    pass


  def setUp(self):
    pass


  def test_register___nothing_is_registered___should_not_have_any_registration(self):
    # Arrange & Act
    store, store_manager, runner = self.create_task_runner(1)

    # Assert
    self.assertEqual(len(runner._registrations), 0)


  def test_register___one_class_is_registered___should_have_one_registration(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(1)

    # Act
    runner.register("add_task", AdditionTask)

    # Assert
    self.assertEqual(len(runner._registrations), 1)


  def test_start___number_of_processes_is_not_set___should_use_cpu_count(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner()

    # Act
    runner.start()

    # Assert
    self.assertEqual(runner._number_of_processes, cpu_count())


  def test_start___number_of_processes_is_set_to_one___should_use_one(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(number_of_processes=1)

    # Act
    runner.start()

    # Assert
    self.assertEqual(runner._number_of_processes, 1)


  def test_start___number_of_processes_is_greater_than_cpu_count___should_use_cpu_count(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(number_of_processes=1000)

    # Act
    runner.start()

    # Assert
    self.assertEqual(runner._number_of_processes, cpu_count())


  def test_start___but_start_twice___should_raise_exception(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(1)
    runner.start()

    # Act & Assert
    with self.assertRaises(Exception) as context:
      runner.start()
    self.assertEqual(str(context.exception), "The task runner is already started")


  def test_stop___but_start_was_not_executed___should_raise_exception(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(1)

    # Act & Assert
    with self.assertRaises(Exception) as context:
      runner.stop()
    self.assertEqual(str(context.exception), "The task runner cannot be stopped because it was not started")


  def test_stop___start_and_directly_stop___should_shutdown_all_processes(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner()
    runner.start()
    self.assertTrue(runner._is_started)
    self.assertEqual(len(runner._processes), cpu_count())
    self.assertIsNotNone(runner._logging_thread)
    self.assertIsNotNone(runner._store)

    # Act
    runner.stop()

    # Assert
    self.assertFalse(runner._is_started)
    self.assertEqual(len(runner._processes), 0)
    self.assertIsNone(runner._logging_thread)
    self.assertIsNotNone(runner._store)


  def test_add___but_task_runner_is_not_started___should_raise_exception(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(1)

    # Act & Assert
    with self.assertRaises(Exception) as context:
      runner.add("add_task", "Add two numbers", {"number1": 1, "number2": 2})
    self.assertEqual(str(context.exception), "The task runner must be started first")


  def test_add___add_1_task___should_calculate_the_result(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(1)
    runner.register("add_task", AdditionTask)
    runner.start()

    # Act
    runner.add("add_task", "Add two numbers", {"number1": 1, "number2": 2})

    # Assert
    runner.stop()
    tasks = store.get_all_tasks()
    self.assertIsNotNone(tasks)
    self.assertEqual(len(tasks), 1)
    self.assertEqual(tasks[0].progress_percentage, 100)
    store_manager.shutdown()


  def test_add___add_5000_tasks___should_calculate_the_results(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(1)
    runner.register("add_task", AdditionTask)
    runner.start()

    # Act
    for i in range(1, 5001):
      runner.add("add_task", "Add two numbers", {"number1": i, "number2": i})

    # Assert
    runner.stop()
    tasks = store.get_all_tasks()
    self.assertIsNotNone(tasks)
    self.assertEqual(len(tasks), 5000)

    for task in tasks:
      number1 = task.input["number1"]
      number2 = task.input["number2"]
      result = task.output["result"]
      self.assertEqual(number1 + number2, result)
    
    store_manager.shutdown()


  def test_logging___exception_task___should_raise_exception(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(1)
    runner.register("exception_task", ExceptionTask)
    runner.start()

    # Act
    runner.add("exception_task", "Raise exception", None)

    # Assert
    runner.stop()
    tasks = store.get_all_tasks()
    self.assertIsNotNone(tasks)
    store_manager.shutdown()


  def create_task_runner(self, number_of_processes: int = cpu_count()):
    StoreManager.register("Store", MemoryTaskStore)
    store_manager = StoreManager()
    store_manager.start()
    store = store_manager.Store()
    runner = TaskRunner(store, number_of_processes, logging_level=logging.INFO)
    return (store, store_manager, runner)