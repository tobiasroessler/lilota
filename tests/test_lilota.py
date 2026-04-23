import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from alembic import command
from dataclasses import dataclass
import logging
from unittest import TestCase, main
from typing import Any
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.core import Lilota, ManagedProcess
from lilota.models import Node, NodeLeader, Task, TaskStatus, LogEntry, NodeType
from lilota.db.alembic import get_alembic_config
from lilota.stores import SqlAlchemyLogStore
from lilota.worker import LilotaWorker
import time
from uuid import UUID


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


  def setUp(self):
    # Create SqlAlchemy engine and session
    with LilotaTestCase.get_session() as session:
      session.query(Task).delete()
      session.query(Node).delete()
      session.query(LogEntry).delete()
      session.query(NodeLeader).delete()
      session.commit()


  def test_stop___start_and_directly_stop___should_shutdown_all_processes(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=8
    )

    try:
      lilota.start()
      self.assertTrue(lilota._is_started)
      self.assertEqual(len(lilota._process_manager._processes), 8)
      for mp in lilota._process_manager._processes:
        self.assertIsNone(mp.proc.poll())
    except Exception:
      lilota.stop()

    # Act
    lilota.stop()

    # Assert
    self.assertFalse(lilota._is_started)
    self.assertEqual(len(lilota._process_manager._processes), 0)


  def test_schedule___add_1_hello_world_task___should_execute_task(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/hello_world_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:
      id = lilota.schedule("hello_world")
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep(seconds=1)
      task = lilota.get_task_by_id(id)
      self.assertEqual(task.status, TaskStatus.COMPLETED)
      self.assertIsNone(task.error)
      self.assertEqual(task.progress_percentage, 100)
      self.assertIsNone(task.input)
      self.assertIsNone(task.output)
    finally:
      lilota.stop()


  def test_schedule___add_1_task_with_only_input_model___should_execute_task(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:  
      id = lilota.schedule("only_input_model", AddInput(a=1, b=2))
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep()
      task = lilota.get_task_by_id(id)
      self.assertEqual(task.status, TaskStatus.COMPLETED)
      self.assertIsNone(task.error)
      self.assertEqual(task.progress_percentage, 100)
      self.assertIsNotNone(task.input)
      self.assertIsNone(task.output)
    finally:
      lilota.stop()


  def test_schedule___add_1_task_with_only_output_model___should_execute_task(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:
      id = lilota.schedule("only_output_model")
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep(seconds=1)
      task = lilota.get_task_by_id(id)
      self.assertEqual(task.status, TaskStatus.COMPLETED)
      self.assertIsNone(task.error)
      self.assertEqual(task.progress_percentage, 100)
      self.assertIsNone(task.input)
      self.assertIsNotNone(task.output)
      result = task.output['sum']
      self.assertEqual(3, result)
    finally:
      lilota.stop()


  def test_schedule___add_1_task_with_only_taskprogress___should_execute_task(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/taskprogress_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:
      id = lilota.schedule("only_taskprogress")
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep(3)
      task: Task = lilota.get_task_by_id(id)
      self.assertEqual(task.progress_percentage, 50)
    finally:
      lilota.stop()


  def test_schedule___add_1_task_using_input_and_output_model___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:
      id = lilota.schedule("add", AddInput(a=1, b=2))
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep()
      task = lilota.get_task_by_id(id)
      self.assertEqual(task.status, TaskStatus.COMPLETED)
      self.assertIsNone(task.error)
      self.assertEqual(task.progress_percentage, 100)
      number1 = task.input['a']
      number2 = task.input['b']
      result = task.output['sum']
      self.assertEqual(number1 + number2, result)
    finally:
      lilota.stop()


  def test_schedule___with_taskprogress_object_passed___should_have_registered_task(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/taskprogress_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:
      id = lilota.schedule("add_with_taskprogress", AddInputDataclass(a=1, b=2))
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep()
      task: Task = lilota.get_task_by_id(id)
      self.assertEqual(task.progress_percentage, 50)
    finally:
      lilota.stop()


  def test_schedule___add_1_task_using_dataclasses___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:
      id = lilota.schedule("add_with_dataclasses", AddInputDataclass(a=1, b=2))
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep()
      task = lilota.get_task_by_id(id)
      self.assertEqual(task.status, TaskStatus.COMPLETED)
      self.assertIsNone(task.error)
      self.assertEqual(task.progress_percentage, 100)
      number1 = task.input['a']
      number2 = task.input['b']
      result = task.output['sum']
      self.assertEqual(number1 + number2, result)
    finally:
      lilota.stop()


  def test_schedule___add_1_task_using_dict___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:
      id = lilota.schedule("add_with_dict", { "a": 1, "b": 2 })
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep(seconds=1)
      task = lilota.get_task_by_id(id)
      self.assertEqual(task.status, TaskStatus.COMPLETED)
      self.assertIsNone(task.error)
      self.assertEqual(task.progress_percentage, 100)
      number1 = task.input['a']
      number2 = task.input['b']
      result = task.output['sum']
      self.assertEqual(number1 + number2, result)
    finally:
      lilota.stop()


  def test_schedule___add_1_task_using_invalid_model___should_calculate_the_result(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=1
    )
    lilota.start()
    id = None

    # Act & Assert
    try:
      id = lilota.schedule("add_with_invalid_model", AddInputInvalid(a=1, b=2))
      self.fail("A TypeError was expected")
    except TypeError as ex:
      self.assertEqual(str(ex), "Unsupported type: AddInputInvalid. Expected ModelProtocol, dataclass, or dict.")
      self.assertIsNone(id)
    finally:
      lilota.stop()


  def test_schedule___add_500_tasks___should_calculate_the_results(self):
    # Arrange
    ids = []
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py"
    )
    lilota.start()

    # Act
    try:
      for i in range(1, 501):
        id = lilota.schedule("add", AddInput(a=i, b=i))
        ids.append(id)
    except:
      lilota.stop()

    # Assert
    try:
      self.wait(lilota)
      self.assertEqual(len(ids), 500)

      index = 0
      for id in ids:
        index = index + 1
        task = lilota.get_task_by_id(id)
        number1 = task.input['a']
        number2 = task.input['b']
        if task.output is None:
          self.fail()
        result = task.output['sum']
        self.assertEqual(number1 + number2, result)
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertIsNone(task.error)
    finally:
      lilota.stop()


  def test_schedule___start_lilota_twice_and_add_5_tasks___should_calculate_the_results(self):
    # Arrange
    ids = []
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=1
    )
    lilota.start()
    lilota.stop()
    lilota.start()

    # Act
    try:
      for i in range(1, 6):
        id = lilota.schedule("add", AddInput(a=i, b=i))
        ids.append(id)
    except:
      lilota.stop()

    # Assert
    try:
      self.wait(lilota)
      self.assertEqual(len(ids), 5)

      index = 0
      for id in ids:
        index = index + 1
        task = lilota.get_task_by_id(id)
        number1 = task.input['a']
        number2 = task.input['b']
        if task.output is None:
          self.fail()
        result = task.output['sum']
        self.assertEqual(number1 + number2, result)
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertIsNone(task.error)
    finally:
      lilota.stop()


  def test_schedule___add_1_task_but_function_raises_exception___should_log_exception(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=1
    )
    lilota.start()

    # Act
    try:
      id = lilota.schedule("add_with_exception", AddInput(a=1, b=2))
    except:
      lilota.stop()

    # Assert
    try:
      self.sleep(3)
      task = lilota.get_task_by_id(id)
      self.assertEqual(task.status, TaskStatus.FAILED)
      self.assertIsNotNone(task.error)
      self.assertEqual(task.error["type"], "Exception")
      self.assertEqual(task.error["message"], "Error")
      self.assertEqual(task.progress_percentage, 100)
      self.assertIsNone(task.output)
    finally:
      lilota.stop()


  def test_logging___when_starting_lilota_with_two_workers___should_log_correctly(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/lilota_test_script.py",
      number_of_workers=2,
      logging_level=logging.DEBUG
    )
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaTestCase.DB_URL)

    # Act
    try:
      lilota.start()
    except:
      lilota.stop()

    # Assert
    leadership_acquired = False

    try:
      self.sleep(seconds=1)
      scheduler_node = lilota._scheduler.get_node()
      worker_nodes = [n for n in lilota.get_all_nodes() if n.type == NodeType.WORKER]
      worker_node_1: Node = worker_nodes[0]
      worker_node_2: Node = worker_nodes[1]

      log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(scheduler_node.id)
      self.assertEqual(len(log_entries), 1)
      self.assertEqual(log_entries[0].message, "Node started")

      log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(worker_node_1.id)
      self.assertEqual("Node started", log_entries[0].message)
      if len(log_entries) == 2:
        self.assertEqual(f"Leadership acquired first time (node id: {worker_node_1.id})", log_entries[1].message)
        leadership_acquired = True

      log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(worker_node_2.id)
      self.assertEqual("Node started", log_entries[0].message)
      if len(log_entries) == 2:
        self.assertEqual(f"Leadership acquired first time (node id: {worker_node_2.id})", log_entries[1].message)
        leadership_acquired = True

      self.assertTrue(leadership_acquired)
    finally:
      lilota.stop()


  def test_timeout___when_having_an_infinite_loop___should_stop_worker(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/infinite_loop_test_script.py",
      number_of_workers=1,
      logging_level=logging.DEBUG
    )
    log_store: SqlAlchemyLogStore = SqlAlchemyLogStore(LilotaTestCase.DB_URL)
    lilota.start()
    process: ManagedProcess = None
    process_id: UUID = None

    # Act
    try:
      process = lilota._process_manager._processes[0]
      process_id = process.id
      self.assertIsNotNone(process)
      self.assertIsNone(process.proc.poll())
      task_id = lilota.schedule("infinite_loop")
    except:
      lilota.stop()

    # Assert (debugging not possible)
    self.sleep(3)
    
    try:
      new_process = lilota._process_manager._processes[0]
      self.assertNotEqual(new_process.id, process_id)
      self.assertIsNone(new_process.proc.poll())
      worker: LilotaWorker = [n for n in lilota.get_all_nodes() if n.type == NodeType.WORKER][0]
      self.assertIsNotNone(worker)
      log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(worker.id)
      self.assertEqual(len(log_entries), 1)
      self.assertEqual(log_entries[0].message, f"The process will be stopped because the task 'infinite_loop' ({task_id}) has expired")
      self.assertEqual(log_entries[0].level, "ERROR")
    finally:
      lilota.stop()


  def test_exception_in_script___when_directly_failing___should_stop_worker(self):
    # Arrange
    lilota = Lilota(
      db_url=LilotaTestCase.DB_URL,
      script_path="/Users/torox/Sandbox/Python/lilota/lilota/tests/scripts/exception_test_script.py",
      number_of_workers=1,
      logging_level=logging.DEBUG
    )
    self.assertIsNone(lilota.error)
    
    # Act
    lilota.start()
    lilota.wait_for_failure()

    # Assert
    try:
      self.assertTrue(lilota.has_failed())
      self.assertIsNotNone(lilota.error)
      self.assertEqual(len(lilota._process_manager._processes), 0)
    finally:
      lilota.stop()


  def sleep(self, seconds: float = 0.3):
    time.sleep(seconds)


  def wait(self, lilota: Lilota) -> None:
    max = 60
    index = 0

    while True:
      has_unfinished_tasks = lilota._scheduler.has_unfinished_tasks()
      if has_unfinished_tasks:
        self.sleep(1)
        index = index + 1
        if index == max:
          raise Exception("Should finish earlier")
      else:
        break

      if lilota.has_failed():
        raise Exception(lilota.error)



if __name__ == '__main__':
  main()