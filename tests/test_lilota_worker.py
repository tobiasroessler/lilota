import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dataclasses import dataclass
from unittest import TestCase, main
from unittest.mock import Mock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lilota.constants import DEFAULT_TEST_DB_URL
from lilota.worker import LilotaWorker, WorkerHeartbeatTask
from lilota.models import (
    Node,
    NodeType,
    NodeStatus,
    NodeLeader,
    Task,
    TaskStatus,
    LogEntry,
)
from lilota.db.alembic import upgrade_db
from lilota.stores import NodeLeaderStore, NodeStore, TaskStore, LogStore
import logging
import time
from datetime import datetime, timedelta, timezone
import threading
from uuid import UUID, uuid4


@dataclass
class AddInput:
    a: int
    b: int


@dataclass
class AddOutput:
    sum: int


def add(data: AddInput) -> AddOutput:
    return AddOutput(sum=data.a + data.b)


def infinite_loop() -> None:
    while True:
        pass


class LilotaWorkerTestCase(TestCase):
    DB_URL = DEFAULT_TEST_DB_URL

    @classmethod
    def get_session(cls):
        engine = create_engine(cls.DB_URL)
        Session = sessionmaker(bind=engine)
        return Session()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Apply the migrations
        upgrade_db(LilotaWorkerTestCase.DB_URL)

    def setUp(self):
        # Create SqlAlchemy engine and session
        with LilotaWorkerTestCase.get_session() as session:
            # Delete all tasks and nodes
            session.query(Task).delete()
            session.query(Node).delete()
            session.query(LogEntry).delete()
            session.query(NodeLeader).delete()
            session.commit()

    def test_init___invalid_interval_jitter_values___should_raise_error(self):
        invalid_values = [-1, 0, 1, 1.1]

        for value in invalid_values:
            with self.subTest(jitter=value):
                with self.assertRaises(ValueError) as ctx:
                    LilotaWorker(
                        LilotaWorkerTestCase.DB_URL,
                        node_heartbeat_interval_jitter=value,
                    )

                self.assertEqual(
                    str(ctx.exception),
                    "node_heartbeat_interval_jitter must be greater than 0 and smaller than 1",
                )

    def test_init___invalid_task_heartbeat_interval_values___should_raise_error(self):
        invalid_values = [-1, 0]

        for value in invalid_values:
            with self.subTest(task_heartbeat_interval=value):
                with self.assertRaises(ValueError) as ctx:
                    LilotaWorker(
                        LilotaWorkerTestCase.DB_URL,
                        task_heartbeat_interval=value,
                    )

                self.assertEqual(
                    str(ctx.exception),
                    "task_heartbeat_interval must be greater than 0",
                )

    def test_init___invalid_max_task_heartbeat_interval_values___should_raise_error(
        self,
    ):
        invalid_values = [-1, 0]

        for value in invalid_values:
            with self.subTest(max_task_heartbeat_interval=value):
                with self.assertRaises(ValueError) as ctx:
                    LilotaWorker(
                        LilotaWorkerTestCase.DB_URL,
                        max_task_heartbeat_interval=value,
                    )

                self.assertEqual(
                    str(ctx.exception),
                    "max_task_heartbeat_interval must be greater than 0",
                )

    def test_execute_tasks___after_executing_task___should_wait_task_heartbeat_interval(
        self,
    ):
        # Arrange
        worker = LilotaWorker(
            LilotaWorkerTestCase.DB_URL,
            task_heartbeat_interval=0.3,
        )
        worker._register(
            name="add", func=add, input_model=AddInput, output_model=AddOutput
        )
        task = Task(id=uuid4(), name="add", input={"a": 1, "b": 2})

        # Act
        # The worker finds the task, and no task when it looks the next time
        waits = self.waits_of_worker(worker, [task, None])

        # Assert
        self.assertEqual(waits, [0.3, 0.6])

    def test_register___nothing_is_registered___should_not_have_any_registration(self):
        # Arrange & Act
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL)

        # Assert
        self.assertEqual(len(worker._registry), 0)

    def test_register___one_class_is_registered___should_have_one_registration(self):
        # Arrange
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL)

        # Act
        worker._register("add_task", AddInput)

        # Assert
        self.assertEqual(len(worker._registry), 1)

    def test_register___task_is_already_registered___should_raise_exception(self):
        # Arrange
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL)
        worker._register("add_task", AddInput)

        # Act & Assert
        try:
            worker._register("add_task", AddInput)
        except RuntimeError as err:
            self.assertEqual(str(err), "Task 'add_task' is already registered")

    def test_register___set_max_attempts___should_be_set_on_registered_task(self):
        # Arrange
        max_attempts = 10
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL)

        # Act
        worker._register("add_task", AddInput, max_attempts=max_attempts)

        # Assert
        self.assertEqual(len(worker._registry), 1)
        registered_task = worker._registry["add_task"]
        self.assertEqual(registered_task.max_attempts, max_attempts)

    def test_start___but_started_twice___should_raise_exception(self):
        # Arrange
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL)
        worker._register(
            name="add", func=add, input_model=AddInput, output_model=AddOutput
        )
        self.start_worker(worker)

        # Act & Assert
        with self.assertRaises(Exception) as context:
            worker.start()
        self.assertEqual(str(context.exception), "The node is already started")

    def test_start___should_create_node(self):
        # Act
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL)
        self.start_worker(worker)

        # Assert
        node: Node = worker.get_node()
        self.assertEqual(node.id, worker._node_id)
        self.assertEqual(node.type, NodeType.WORKER)
        self.assertEqual(node.status, NodeStatus.RUNNING)
        self.assertIsNotNone(node.created_at)
        self.assertIsNotNone(node.last_seen_at)

    def test_start___with_heartbeat___should_start_the_heartbeat(self):
        # Arrange
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL, node_heartbeat_interval=1.0)
        self.start_worker(worker)
        node: Node = worker.get_node()
        first_seen = node.last_seen_at
        self.assertIsNotNone(first_seen)

        # Act
        self.wait_until(lambda: worker.get_node().last_seen_at > first_seen)

        # Assert
        node: Node = worker.get_node()
        self.assertGreater(
            node.last_seen_at, first_seen, "Heartbeat should update last_seen_at"
        )

    def test_stop___but_start_was_not_executed___should_raise_exception(self):
        # Arrange
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL)
        worker._register(
            name="add", func=add, input_model=AddInput, output_model=AddOutput
        )

        # Act & Assert
        with self.assertRaises(Exception) as context:
            worker.stop()
        self.assertEqual(
            str(context.exception),
            "The node cannot be stopped because it was not started",
        )

    def test_stop___with_heartbeat___should_stop_the_heartbeat(self):
        # Arrange
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL, node_heartbeat_interval=1.0)
        self.start_worker(worker)
        self.wait_for_heartbeat(worker)

        # Act
        worker.stop()

        # Read after the stop: a heartbeat between the two would change last_seen_at
        node: Node = worker.get_node()
        first_seen = node.last_seen_at
        self.assertIsNotNone(first_seen)
        time.sleep(2)

        # Assert
        node: Node = worker.get_node()
        self.assertEqual(
            node.last_seen_at, first_seen, "Heartbeat should not update last_seen_at"
        )

    def test_logging___when_starting_scheduler___should_log_correctly(self):
        # Arrange
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL, logging_level=logging.DEBUG)
        log_store: LogStore = LogStore(LilotaWorkerTestCase.DB_URL)

        # Act
        self.start_worker(worker)
        node: Node = worker.get_node()
        self.wait_for_heartbeat(worker)
        worker.stop()

        # Assert
        log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node.id)
        self.assertEqual(len(log_entries), 3)
        self.assertEqual("Node started", log_entries[0].message)
        self.assertEqual(
            f"Leadership acquired first time (node id: {node.id})",
            log_entries[1].message,
        )
        self.assertEqual("Node stopped", log_entries[2].message)

    def test_logging___when_starting_scheduler_twice___should_log_correctly(self):
        # Arrange
        worker1 = LilotaWorker(LilotaWorkerTestCase.DB_URL, logging_level=logging.DEBUG)
        worker2 = LilotaWorker(LilotaWorkerTestCase.DB_URL, logging_level=logging.DEBUG)
        log_store: LogStore = LogStore(LilotaWorkerTestCase.DB_URL)

        # Act: the second worker starts when the first one is the leader
        self.start_worker(worker1)
        self.wait_for_heartbeat(worker1)
        self.start_worker(worker2)
        self.wait_for_heartbeat(worker2)
        node1: Node = worker1.get_node()
        node2: Node = worker2.get_node()
        worker1.stop()
        worker2.stop()

        # Assert
        log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node1.id)
        self.assertEqual(len(log_entries), 3)
        self.assertEqual("Node started", log_entries[0].message)
        self.assertEqual(
            f"Leadership acquired first time (node id: {node1.id})",
            log_entries[1].message,
        )
        self.assertEqual("Node stopped", log_entries[2].message)

        log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node2.id)
        self.assertEqual(len(log_entries), 2)
        self.assertEqual("Node started", log_entries[0].message)
        self.assertEqual("Node stopped", log_entries[1].message)

    def test_leadership___with_one_worker___should_set_this_worker_as_leader(self):
        # Arrange
        log_store: LogStore = LogStore(LilotaWorkerTestCase.DB_URL)

        with LilotaWorkerTestCase.get_session() as session:
            session.query(NodeLeader).delete()
            worker = LilotaWorker(
                LilotaWorkerTestCase.DB_URL,
                node_heartbeat_interval=1.0,
                node_heartbeat_interval_jitter=None,
                node_timeout_sec=20,
                logging_level=logging.DEBUG,
            )
            node_leader = session.get(NodeLeader, 1)
            self.assertIsNone(node_leader)
            session.commit()

        # Act
        self.start_worker(worker)
        self.wait_for_heartbeat(worker)
        worker._heartbeat.stop()

        # Assert
        with LilotaWorkerTestCase.get_session() as session:
            node_leader = session.get(NodeLeader, 1)
            self.assertIsNotNone(node_leader)
            self.assertEqual(node_leader.node_id, worker.get_node().id)

        node_id = worker.get_node().id
        log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(node_id)
        self.assertEqual(len(log_entries), 2)
        self.assertEqual("Node started", log_entries[0].message)
        self.assertEqual(
            f"Leadership acquired first time (node id: {node_id})",
            log_entries[1].message,
        )

    def test_leadership___with_two_workers_and_first_one_expires___should_set_second_worker_as_leader(
        self,
    ):
        # Arrange
        log_store: LogStore = LogStore(LilotaWorkerTestCase.DB_URL)

        with LilotaWorkerTestCase.get_session() as session:
            session.query(NodeLeader).delete()
            session.query(Node).delete()
            worker1 = LilotaWorker(
                LilotaWorkerTestCase.DB_URL,
                node_heartbeat_interval=1.0,
                node_heartbeat_interval_jitter=None,
                node_timeout_sec=2,
                logging_level=logging.DEBUG,
            )
            node_leader = session.get(NodeLeader, 1)
            self.assertIsNone(node_leader)
            session.commit()

        self.start_worker(worker1)
        self.wait_for_heartbeat(worker1)
        worker1._heartbeat.stop()

        with LilotaWorkerTestCase.get_session() as session:
            node_leader = session.get(NodeLeader, 1)
            self.assertIsNotNone(node_leader)
            self.assertEqual(node_leader.node_id, worker1.get_node().id)

        # Act
        worker2 = LilotaWorker(
            LilotaWorkerTestCase.DB_URL,
            node_heartbeat_interval=1.0,
            node_timeout_sec=2,
            logging_level=logging.DEBUG,
        )
        self.start_worker(worker2)

        # The second worker becomes the leader when the lease of the first one has
        # expired. It looks for dead nodes when its own heartbeats have reached the
        # database for as long as the node timeout of 2 seconds.
        self.wait_for_log_entry(worker2.get_node().id, "Marked 1 stale node(s) as DEAD")
        worker2._heartbeat.stop()

        # Assert
        with LilotaWorkerTestCase.get_session() as session:
            node_leader = session.get(NodeLeader, 1)
            self.assertIsNotNone(node_leader)
            self.assertEqual(node_leader.node_id, worker2.get_node().id)

        worker1_id = worker1.get_node().id
        log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(worker1_id)
        self.assertEqual(len(log_entries), 2)
        self.assertEqual("Node started", log_entries[0].message)
        self.assertEqual(
            f"Leadership acquired first time (node id: {worker1_id})",
            log_entries[1].message,
        )
        self.assertFalse(
            any(
                entry.message == "Leadership acquired (takeover)"
                for entry in log_entries
            )
        )

        worker2_id = worker2.get_node().id
        log_entries: list[LogEntry] = log_store.get_log_entries_by_node_id(worker2_id)
        self.assertEqual("Node started", log_entries[0].message)
        self.assertEqual(
            f"Leadership acquired (new node id: {worker2_id})", log_entries[1].message
        )

        # In between the leader has renewed its lease
        self.assertIn(
            "Marked 1 stale node(s) as DEAD", [entry.message for entry in log_entries]
        )

    def test_leadership___with_two_workers_and_first_one_is_leader___should_not_change_leader(
        self,
    ):
        # Arrange
        with LilotaWorkerTestCase.get_session() as session:
            session.query(NodeLeader).delete()
            session.query(Node).delete()
            node_leader = session.get(NodeLeader, 1)
            self.assertIsNone(node_leader)
            session.commit()

        worker1 = LilotaWorker(
            LilotaWorkerTestCase.DB_URL,
            node_heartbeat_interval=1.0,
            node_heartbeat_interval_jitter=None,
            node_timeout_sec=2,
        )
        self.start_worker(worker1)
        self.wait_for_heartbeat(worker1)

        worker2 = LilotaWorker(
            LilotaWorkerTestCase.DB_URL,
            node_heartbeat_interval=1.0,
            node_heartbeat_interval_jitter=None,
            node_timeout_sec=2,
        )
        self.start_worker(worker2)
        self.wait_for_heartbeat(worker2)

        with LilotaWorkerTestCase.get_session() as session:
            node_leader = session.get(NodeLeader, 1)
            self.assertEqual(node_leader.node_id, worker1.get_node().id)
            first_expiration_date = node_leader.lease_expires_at

        # Act: the leader renews its lease with its next heartbeat
        self.wait_until(
            lambda: self.get_node_leader().lease_expires_at > first_expiration_date
        )

        # Assert
        with LilotaWorkerTestCase.get_session() as session:
            node_leader = session.get(NodeLeader, 1)
            self.assertEqual(node_leader.node_id, worker1.get_node().id)
            next_expiration_date = node_leader.lease_expires_at
            self.assertGreater(next_expiration_date, first_expiration_date)

    def test_update_status_on_expired_tasks___with_one_running_and_expired_task___should_set_status_expired(
        self,
    ):
        # Arrange
        logger = logging.getLogger("test_logger")
        task_id: UUID = self.create_task(
            Task(
                name="test",
                status=TaskStatus.RUNNING,
                run_at=datetime(2026, 3, 20, 0, 0, 0),
                expires_at=datetime(2026, 3, 20, 1, 0, 0),
            )
        )
        store = TaskStore(LilotaWorkerTestCase.DB_URL, logger)
        worker = LilotaWorker(LilotaWorkerTestCase.DB_URL, logging_level=logging.DEBUG)

        # Act
        self.start_worker(worker)
        self.wait_until(
            lambda: store.get_task_by_id(task_id).status == TaskStatus.EXPIRED
        )

        # Assert
        task: Task = store.get_task_by_id(task_id)
        self.assertEqual(task.status, TaskStatus.EXPIRED)

    def test_heartbeat___when_worker_has_just_started___should_not_mark_other_nodes_as_dead(
        self,
    ):
        # Arrange: a node that was not seen for an hour, and a worker that knows
        # nothing about the time before its first heartbeat
        node_id = self.create_node(last_seen_at=self.now() - timedelta(hours=1))
        heartbeat_task = self.create_heartbeat_task()

        # Act
        heartbeat_task.execute()

        # Assert
        self.assertEqual(self.get_node(node_id).status, NodeStatus.RUNNING)

    def test_heartbeat___when_heartbeats_have_reached_database_for_node_timeout___should_mark_other_nodes_as_dead(
        self,
    ):
        # Arrange
        node_id = self.create_node(last_seen_at=self.now() - timedelta(hours=1))
        heartbeat_task = self.create_heartbeat_task()

        # Act: heartbeats without a gap, for longer than the node timeout of 1 second
        self.execute_heartbeats(
            heartbeat_task,
            until=lambda: self.get_node(node_id).status == NodeStatus.DEAD,
        )

        # Assert
        self.assertEqual(self.get_node(node_id).status, NodeStatus.DEAD)

    def test_heartbeat___when_heartbeats_were_missing___should_not_give_back_tasks_of_other_nodes(
        self,
    ):
        # Arrange: a worker that may find dead nodes
        heartbeat_task = self.create_heartbeat_task()
        self.execute_heartbeats(
            heartbeat_task, until=heartbeat_task._can_find_dead_nodes
        )
        may_find_dead_nodes = heartbeat_task._can_find_dead_nodes()

        # The database cannot be reached for 1.5 seconds: the heartbeats of the worker
        # are missing, and so are those of another worker that is executing a task
        time.sleep(1.5)
        node_id = self.create_node(last_seen_at=self.now() - timedelta(seconds=1.5))
        store = TaskStore(LilotaWorkerTestCase.DB_URL, None)
        task_id = store.create_task("test")
        store.get_next_task(node_id)
        store.start_task(task_id, max_attempts=1)

        # Act: the first heartbeat after the outage
        heartbeat_task.execute()

        # Assert: the other worker was not seen for longer than the node timeout of
        # 1 second, but it is alive
        task = store.get_task_by_id(task_id)
        self.assertTrue(may_find_dead_nodes)
        self.assertEqual(self.get_node(node_id).status, NodeStatus.RUNNING)
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.locked_by, node_id)

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def start_worker(self, worker: LilotaWorker) -> None:
        """Start the worker in a thread and wait until it has started. It is stopped when the test has ended.

        The worker sends heartbeats, but it does not look for tasks. stop() does not
        end the loop that looks for tasks, so the thread would go on taking the tasks
        that the tests after this one create.
        """
        started = threading.Event()
        stopped = threading.Event()

        def wait_for_stop() -> None:
            started.set()
            stopped.wait()

        worker._execute_tasks = wait_for_stop
        thread = threading.Thread(target=worker.start, daemon=True)

        def stop_worker() -> None:
            try:
                if worker._is_started:
                    worker.stop()
            finally:
                stopped.set()
                thread.join(10)

        self.addCleanup(stop_worker)
        thread.start()
        self.assertTrue(started.wait(30), "The worker did not start")

    def wait_until(self, condition, timeout: float = 30) -> bool:
        """Wait until the condition is met. Returns False after the timeout."""
        deadline = time.monotonic() + timeout

        while True:
            if condition():
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.05)

    def wait_for_heartbeat(self, worker: LilotaWorker) -> bool:
        """Wait until the worker has sent its first heartbeat, with which it has tried to become the leader.

        Returns False after the timeout.
        """
        heartbeat_task = worker._heartbeat._task
        return self.wait_until(lambda: heartbeat_task._last_heartbeat_at is not None)

    def wait_for_log_entry(
        self, node_id: UUID, message: str, timeout: float = 30
    ) -> bool:
        """Wait until the node has written the message to the log. Returns False after the timeout."""
        log_store = LogStore(LilotaWorkerTestCase.DB_URL)
        return self.wait_until(
            lambda: any(
                entry.message == message
                for entry in log_store.get_log_entries_by_node_id(node_id)
            ),
            timeout,
        )

    def get_node_leader(self) -> NodeLeader:
        with LilotaWorkerTestCase.get_session() as session:
            return session.get(NodeLeader, 1)

    def create_heartbeat_task(self) -> WorkerHeartbeatTask:
        """Create the heartbeat task of a worker: a heartbeat every 0.2 seconds, and a node timeout of 1 second."""
        logger = logging.getLogger("test_logger")
        node_timeout_sec = 1
        node_store = NodeStore(LilotaWorkerTestCase.DB_URL, logger)
        return WorkerHeartbeatTask(
            0.2,
            None,
            node_store.create_node(NodeType.WORKER, NodeStatus.RUNNING),
            node_timeout_sec,
            node_store,
            NodeLeaderStore(LilotaWorkerTestCase.DB_URL, logger, node_timeout_sec),
            TaskStore(LilotaWorkerTestCase.DB_URL, logger),
            logger,
        )

    def execute_heartbeats(
        self, heartbeat_task: WorkerHeartbeatTask, until, timeout: float = 30
    ) -> None:
        """Execute the heartbeat task in its interval until the condition is met, or until the timeout."""
        deadline = time.monotonic() + timeout

        while True:
            heartbeat_task.execute()
            if until() or time.monotonic() >= deadline:
                return
            time.sleep(heartbeat_task.interval)

    def waits_of_worker(
        self, worker: LilotaWorker, tasks: list[Task | None]
    ) -> list[float]:
        """Let the worker look for a task once for every entry of the list, and return how long it waits after each time.

        The worker gets the entry as the task it has found, None for no task. It
        does not use the database for its tasks, and it does not really wait.
        """

        class EndOfTest(Exception):
            pass

        store = Mock()
        store.get_next_task.side_effect = [*tasks, EndOfTest()]
        store.start_task.side_effect = lambda id, max_attempts, timeout: next(
            task for task in tasks if task is not None and task.id == id
        )
        worker._task_store = store

        thread = threading.current_thread()
        sleep = time.sleep
        waits = []

        def record_wait(seconds: float) -> None:
            # Workers that other tests have started still look for tasks in their threads
            if threading.current_thread() is thread:
                waits.append(seconds)
            else:
                sleep(seconds)

        with patch("lilota.worker.time.sleep", record_wait):
            with self.assertRaises(EndOfTest):
                worker._execute_tasks()

        return waits

    def create_node(self, last_seen_at: datetime) -> UUID:
        with LilotaWorkerTestCase.get_session() as session:
            node = Node(
                type=NodeType.WORKER,
                status=NodeStatus.RUNNING,
                last_seen_at=last_seen_at,
            )
            session.add(node)
            session.commit()
            return node.id

    def get_node(self, id: UUID) -> Node:
        with LilotaWorkerTestCase.get_session() as session:
            return session.get(Node, id)

    def create_task(self, task: Task) -> Task:
        with LilotaWorkerTestCase.get_session() as session:
            session.add(task)
            session.commit()
            return task.id


if __name__ == "__main__":
    main()
