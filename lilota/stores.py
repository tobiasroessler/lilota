from abc import ABC
import os
from .models import (
    Node,
    NodeType,
    NodeStatus,
    NodeLeader,
    Task,
    TaskStatus,
    TaskProgress,
    LogEntry,
)
from datetime import datetime, timezone, timedelta
from sqlalchemy import (
    create_engine,
    delete,
    func,
    select,
    update,
    and_,
    not_,
    null,
    or_,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from typing import Any
from lilota.utils import normalize_data, to_json
from uuid import UUID
import logging


_ENGINE_CACHE = {}


def get_engine(db_url):
    if db_url not in _ENGINE_CACHE:
        _ENGINE_CACHE[db_url] = create_engine(
            db_url,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_pre_ping=True,
        )
    return _ENGINE_CACHE[db_url]


class StoreBase(ABC):
    """Abstract base class for all database stores.

    Provides common initialization and session management.
    """

    def __init__(self, db_url: str, logger: logging.Logger):
        """
        Args:
            db_url (str): Database connection URL.
            logger (logging.Logger): Logger for store operations.
        """
        self._db_url = db_url
        self._logger = logger
        self._engine = None
        self._Session = None

    def _ensure_engine(self):
        if self._engine is None:
            self._engine = get_engine(self._db_url)
            self._Session = sessionmaker(bind=self._engine, expire_on_commit=False)

    def _get_session(self):
        self._ensure_engine()
        return self._Session()


class NodeStore(StoreBase):
    """Database store for managing Lilota nodes."""

    def __init__(self, db_url: str, logger: logging.Logger):
        """
        Args:
            db_url (str): Database connection URL.
            logger (logging.Logger): Logger for store operations.
        """
        super().__init__(db_url, logger)

    def create_node(self, type: NodeType, status: NodeStatus = NodeStatus.IDLE) -> UUID:
        """Create a new node record in the database.

        Args:
            type (NodeType): Type of the node (scheduler or worker).
            status (NodeStatus): Initial lifecycle status.

        Returns:
            UUID: The unique identifier of the created node.
        """
        node = Node(type=type, status=status)

        with self._get_session() as session:
            with session.begin():
                session.add(node)

        return node.id

    def get_all_nodes(self):
        """Return all nodes in the database."""
        with self._get_session() as session:
            return session.query(Node).all()

    def get_node_by_id(self, id: UUID):
        """Return a node by its UUID.

        Args:
            id (UUID): Node identifier.

        Returns:
            Node | None: Node object if found, else None.
        """
        with self._get_session() as session:
            node = session.get(Node, id)
            if node is None:
                return None
            return node

    def update_node_status(self, id: UUID, status: NodeStatus):
        """Update the status of a node.

        Args:
            id (UUID): Node identifier.
            status (NodeStatus): New status.
        """
        with self._get_session() as session:
            with session.begin():
                stmt = update(Node).where(Node.id == id).values(status=status)
                session.execute(stmt)

    def update_status_on_dead_nodes(self, cutoff: datetime, exclude_node_id: UUID):
        """Mark nodes as DEAD if their last_seen_at is older than cutoff.

        Args:
            cutoff (datetime): Time threshold to consider a node dead.
            exclude_node_id (UUID): Node to exclude from the update.

        Returns:
            int: Number of nodes marked as DEAD.
        """
        with self._get_session() as session:
            with session.begin():
                result = session.execute(
                    update(Node)
                    .where(Node.last_seen_at < cutoff)
                    .where(Node.id != exclude_node_id)
                    .where(Node.status != NodeStatus.DEAD)
                    .values(status=NodeStatus.DEAD)
                )
                return result.rowcount

    def delete_dead_nodes(self, cutoff: datetime) -> int:
        """Delete the nodes with the status DEAD that were last seen before the cutoff.

        Args:
            cutoff (datetime): Nodes last seen before this time are deleted.

        Returns:
            int: Number of nodes that were deleted.
        """
        with self._get_session() as session:
            with session.begin():
                result = session.execute(
                    delete(Node).where(
                        Node.status == NodeStatus.DEAD, Node.last_seen_at < cutoff
                    )
                )
                return result.rowcount

    def update_node_last_seen_at(self, id: UUID):
        """Update the heartbeat timestamp of a node.

        Args:
            id (UUID): Node identifier.
        """
        with self._get_session() as session:
            with session.begin():
                session.execute(
                    update(Node)
                    .where(Node.id == id)
                    .values(last_seen_at=datetime.now(timezone.utc))
                )


class TaskStore(StoreBase):
    """Database store for managing Lilota tasks."""

    BATCH_SIZE = 50

    def __init__(self, db_url: str, logger: logging.Logger):
        """
        Args:
            db_url (str): Database connection URL.
            logger (logging.Logger): Logger for store operations.
        """
        super().__init__(db_url, logger)

    def create_task(self, name: str, input: Any = None):
        """Create a new task record in the database.

        Args:
            name (str): Registered task name.
            input (Any, optional): Input data for the task.

        Returns:
            UUID: The unique identifier of the created task.
        """
        if input is not None:
            input = normalize_data(input)

        task = Task(name=name, input=input, status=TaskStatus.CREATED)

        with self._get_session() as session:
            with session.begin():
                session.add(task)

        return task.id

    def get_all_tasks(self):
        """Return all tasks ordered by ID."""
        with self._get_session() as session:
            return session.query(Task).order_by(Task.id).all()

    def get_unfinished_tasks(self) -> list[Task]:
        """Return all tasks that are not yet completed or failed."""
        with self._get_session() as session:
            return (
                session.query(Task)
                .filter(
                    Task.status.in_(
                        [TaskStatus.CREATED, TaskStatus.SCHEDULED, TaskStatus.RUNNING]
                    )
                )
                .order_by(Task.id)
                .all()
            )

    def expire_overdue_tasks(self) -> None:
        """Set status to "expired" for running tasks whose expiration time has passed."""
        with self._get_session() as session:
            with session.begin():
                session.execute(
                    update(Task)
                    .where(
                        Task.status == TaskStatus.RUNNING,
                        Task.expires_at.is_not(None),
                        Task.expires_at < datetime.now(timezone.utc),
                    )
                    .values(status=TaskStatus.EXPIRED)
                )

                self._remove_input_of_finished_tasks(session)

    def release_tasks_of_dead_nodes(
        self, cutoff: datetime, exclude_node_id: UUID
    ) -> int:
        """Give back the tasks that were taken by a node that is dead.

        A worker that dies cannot finish the tasks it has taken, and no other
        worker takes a task that is locked. The tasks get the status "created"
        again, as if no worker had taken them, so that the next worker that is
        free executes them. This is not an attempt: ``attempts`` stays as it is.

        A node is dead if it was not seen since the cutoff, which is the rule of
        ``NodeStore.update_status_on_dead_nodes``, or if it does not exist. The
        status of the node is not looked at, because a node that was taken for
        dead and is seen again keeps the status DEAD.

        A task that has run into its timeout is not given back. It has taken too
        long, whatever happened to its worker, and is left to
        ``expire_overdue_tasks`` and ``retry_tasks``.

        Args:
            cutoff (datetime): Time threshold to consider a node dead.
            exclude_node_id (UUID): Node whose tasks are never given back.

        Returns:
            int: Number of tasks that were given back.
        """
        nodes_alive = select(Node.id).where(
            or_(Node.last_seen_at >= cutoff, Node.id == exclude_node_id)
        )

        with self._get_session() as session:
            with session.begin():
                result = session.execute(
                    update(Task)
                    .where(
                        Task.status.in_([TaskStatus.SCHEDULED, TaskStatus.RUNNING]),
                        Task.locked_by.is_not(None),
                        Task.locked_by.not_in(nodes_alive),
                        or_(
                            Task.expires_at.is_(None),
                            Task.expires_at >= datetime.now(timezone.utc),
                        ),
                    )
                    .values(
                        status=TaskStatus.CREATED,
                        pid=0,
                        progress_percentage=0,
                        start_date_time=None,
                        expires_at=None,
                        locked_by=None,
                        locked_at=None,
                    )
                )
                return result.rowcount

    def delete_finished_tasks(self, cutoff: datetime) -> int:
        """Delete the tasks that finished before the cutoff.

        A task is finished when it is completed, failed, expired or cancelled. A
        failed or expired task that still waits for its retry is kept. Without
        it the retry would never be created.

        A task finished at the time it ended. An expired task has no end time,
        so the time it expired is used instead.

        Args:
            cutoff (datetime): Tasks that finished before this time are deleted.

        Returns:
            int: Number of tasks that were deleted.
        """
        finished_at = func.coalesce(Task.end_date_time, Task.expires_at, Task.run_at)
        waits_for_retry = and_(
            Task.status.in_([TaskStatus.FAILED, TaskStatus.EXPIRED]),
            Task.retried_at.is_(None),
            Task.attempts < Task.max_attempts,
        )

        with self._get_session() as session:
            with session.begin():
                result = session.execute(
                    delete(Task).where(
                        Task.status.in_(
                            [
                                TaskStatus.COMPLETED,
                                TaskStatus.FAILED,
                                TaskStatus.EXPIRED,
                                TaskStatus.CANCELLED,
                            ]
                        ),
                        finished_at < cutoff,
                        not_(waits_for_retry),
                    )
                )
                return result.rowcount

    def has_unfinished_tasks(self) -> bool:
        """Check if there are any unfinished tasks in the database."""
        with self._get_session() as session:
            return session.query(
                session.query(Task)
                .filter(
                    Task.status.in_(
                        [TaskStatus.CREATED, TaskStatus.SCHEDULED, TaskStatus.RUNNING]
                    )
                )
                .exists()
            ).scalar()

    def get_task_by_id(self, id: UUID) -> Task:
        """Return a task by its UUID."""
        with self._get_session() as session:
            task = session.get(Task, id)
            if task is None:
                return None
            return task

    def get_next_task(self, worker_id: UUID) -> Task:
        """Return the next available task for a worker and lock it.

        Args:
            worker_id (UUID): Worker node locking the task.

        Returns:
            Task | None: The next scheduled task, or None if no task is available.
        """
        with self._get_session() as session:
            with session.begin():
                task_id = session.execute(
                    select(Task.id)
                    .where(Task.status == TaskStatus.CREATED)
                    .where(Task.run_at <= datetime.now(timezone.utc))
                    .order_by(Task.run_at)
                    .limit(1)
                ).scalar()

                if not task_id:
                    return None

                result = session.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .where(Task.status == TaskStatus.CREATED)
                    .values(
                        status=TaskStatus.SCHEDULED,
                        locked_at=datetime.now(timezone.utc),
                        locked_by=worker_id,
                    )
                )

            if result.rowcount != 1:
                return None

            return session.get(Task, task_id)

    def start_task(
        self,
        id: UUID,
        max_attempts: int,
        timeout: timedelta | None = None,
        keep_input: bool = True,
    ) -> Task:
        """Mark a task as RUNNING and initialize metadata.

        Args:
            id (UUID): Id of the task.
            max_attempts (int): Upper limit on how many times the task may be executed.
            timeout (timedelta | None): Optional timeout that can be set for a task.
            keep_input (bool): False to set the input to NULL once the task is
                finished for good. Written to the row, so that the leader knows it
                too when it expires or retries the task.

        Returns:
            Task | None: The started task, or None if no task is available.
        """
        expires_at = None
        start_date_time = datetime.now(timezone.utc)
        timeout_sec = int(timeout.total_seconds()) if timeout is not None else None

        with self._get_session() as session:
            with session.begin():
                task = self._load_task(session, id)
                task.timeout = timeout_sec

                if timeout is not None:
                    expires_at = start_date_time + timeout

                task.pid = os.getpid()
                task.status = TaskStatus.RUNNING
                task.max_attempts = max_attempts
                task.keep_input = keep_input
                task.progress_percentage = 0
                task.start_date_time = start_date_time
                task.expires_at = expires_at
                task.end_date_time = None
                return task

    def retry_tasks(self, batch_size=BATCH_SIZE):
        """Retry eligible tasks by creating new scheduled task instances.

        This method scans for tasks that are eligible for retry based on their
        status, expiration, and retry limits. It uses optimistic locking to safely
        "claim" tasks in concurrent environments, ensuring that no task is retried
        more than once across multiple workers.

        Eligible tasks include:
            - Tasks with status "failed" or "expired"
            - Tasks with status "running" that have exceeded their expiration time

        A task is only retried if:
            - It has not already been retried (`retried_at` is None)
            - It has remaining retry attempts (`attempts < max_attempts`)

        For each claimed task:
            - A new task is created with incremented attempt count
            - The retry is scheduled with a delay proportional to the number of attempts
            - The original task is marked with `retried_at` to prevent duplicate retries

        Concurrency is handled using optimistic locking via the `version` field.
        If another worker updates the task first, the current worker skips it.

        Args:
            batch_size (int, optional):
                Maximum number of tasks to process in a single run.
                Defaults to BATCH_SIZE.

        Returns:
            int:
                The number of retry tasks successfully created.

        Raises:
            sqlalchemy.exc.SQLAlchemyError:
                If a database error occurs during the transaction.

        Notes:
            - Uses a linear backoff strategy: delay = 5 * current_attempts (seconds)
            - All operations are executed within a single transactional session
            - Safe for concurrent execution across multiple workers
        """
        now = datetime.now(timezone.utc)

        with self._get_session() as session:
            with session.begin():
                # 1. Select tasks
                stmt = (
                    select(Task)
                    .where(
                        and_(
                            Task.retried_at.is_(None),
                            Task.attempts < Task.max_attempts,
                            or_(
                                Task.status.in_(["failed", "expired"]),
                                and_(
                                    Task.status == "running",
                                    Task.expires_at.is_not(None),
                                    Task.expires_at < now,
                                ),
                            ),
                        )
                    )
                    .order_by(Task.run_at)
                    .limit(batch_size)
                )

                tasks = session.execute(stmt).scalars().all()

                if not tasks:
                    return 0

                new_tasks = []

                for task in tasks:
                    if task.retried_at is not None:
                        continue

                    current_version = task.version
                    current_attempts = task.attempts

                    # 2. Try to "claim" via optimistic locking. The status is
                    # checked again: a task that was running when it was selected
                    # may have been completed since then, and must not be retried.
                    result = session.execute(
                        update(Task)
                        .where(
                            Task.id == task.id,
                            Task.version == current_version,
                            Task.retried_at.is_(None),
                            Task.status == task.status,
                        )
                        .values(retried_at=now, version=current_version + 1)
                    )

                    # 3. If no row updated: someone else took it
                    if result.rowcount == 0:
                        continue

                    # 4. Create retry task
                    retry_task = Task(
                        name=task.name,
                        status=TaskStatus.CREATED,
                        run_at=now + timedelta(seconds=5 * current_attempts),
                        attempts=current_attempts + 1,
                        max_attempts=task.max_attempts,
                        previous_task_id=task.id,
                        timeout=task.timeout,
                        keep_input=task.keep_input,
                        input=task.input,
                    )

                    new_tasks.append(retry_task)

                session.add_all(new_tasks)

                # The retries have a copy of the input now
                if new_tasks:
                    self._remove_input_of_finished_tasks(session)

                return len(new_tasks)

    def set_progress(self, id: UUID, progress: int):
        """Update the progress percentage of a task.

        Args:
            id (UUID): Id of the task.
            progress (int): The progress of the task (0-100)
        """
        with self._get_session() as session:
            with session.begin():
                task = self._load_task(session, id)
                task.progress_percentage = max(0, min(progress, 100))

    def end_task_success(
        self, id: UUID, output: Any, task_progress: TaskProgress
    ) -> bool:
        """Mark a task as successfully completed.

        A task that has ended or was retried in the meantime is not changed, see
        ``_end_task``.

        Args:
            id (UUID): Id of the task.
            output (Any): Return value of the task function. It is converted
                with ``to_json``.

        Returns:
            bool: True if the task was completed, False if it was not changed.

        Raises:
            TypeError: If the output cannot be converted to JSON.
        """
        output = to_json(output)
        return self._end_task(
            id, TaskStatus.COMPLETED, task_progress, {"output": output}
        )

    def end_task_failure(
        self,
        id: UUID,
        error: dict,
        task_progress: TaskProgress,
        can_be_retried: bool = True,
    ):
        """Mark a task as failed.

        Args:
            id (UUID): Id of the task.
            error (dict): Error information to store.
            can_be_retried (bool): False for a task that must not be executed
                again, whatever attempts it has left. Its ``max_attempts`` is
                set to the attempts it has made, so that ``retry_tasks`` leaves
                it alone. Defaults to True.

        Returns:
            bool: True if the task was set to failed, False if it was not changed
            because it has ended or was retried in the meantime, see ``_end_task``.
        """
        values = {"error": error}
        if not can_be_retried:
            values["max_attempts"] = Task.attempts

        return self._end_task(id, TaskStatus.FAILED, task_progress, values)

    def delete_task_by_id(self, id: UUID):
        """Delete a task by its UUID.

        Returns:
            bool: True if deleted, False if task not found.
        """
        with self._get_session() as session:
            with session.begin():
                task = session.get(Task, id)
                if task is None:
                    return False
                session.delete(task)
        return True

    def _end_task(
        self,
        id: UUID,
        status: TaskStatus,
        task_progress: TaskProgress,
        values: dict,
    ) -> bool:
        """Set the status a task ends with, unless it has ended or was retried.

        While a worker is about to store how its task has ended, the leader may
        expire the same task because it has run into its timeout, and create its
        retry. Only one of them may win. If the worker stored its result
        afterwards, the task would be completed and executed again by its retry.

        So the task is changed with a single statement that checks its state, as
        ``expire_overdue_tasks`` does. A task is not changed if it is completed,
        failed, expired or cancelled, if it has been retried, or if it does not
        exist any more.

        Args:
            id (UUID): Id of the task.
            status (TaskStatus): The status the task ends with.
            task_progress (TaskProgress): Tells whether the task has set its
                progress itself.
            values (dict): Further columns to set.

        Returns:
            bool: True if the task was changed.
        """
        values = {
            **values,
            "status": status,
            "end_date_time": datetime.now(timezone.utc),
        }
        if not task_progress.set_progress_manually:
            values["progress_percentage"] = 100

        with self._get_session() as session:
            with session.begin():
                result = session.execute(
                    update(Task)
                    .where(
                        Task.id == id,
                        Task.status.in_(
                            [
                                TaskStatus.CREATED,
                                TaskStatus.SCHEDULED,
                                TaskStatus.RUNNING,
                            ]
                        ),
                        Task.retried_at.is_(None),
                    )
                    .values(**values)
                )

                self._remove_input_of_finished_tasks(session, id)

                return result.rowcount == 1

    def _remove_input_of_finished_tasks(self, session, id: UUID | None = None) -> None:
        """Set the input to NULL of tasks with keep_input=False that are finished for good.

        A task is finished for good when nothing can execute it again:
          - it is completed,
          - it was retried: its retry has a copy of the input,
          - it has failed or expired with no attempts left.

        Called in the transaction of every statement that can finish a task, so the
        input is gone together with the change that finished it.

        Args:
          session: The session of that transaction.
          id (UUID | None): Only look at this task. None looks at all tasks, for the
            leader, which expires and retries many at once.
        """
        statement = update(Task).where(
            Task.keep_input.is_(False),
            Task.input.is_not(None),
            or_(
                Task.status == TaskStatus.COMPLETED,
                Task.retried_at.is_not(None),
                and_(
                    Task.status.in_([TaskStatus.FAILED, TaskStatus.EXPIRED]),
                    Task.attempts >= Task.max_attempts,
                ),
            ),
        )

        if id is not None:
            statement = statement.where(Task.id == id)

        # null(), not None: for a JSON column None is stored as the JSON value null,
        # which is not NULL to the WHERE clause above, and the row would be found
        # again every time.
        session.execute(statement.values(input=null()))

    def _load_task(self, session, id: UUID) -> Task:
        task: Task = session.get(Task, id)
        if task is None:
            raise ValueError(f"Task {id} not found")
        return task


class LogStore:
    """Database store for logging entries into Lilota."""

    def __init__(self, db_url: str):
        """
        Args:
            db_url (str): Database connection URL.
        """
        self._db_url = db_url
        self._engine = None
        self._Session = None

    def _ensure_engine(self):
        if self._engine is None:
            self._engine = create_engine(self._db_url, pool_pre_ping=True)
            self._Session = sessionmaker(
                bind=self._engine,
                expire_on_commit=False,
            )

    def get_session(self):
        """Return a new SQLAlchemy session."""
        self._ensure_engine()
        return self._Session()

    def get_log_entries(self) -> list[LogEntry]:
        """Return all log entries."""
        with self.get_session() as session:
            return session.query(LogEntry).order_by(LogEntry.created_at).all()

    def delete_log_entries(self, cutoff: datetime) -> int:
        """Delete the log entries that were written before the cutoff.

        Args:
            cutoff (datetime): Entries written before this time are deleted.

        Returns:
            int: Number of log entries that were deleted.
        """
        with self.get_session() as session:
            with session.begin():
                result = session.execute(
                    delete(LogEntry).where(LogEntry.created_at < cutoff)
                )
                return result.rowcount

    def get_log_entries_by_node_id(self, node_id: UUID) -> list[LogEntry]:
        """Return all log entries associated with a given node."""
        with self.get_session() as session:
            return (
                session.query(LogEntry)
                .filter(LogEntry.node_id == node_id)
                .order_by(LogEntry.created_at)
                .all()
            )


class NodeLeaderStore(StoreBase):
    """Store managing leader election for worker nodes."""

    def __init__(self, db_url: str, logger: logging.Logger, node_timeout_sec: int):
        """
        Args:
            db_url (str): Database connection URL.
            logger (logging.Logger): Logger for store operations.
            node_timeout_sec (int): Leader lease timeout in seconds.
        """
        super().__init__(db_url, logger)
        self._node_timeout_sec: int = node_timeout_sec

    def try_acquire_leadership(self, node_id) -> bool:
        """Attempt to acquire leadership for the given node.

        Returns True if leadership is acquired, False otherwise.
        """
        now = datetime.now(timezone.utc)
        new_expiry = now + timedelta(seconds=self._node_timeout_sec)

        with self._get_session() as session:
            try:
                # Try to take over expired lease
                result = session.execute(
                    update(NodeLeader)
                    .where(
                        NodeLeader.id == 1,
                        NodeLeader.lease_expires_at < now,
                    )
                    .values(
                        node_id=node_id,
                        lease_expires_at=new_expiry,
                    )
                )

                if result.rowcount == 1:
                    session.commit()
                    if self._logger:
                        self._logger.debug(
                            f"Leadership acquired (new node id: {node_id})"
                        )
                    return True

                # Check if row exists
                exists = session.execute(
                    select(NodeLeader.id).where(NodeLeader.id == 1)
                ).first()

                if exists:
                    session.rollback()
                    return False

                # No row → try to create it
                session.add(
                    NodeLeader(
                        id=1,
                        node_id=node_id,
                        lease_expires_at=new_expiry,
                    )
                )
                session.commit()
                if self._logger:
                    self._logger.debug(
                        f"Leadership acquired first time (node id: {node_id})"
                    )
                return True
            except IntegrityError:
                # Someone else won the race to insert
                session.rollback()
                return False
            except Exception:
                session.rollback()
                if self._logger:
                    self._logger.exception("Unexpected error in leader election")
                raise

    def renew_leadership(self, node_id):
        """Renew leadership lease for the given node.

        Returns True if renewed successfully.
        """
        now = datetime.now(timezone.utc)
        new_expiry = now + timedelta(seconds=self._node_timeout_sec)

        with self._get_session() as session:
            try:
                result = session.execute(
                    update(NodeLeader)
                    .where(
                        NodeLeader.id == 1,
                        NodeLeader.node_id == node_id,
                        NodeLeader.lease_expires_at >= now,
                    )
                    .values(lease_expires_at=new_expiry)
                )

                session.commit()
                renewed = result.rowcount == 1
                if renewed and self._logger:
                    self._logger.debug(f"Leadership renewed (node id: {node_id})")
                return renewed
            except Exception as ex:
                session.rollback()
                if self._logger:
                    self._logger.exception(f"Renew leadership failed: {ex}")
                raise
