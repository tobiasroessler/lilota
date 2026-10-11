# Database

**lilota** executes tasks in a managed way. All information processed by **lilota** are stored in various database tables in order to make it transparent to the user what is happening in the system.

If you do not specify a connection string in the **db_url** then **lilota** uses **sqlite:///lilota.db** and stores the data in a SQLite database. **lilota** uses **SQLAlchemy** and therefore all databases that are supported by SQLAlchemy can be used here.


## Creating the tables

You do not have to create the tables yourself. Every scheduler and worker brings the database up to date when it is created, using **lilota**'s own **Alembic** migrations. To do it without starting anything, for example in a deployment step:

``` python
from lilota.db.alembic import upgrade_db

upgrade_db("postgresql+psycopg://user:password@localhost:5432/myapp")
```

Several processes can do this at the same moment, for example the worker processes of a web server that each create a **Lilota**, or several containers that start together. Only one of them creates the tables. The others notice that they came too late, wait a second and then find the database up to date.

For the same reason an error in a migration is not reported at once: **lilota** tries 5 times, with a second in between, before it gives up with **Could not update the database**.


## Sharing a database with your application

**lilota** can live in the same database as your application, even if your application manages its schema with **Alembic** too.

All tables **lilota** creates start with **lilota_**. **lilota** records its migration revision in its own table, **lilota_alembic_version**, rather than in Alembic's default **alembic_version**. Your application keeps **alembic_version** for itself, and neither one reads or changes the other's revision.

One thing has to be set up in your application. Alembic's autogenerate compares the database with your models. It sees the **lilota_** tables, does not find them in your models and generates a migration that drops them. Tell autogenerate to ignore them in your application's **migrations/env.py**:

``` python
def include_object(object, name, type_, reflected, compare_to):
    return not (type_ == "table" and name.startswith("lilota_"))

# ... and in both context.configure(...) calls:
context.configure(
    ...,
    include_object=include_object,
)
```

**lilota_alembic_version** starts with **lilota_** as well, so this filter covers it too.


## Upgrading from lilota 1.1.1 or older

Up to 1.1.1, **lilota** recorded its revision in **alembic_version**. Nothing has to be done by hand: on the first start after the upgrade, **lilota** moves its revision from **alembic_version** to **lilota_alembic_version**. It drops **alembic_version** if nothing else is left in it. Revisions that are not **lilota**'s are left where they are, so a revision of your application's in the same table is untouched.


## Cleaning up

**lilota** deletes nothing by itself. Every task, every retry, every log entry and every node stays in the database, and the row of a task keeps its **input**, **output** and **error**.

To delete what is old, call **cleanup** from time to time, for example once a day:

``` python
from datetime import timedelta

deleted = lilota.cleanup(older_than=timedelta(days=30))
print(deleted)  # {'tasks': 1250, 'log_entries': 310, 'nodes': 12}
```

**cleanup** deletes the rows that are older than the given age:

| Table           | Deleted                                                                    | The age is counted from                |
| --------------- | -------------------------------------------------------------------------- | -------------------------------------- |
| **lilota_task** | tasks with the status **completed**, **failed**, **expired** or **cancelled** | the time the task ended or expired     |
| **lilota_log**  | all entries                                                                | the time the entry was written         |
| **lilota_node** | nodes with the status **dead**                                             | the time the node was last seen        |

It never deletes

* a task that is waiting or being executed
* a failed or expired task that still waits for its retry
* a node that is alive

**cleanup** works in every mode, and **lilota** does not have to be started for it.

Two things to know:

* A node that has ended gets the status **dead** when a worker that is running notices that it is gone. Until then it is not deleted.
* A retry keeps the ID of the task it retries in **previous_task_id**. That task can be deleted before the retry is.


## Tables

### Node (lilota_node)

This table stores information about the scheduler responsible for scheduling tasks in the system, as well as the workers responsible for executing them.

| Column         | Description                                                                                        |
| -------------- | -------------------------------------------------------------------------------------------------- |
| `id`           | Unique identifier of the node (UUID).                                                              |
| `name`         | Optional human-readable name of the node.                                                          |
| `type`         | Type of node (`scheduler` or `worker`).                                                            |
| `status`       | Current lifecycle status of the node (e.g., `starting`, `running`, `stopped`, `dead`).             |
| `created_at`   | Timestamp when the node record was created.                                                        |
| `last_seen_at` | Timestamp of the most recent heartbeat received from the node. Used to detect stale or dead nodes. |


### NodeLeader (lilota_node_leader)

This table stores information about the worker that currently acts as the leader.

| Column             | Description                                                        |
| ------------------ | ------------------------------------------------------------------ |
| `id`               | Primary key for the leader record (typically a single-row table).  |
| `node_id`          | Identifier of the node currently acting as the cluster leader.     |
| `lease_expires_at` | Timestamp indicating when the leader lease expires if not renewed. |


### Task (lilota_task)

This table stores information about the tasks executed by the system.

| Column                | Description                                                                                         |
| --------------------- | --------------------------------------------------------------------------------------------------- |
| `id`                  | Unique identifier of the task (UUID).                                                               |
| `name`                | Name of the registered task function to execute.                                                    |
| `pid`                 | Process identifier associated with the task execution.                                              |
| `status`              | Current status of the task (`created`, `scheduled`, `running`, `completed`, `failed`, `expired`, `cancelled`). |
| `run_at`              | Timestamp indicating when the task becomes eligible for execution.                                  |
| `attempts`            | Which attempt this task is: 1 for a task that was scheduled, 2 for its first retry, and so on.      |
| `max_attempts`        | How often the task is executed at most. The first execution counts.                                 |
| `retried_at`          | Timestamp when a retry was created for this task. Empty if the task was not retried.                |
| `previous_task_id`    | For a retry: identifier of the task it retries. Empty for a task that was scheduled.                |
| `timeout`             | Maximum execution duration allowed for the task before it should be considered timed out.           |
| `expires_at`          | Optional timestamp (UTC) after which the task is considered expired and should no longer be executed.          |
| `progress_percentage` | Current progress of the task expressed as a percentage (0–100).                                     |
| `start_date_time`     | Timestamp when the task started execution.                                                          |
| `end_date_time`       | Timestamp when the task finished execution (successfully or with failure).                          |
| `input`               | JSON payload containing the input parameters for the task.                                          |
| `output`              | JSON payload containing the result produced by the task.                                            |
| `error`               | JSON object describing an error if the task execution failed.                                       |
| `locked_by`           | Identifier of the worker node currently holding the execution lock for the task.                    |
| `locked_at`           | Timestamp when the task was locked by a worker for execution.                                       |


### LogEntry (lilota_log)

**lilota** supports logging and stores all log messages in this table.

| Column       | Description                                                   |
| ------------ | ------------------------------------------------------------- |
| `id`         | Unique identifier of the log entry.                           |
| `created_at` | Timestamp when the log message was created.                   |
| `level`      | Logging level (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`).    |
| `logger`     | Name of the logger that produced the message.                 |
| `message`    | Log message text.                                             |
| `process`    | Identifier of the process that produced the log entry.        |
| `thread`     | Identifier of the thread that produced the log entry.         |
| `node_id`    | Optional reference to the node associated with the log entry. |
| `task_id`    | Optional reference to the task associated with the log entry. |


### Migration version (lilota_alembic_version)

This table is managed by **Alembic** and stores which **lilota** migration the database is at. See [Sharing a database with your application](#sharing-a-database-with-your-application) for why it is not called **alembic_version**.

| Column        | Description                                   |
| ------------- | --------------------------------------------- |
| `version_num` | Revision of the latest applied **lilota** migration. |
