# Database

**lilota** executes tasks in a managed way. All information processed by **lilota** is stored in various database tables in order to make it transparent to the user what is happening in the system.


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
| `status`              | Current status of the task (`created`, `scheduled`, `running`, `completed`, `failed`, `cancelled`). |
| `run_at`              | Timestamp indicating when the task becomes eligible for execution.                                  |
| `attempts`            | Number of execution attempts made for this task.                                                    |
| `max_attempts`        | Maximum allowed retry attempts before the task is marked as failed.                                 |
| `timeout`             | Maximum execution duration allowed for the task before it should be considered timed out.           |
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
