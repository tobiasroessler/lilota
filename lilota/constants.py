DEFAULT_DB_URL = "sqlite:///lilota.db"
DEFAULT_TEST_DB_URL = "sqlite:///lilota_test.db"
# DEFAULT_TEST_DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/lilota_test"

# Every table lilota owns starts with this prefix.
TABLE_PREFIX = "lilota_"

# The table Alembic uses to remember which lilota migration a database is at. It is not
# Alembic's default "alembic_version" because lilota may share a database with an
# application that runs its own Alembic migrations, and both would then read and write
# the same row.
VERSION_TABLE = f"{TABLE_PREFIX}alembic_version"

# Where lilota up to 1.1.1 kept its migration revision.
LEGACY_VERSION_TABLE = "alembic_version"

# The exit code of a worker process that stops itself because the task it is executing
# has expired. With it the process manager can tell a worker that lilota stopped on
# purpose from a worker script that failed. Both end with an error, but only the first
# one is to be restarted. 75 is EX_TEMPFAIL in sysexits.h: a failure that is worth
# another try.
WORKER_EXIT_CODE_TASK_EXPIRED = 75

# The environment variable in which the process manager tells a worker process the ID
# of the process that started it. With it the worker notices when that process is gone
# and stops itself. A worker script that is started on its own does not have the
# variable and keeps running.
WORKER_PARENT_PID_ENV = "LILOTA_PARENT_PID"

# How often a worker process checks whether the process that started it is still
# there, in seconds.
WORKER_PARENT_CHECK_INTERVAL = 1.0
