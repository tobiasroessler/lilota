import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from pathlib import Path
import subprocess
import tempfile
import time
from unittest import TestCase, main
from unittest.mock import patch
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, inspect, text
from lilota.constants import LEGACY_VERSION_TABLE, MIGRATION_ATTEMPTS, VERSION_TABLE
from lilota.db.alembic import (
    adopt_legacy_version_table,
    get_alembic_config,
    include_object,
    upgrade_db,
)
from lilota.models import Base

LILOTA_TABLES = {"lilota_log", "lilota_node", "lilota_node_leader", "lilota_task"}

# A revision of the application that shares the database with lilota.
HOST_REVISION = "0123456789ab"

# The code of a process that migrates a database at a point in time. With it several
# processes migrate the same database at the same moment. The process gets the URL of
# the database and the point in time as arguments.
MIGRATE_AT_POINT_IN_TIME = """
import sys
import time
from lilota.db.alembic import upgrade_db

time.sleep(max(0.0, float(sys.argv[2]) - time.time()))
upgrade_db(sys.argv[1])
"""


class UpgradeDbTestCase(TestCase):
    """Tests for lilota's migrations, each against a new, empty SQLite database."""

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.db_url = f"sqlite:///{Path(self._directory.name) / 'test.db'}"
        self.engine = create_engine(self.db_url)
        self.head = ScriptDirectory.from_config(
            get_alembic_config(self.db_url)
        ).get_current_head()

    def tearDown(self):
        self.engine.dispose()
        self._directory.cleanup()

    def table_names(self) -> set[str]:
        with self.engine.connect() as connection:
            return set(inspect(connection).get_table_names())

    def versions(self, table_name: str) -> list[str]:
        with self.engine.connect() as connection:
            rows = connection.execute(text(f"SELECT version_num FROM {table_name}"))
            return sorted(row[0] for row in rows)

    def create_host_version_table(self, *revisions: str):
        """Create the "alembic_version" table an application using Alembic has."""
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    f"CREATE TABLE {LEGACY_VERSION_TABLE} "
                    "(version_num VARCHAR(32) NOT NULL PRIMARY KEY)"
                )
            )
            for revision in revisions:
                connection.execute(
                    text(f"INSERT INTO {LEGACY_VERSION_TABLE} VALUES (:revision)"),
                    {"revision": revision},
                )

    def create_legacy_lilota_database(self):
        """Create a database as lilota 1.1.1 left it: revision in "alembic_version"."""
        upgrade_db(self.db_url)
        with self.engine.begin() as connection:
            connection.execute(
                text(f"ALTER TABLE {VERSION_TABLE} RENAME TO {LEGACY_VERSION_TABLE}")
            )

    def test_upgrade_db___empty_database___should_use_own_version_table(self):
        # Act
        upgrade_db(self.db_url)

        # Assert
        self.assertEqual(self.table_names(), LILOTA_TABLES | {VERSION_TABLE})
        self.assertEqual(self.versions(VERSION_TABLE), [self.head])

    def test_upgrade_db___called_twice___should_change_nothing(self):
        # Arrange
        upgrade_db(self.db_url)

        # Act
        upgrade_db(self.db_url)

        # Assert
        self.assertEqual(self.table_names(), LILOTA_TABLES | {VERSION_TABLE})
        self.assertEqual(self.versions(VERSION_TABLE), [self.head])

    def test_upgrade_db___several_processes_at_the_same_moment___should_migrate_without_error(
        self,
    ):
        # Arrange: 6 processes that migrate the new database 2 seconds from now, which
        # is the time they need to start
        point_in_time = time.time() + 2
        processes = [
            subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    MIGRATE_AT_POINT_IN_TIME,
                    self.db_url,
                    str(point_in_time),
                ],
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(6)
        ]

        # Act
        errors = []
        for process in processes:
            _, stderr = process.communicate()
            if process.returncode != 0:
                errors.append(self.error_message(stderr))

        # Assert
        self.assertEqual(
            errors,
            [],
            f"{len(errors)} of {len(processes)} processes that migrated the database "
            "at the same moment have failed",
        )
        self.assertEqual(self.table_names(), LILOTA_TABLES | {VERSION_TABLE})
        self.assertEqual(self.versions(VERSION_TABLE), [self.head])

    def test_upgrade_db___database_that_cannot_be_migrated___should_raise_after_last_attempt(
        self,
    ):
        # Arrange: a database that cannot even be opened. Trying again does not help.
        db_url = "sqlite:////folder/that/does/not/exist/lilota.db"

        # Act
        with (
            patch("lilota.db.alembic.MIGRATION_RETRY_DELAY", 0),
            patch(
                "lilota.db.alembic.adopt_legacy_version_table",
                wraps=adopt_legacy_version_table,
            ) as attempt,
            self.assertRaises(Exception) as raised,
        ):
            upgrade_db(db_url)

        # Assert
        self.assertTrue(
            str(raised.exception).startswith("Could not update the database: ")
        )
        self.assertEqual(attempt.call_count, MIGRATION_ATTEMPTS)

    def error_message(self, stderr: str) -> str:
        """Return the line of a traceback that names the exception."""
        lines = stderr.strip().splitlines()
        return next(
            (line for line in lines if line.startswith("Exception: ")), lines[-1]
        )

    def test_upgrade_db___database_of_another_application___should_leave_its_version_alone(
        self,
    ):
        # Arrange
        self.create_host_version_table(HOST_REVISION)

        # Act
        upgrade_db(self.db_url)

        # Assert
        self.assertEqual(
            self.table_names(), LILOTA_TABLES | {VERSION_TABLE, LEGACY_VERSION_TABLE}
        )
        self.assertEqual(self.versions(LEGACY_VERSION_TABLE), [HOST_REVISION])
        self.assertEqual(self.versions(VERSION_TABLE), [self.head])

    def test_upgrade_db___legacy_database___should_move_revision_and_keep_data(self):
        # Arrange
        self.create_legacy_lilota_database()
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO lilota_log (created_at, level, logger, message) "
                    "VALUES (CURRENT_TIMESTAMP, 'INFO', 'test', 'kept')"
                )
            )

        # Act
        upgrade_db(self.db_url)

        # Assert
        self.assertEqual(self.table_names(), LILOTA_TABLES | {VERSION_TABLE})
        self.assertEqual(self.versions(VERSION_TABLE), [self.head])
        with self.engine.connect() as connection:
            messages = connection.execute(text("SELECT message FROM lilota_log"))
            self.assertEqual([row[0] for row in messages], ["kept"])

    def test_upgrade_db___legacy_database_shared_with_application___should_move_only_lilota_revision(
        self,
    ):
        # Arrange
        self.create_legacy_lilota_database()
        with self.engine.begin() as connection:
            connection.execute(
                text(f"INSERT INTO {LEGACY_VERSION_TABLE} VALUES (:revision)"),
                {"revision": HOST_REVISION},
            )

        # Act
        upgrade_db(self.db_url)

        # Assert
        self.assertEqual(self.versions(LEGACY_VERSION_TABLE), [HOST_REVISION])
        self.assertEqual(self.versions(VERSION_TABLE), [self.head])

    def test_include_object___tables_of_another_application___should_not_be_dropped_by_autogenerate(
        self,
    ):
        # Arrange
        upgrade_db(self.db_url)
        host_metadata = MetaData()
        Table("customer", host_metadata, Column("id", Integer, primary_key=True))
        host_metadata.create_all(self.engine)

        # Act
        with self.engine.connect() as connection:
            context = MigrationContext.configure(
                connection,
                opts={
                    "include_object": include_object,
                    "version_table": VERSION_TABLE,
                },
            )
            differences = compare_metadata(context, Base.metadata)

        # Assert
        self.assertEqual(differences, [])


if __name__ == "__main__":
    main()
