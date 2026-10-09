import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from pathlib import Path
import tempfile
from unittest import TestCase, main
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, inspect, text
from lilota.constants import LEGACY_VERSION_TABLE, VERSION_TABLE
from lilota.db.alembic import get_alembic_config, include_object, upgrade_db
from lilota.models import Base

LILOTA_TABLES = {"lilota_log", "lilota_node", "lilota_node_leader", "lilota_task"}

# A revision of the application that shares the database with lilota.
HOST_REVISION = "0123456789ab"


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
