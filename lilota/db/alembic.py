from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from importlib import resources
from sqlalchemy import (
    Connection,
    MetaData,
    Table,
    create_engine,
    delete,
    inspect,
    select,
)
from lilota.constants import LEGACY_VERSION_TABLE, TABLE_PREFIX, VERSION_TABLE


def get_migrations_path() -> str:
    migrations_pkg = resources.files("lilota").joinpath("db").joinpath("migrations")
    return str(migrations_pkg)


def get_alembic_config(db_url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", get_migrations_path())
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def include_object(object, name, type_, reflected, compare_to) -> bool:
    """Limit autogenerate to lilota's own tables.

    lilota may share a database with an application. Without this filter, autogenerate
    would see that application's tables, find them missing from lilota's models and
    generate migrations that drop them.
    """
    if type_ == "table":
        return name.startswith(TABLE_PREFIX)
    return True


def upgrade_db(db_url: str):
    """Bring the lilota tables in the database up to the latest migration.

    Before migrating, a database set up by lilota 1.1.1 or older is moved over to
    lilota's own version table (see adopt_legacy_version_table).

    Args:
      db_url (str): Database connection URL.
    """
    cfg = get_alembic_config(db_url)
    try:
        adopt_legacy_version_table(cfg, db_url)
        command.upgrade(cfg, "head")
    except Exception as ex:
        raise Exception(f"Could not update the database: {str(ex)}")


def current_rev(db_url: str):
    cfg = get_alembic_config(db_url)
    return command.current(cfg, verbose=True)


def adopt_legacy_version_table(cfg: Config, db_url: str) -> None:
    """Move lilota's migration revision from "alembic_version" to its own table.

    lilota up to 1.1.1 recorded its revision in Alembic's default table,
    "alembic_version". Since lilota uses VERSION_TABLE instead, such a database would
    look unmigrated and the initial migration would fail on tables that already exist.
    This copies the revision into VERSION_TABLE so the upgrade continues from where
    the database actually is.

    Only revisions that belong to lilota's own migrations are moved. The table may
    also be used by the application that shares the database, and its rows are left
    alone. "alembic_version" is dropped only if nothing is left in it.

    Several lilota processes usually start at the same time and each runs this. If
    another process adopts the table first, this one finds VERSION_TABLE in place and
    does nothing.

    Args:
      cfg (Config): Alembic configuration for lilota's migrations.
      db_url (str): Database connection URL.
    """
    engine = create_engine(db_url)
    try:
        with engine.begin() as connection:
            _adopt_legacy_version_table(cfg, connection)
    except Exception:
        # Another process may have created VERSION_TABLE in the meantime. Then the
        # work is done, and anything else is a real error.
        with engine.connect() as connection:
            if not _has_table(connection, VERSION_TABLE):
                raise
    finally:
        engine.dispose()


def _adopt_legacy_version_table(cfg: Config, connection: Connection) -> None:
    if _has_table(connection, VERSION_TABLE):
        return

    if not _has_table(connection, LEGACY_VERSION_TABLE):
        return

    script = ScriptDirectory.from_config(cfg)
    lilota_revisions = {revision.revision for revision in script.walk_revisions()}

    legacy_table = Table(LEGACY_VERSION_TABLE, MetaData(), autoload_with=connection)
    stored_revisions = connection.execute(select(legacy_table.c.version_num)).scalars()
    adopted_revisions = [rev for rev in stored_revisions if rev in lilota_revisions]

    if not adopted_revisions:
        return

    # Creates VERSION_TABLE and writes the revision into it, in this transaction.
    migration_context = MigrationContext.configure(
        connection, opts={"version_table": VERSION_TABLE}
    )
    for revision in adopted_revisions:
        migration_context.stamp(script, revision)

    connection.execute(
        delete(legacy_table).where(legacy_table.c.version_num.in_(adopted_revisions))
    )

    remaining = connection.execute(select(legacy_table.c.version_num)).first()
    if remaining is None:
        legacy_table.drop(connection)


def _has_table(connection: Connection, table_name: str) -> bool:
    return inspect(connection).has_table(table_name)
