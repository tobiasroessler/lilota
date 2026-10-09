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
