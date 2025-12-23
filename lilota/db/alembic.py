from alembic import command
from alembic.config import Config
from importlib import resources


def get_migrations_path() -> str:
  migrations_pkg = resources.files("lilota").joinpath("db").joinpath("migrations")
  return str(migrations_pkg)


def get_alembic_config(db_url: str) -> Config:
  cfg = Config()
  cfg.set_main_option("script_location", get_migrations_path())
  cfg.set_main_option("sqlalchemy.url", db_url)
  return cfg


def upgrade_db(db_url: str):
  cfg = get_alembic_config(db_url)
  try:
    command.upgrade(cfg, "head")
  except Exception as ex:
    raise Exception(f"Could not update the database: {str(ex)}")


def current_rev(db_url: str):
  cfg = get_alembic_config(db_url)
  return command.current(cfg, verbose=True)
