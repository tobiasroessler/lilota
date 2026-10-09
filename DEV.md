# Development

## Use uv

* uv init .
* uv venv --python 3.12
* uv add --group dev build twine mkdocs-material mkdocstrings-python ruff
* uv add alembic psycopg psycopg-binary SQLAlchemy
* uv sync
  

## Use alembic

### Recreate DB
* Delete the database file, i.e. lilota.db
* Delete all files in db/migrations/versions folder
* Create initial schema
* Create migrations

### Create initial schema
alembic init lilota/db/migrations

### Set tartget_metadata in migrations/env.py
from lilota.models import Base
target_metadata = Base.metadata

### Keep lilota's version table and table filter in migrations/env.py
Both context.configure(...) calls need:

    version_table=VERSION_TABLE,        # from lilota.constants
    include_object=include_object,      # from lilota.db.alembic

Without version_table, lilota writes its revision into "alembic_version" and clashes with
an application that shares the database. Without include_object, autogenerate against
such a database drops the application's tables. All lilota tables start with "lilota_".

### Create migrations
alembic revision --autogenerate -m "initial schema"


## MkDocs

In project folder: mkdocs new .
mkdocs serve


## Publish package - pip

rm -rf build dist *.egg-info
python3 -m build --sdist
python3 -m build --wheel
twine upload dist/*


## Publish package - uv

rm -rf build dist *.egg-info
uv build
uv publish (use __token__ and then copy the token)


