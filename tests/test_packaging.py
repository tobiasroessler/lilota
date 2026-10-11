import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pathlib import Path
import shutil
import subprocess
import tempfile
from unittest import SkipTest, TestCase, main
from zipfile import ZipFile

PROJECT = Path(__file__).resolve().parent.parent

# The files and folders of the project that a build can put into the wheel.
SOURCES = ["pyproject.toml", "README.md", "LICENSE", "lilota", "sample"]

MIGRATIONS = Path("lilota") / "db" / "migrations"


class WheelTestCase(TestCase):
    """Tests for what is in the wheel that is published.

    The wheel is built once for all tests, with uv, from a copy of the sources.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        if shutil.which("uv") is None:
            raise SkipTest("The wheel is built with uv, which is not installed")

        with tempfile.TemporaryDirectory() as directory:
            # A copy, because a build leaves folders behind in the sources
            sources = Path(directory) / "sources"
            for name in SOURCES:
                if (PROJECT / name).is_dir():
                    shutil.copytree(PROJECT / name, sources / name)
                else:
                    sources.mkdir(exist_ok=True)
                    shutil.copy(PROJECT / name, sources / name)

            # A compiled migration script, as Python leaves it behind when lilota has
            # been used. A new checkout of the project does not have one.
            compiled_file = sources / MIGRATIONS / "__pycache__" / "env.cpython-312.pyc"
            compiled_file.parent.mkdir(exist_ok=True)
            compiled_file.write_bytes(b"")

            wheel_directory = Path(directory) / "wheel"
            build = subprocess.run(
                ["uv", "build", "--wheel", "--out-dir", wheel_directory, sources],
                capture_output=True,
                text=True,
            )
            if build.returncode != 0:
                raise Exception(f"The wheel could not be built:\n{build.stderr}")

            (wheel,) = wheel_directory.glob("*.whl")
            with ZipFile(wheel) as file:
                cls.files = file.namelist()

    def test_wheel___should_contain_only_the_lilota_package(self):
        # Act: the folders that are installed next to each other. The folder with the
        # metadata of the wheel is not a package.
        top_level_names = {
            name.split("/")[0]
            for name in self.files
            if not name.split("/")[0].endswith(".dist-info")
        }

        # Assert
        self.assertEqual(top_level_names, {"lilota"})

    def test_wheel___should_not_contain_compiled_files(self):
        # Act
        compiled_files = [name for name in self.files if name.endswith(".pyc")]

        # Assert
        self.assertEqual(compiled_files, [])

    def test_wheel___should_contain_the_migrations(self):
        # Arrange: every file of the migrations, except for the compiled ones
        migration_files = [
            path.relative_to(PROJECT).as_posix()
            for path in (PROJECT / MIGRATIONS).rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        ]

        # Assert
        self.assertGreater(len(migration_files), 0)
        for name in migration_files:
            with self.subTest(name):
                self.assertIn(name, self.files)


if __name__ == "__main__":
    main()
