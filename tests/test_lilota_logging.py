import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from contextlib import redirect_stderr
import io
import logging
from unittest import TestCase, main
from lilota.logging import configure_logging


class LilotaLoggingTestCase(TestCase):
    def test_error___when_database_cannot_be_reached___should_not_raise(self):
        # Arrange: a logger that writes to a database that cannot be reached
        logger = configure_logging(
            "sqlite:////folder/that/does/not/exist/lilota.db",
            "lilota.test_logging",
            logging.INFO,
        )
        has_raised = False

        # Act
        try:
            # A handler that cannot write a log entry may say so on stderr
            with redirect_stderr(io.StringIO()):
                logger.error("Heartbeat task failed")
        except Exception:
            has_raised = True

        # Assert
        self.assertFalse(
            has_raised,
            "Writing a log entry must not raise when the database cannot be reached",
        )


if __name__ == "__main__":
    main()
