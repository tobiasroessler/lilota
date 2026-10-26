"""task keep_input

Revision ID: c0874683c11d
Revises: f5d588b76a5c
Create Date: 2026-10-26 20:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c0874683c11d"
down_revision: Union[str, Sequence[str], None] = "f5d588b76a5c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # A single statement that fails when it ran before: a second process that migrates
    # at the same moment stops here without having changed anything. Existing tasks
    # keep their input, as they always did.
    op.add_column(
        "lilota_task",
        sa.Column("keep_input", sa.Boolean(), nullable=False, server_default=sa.true()),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("lilota_task", "keep_input")
