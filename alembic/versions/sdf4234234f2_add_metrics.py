"""add metrics

Revision ID: sdf4234234f2
Revises:
Create Date: 2026-05-011 15:45:39.664731

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "sdf4234234f2"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "chess_moves",
        sa.Column("model", sa.String(length=50), primary_key=True),
        sa.Column("accuracy", sa.Float()),
        sa.Column("f1", sa.Float()),
        if_not_exists=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("ml_metrics", if_exists=True)
