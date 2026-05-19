"""merge_heads

Revision ID: d047b4b780d5
Revises: a4b3c2d1e0f9, b7c8d9e0f1a2
Create Date: 2026-05-19 17:22:09.805564

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd047b4b780d5'
down_revision: Union[str, Sequence[str], None] = ('a4b3c2d1e0f9', 'b7c8d9e0f1a2')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
