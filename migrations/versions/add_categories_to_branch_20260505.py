"""Add categories array to branch

Revision ID: b8c3d2e1f0a9
Revises: 941d938a4e6e
Create Date: 2026-05-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b8c3d2e1f0a9'
down_revision: Union[str, Sequence[str], None] = '941d938a4e6e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('branches', sa.Column('categories', postgresql.ARRAY(sa.String()), nullable=False, server_default='{}'))


def downgrade() -> None:
    op.drop_column('branches', 'categories')
