"""add gis_org_id to branches — the 2GIS organization of a branch

Needed for the owner's link to a review in the 2GIS business account: /orgs/<organization>/reviews/<branch>.
Written idempotently (IF [NOT] EXISTS): environments bootstrapped via Base.metadata.create_all may already have the
column from scripts/apply_branch_org_id_column.py.

Revision ID: 7d3e9f1a2b4c
Revises: c1a2b3d4e5f6
Create Date: 2026-10-19
"""
from typing import Sequence, Union

from alembic import op


revision: str = "7d3e9f1a2b4c"
down_revision: Union[str, Sequence[str], None] = "c1a2b3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE branches ADD COLUMN IF NOT EXISTS gis_org_id BIGINT")


def downgrade() -> None:
    op.execute("ALTER TABLE branches DROP COLUMN IF EXISTS gis_org_id")
