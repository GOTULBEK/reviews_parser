"""Add users table for auth/RBAC

Revision ID: b7c8d9e0f1a2
Revises: f3a5c6d7e8b1
Create Date: 2026-05-19 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID as PGUUID

revision: str = "b7c8d9e0f1a2"
down_revision: Union[str, Sequence[str], None] = "f3a5c6d7e8b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE TYPE IF NOT EXISTS user_role AS ENUM ('admin', 'customer')")
    op.create_table(
        "users",
        sa.Column("id", PGUUID(as_uuid=True), primary_key=True),
        sa.Column("login", sa.String(128), nullable=False),
        sa.Column("hashed_password", sa.String(256), nullable=False),
        sa.Column(
            "role",
            sa.Enum("admin", "customer", name="user_role", create_type=False),
            nullable=False,
        ),
        sa.Column(
            "task_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("search_tasks.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_users_login", "users", ["login"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_users_login", table_name="users")
    op.drop_table("users")
    op.execute("DROP TYPE user_role")
