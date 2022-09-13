"""Initial migration

Revision ID: 534d0782e42e
Revises: 
Create Date: 2022-08-31 09:29:40.240865

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '534d0782e42e'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('nivel1')
    op.drop_table('nivel2')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('nivel2',
    sa.Column('id', mysql.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('line_hash', mysql.TEXT(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    mysql_collate='utf8mb4_0900_ai_ci',
    mysql_default_charset='utf8mb4',
    mysql_engine='InnoDB'
    )
    op.create_table('nivel1',
    sa.Column('id', mysql.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('line_hash', mysql.TEXT(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    mysql_collate='utf8mb4_0900_ai_ci',
    mysql_default_charset='utf8mb4',
    mysql_engine='InnoDB'
    )
    # ### end Alembic commands ###