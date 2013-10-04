"""changing user a bit

Revision ID: 30295579ce49
Revises: 141a9ebca96a
Create Date: 2013-06-28 16:54:54.619818

"""

# revision identifiers, used by Alembic.
revision = '30295579ce49'
down_revision = '141a9ebca96a'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('orders', sa.Column('timestamp', sa.DateTime(), nullable=True))
    op.add_column('users', sa.Column('login_allowed', sa.Boolean(), server_default='false', nullable=True))
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('users', 'login_allowed')
    op.drop_column('orders', 'timestamp')
    ### end Alembic commands ###
