"""empty message

Revision ID: 141a9ebca96a
Revises: 3187ef2367e9
Create Date: 2013-06-27 16:21:30.817656

"""

# revision identifiers, used by Alembic.
revision = '141a9ebca96a'
down_revision = '3187ef2367e9'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('contracts', u'ticker',
               existing_type=sa.VARCHAR(),
               nullable=False)
    op.alter_column('futures', u'multiplier',
               existing_type=sa.BIGINT(),
               nullable=True,
               existing_server_default=u'1')
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('futures', u'multiplier',
               existing_type=sa.BIGINT(),
               nullable=False,
               existing_server_default=u'1')
    op.alter_column('contracts', u'ticker',
               existing_type=sa.VARCHAR(),
               nullable=True)
    ### end Alembic commands ###
