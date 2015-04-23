from sqlalchemy import *
from migrate import *


from migrate.changeset import schema
pre_meta = MetaData()
post_meta = MetaData()
subjob = Table('subjob', pre_meta,
    Column('id', INTEGER, primary_key=True, nullable=False),
    Column('job_ib', INTEGER),
    Column('data', TEXT),
)

subjob = Table('subjob', post_meta,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('job_id', Integer),
    Column('data', Text),
)


def upgrade(migrate_engine):
    # Upgrade operations go here. Don't create your own engine; bind
    # migrate_engine to your metadata
    pre_meta.bind = migrate_engine
    post_meta.bind = migrate_engine
    pre_meta.tables['subjob'].columns['job_ib'].drop()
    post_meta.tables['subjob'].columns['job_id'].create()


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    pre_meta.bind = migrate_engine
    post_meta.bind = migrate_engine
    pre_meta.tables['subjob'].columns['job_ib'].create()
    post_meta.tables['subjob'].columns['job_id'].drop()
