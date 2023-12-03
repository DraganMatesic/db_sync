import sqlalchemy.sql.schema
from sqlalchemy import *
from sqlalchemy import event
from sqlalchemy.orm import declarative_base
from db_sync.utilities import database as db
from db_sync.utilities import functions
from functools import partial


schema = 'public'
Base = declarative_base()
Codebooks = declarative_base()


def hashing(context):
    print(context.__dict__)
    # print(context.get_current_parameters())
    return 1


class Data(Base):
    __tablename__ = 't_data'

    id_seq = Sequence('data_id_seq', metadata=Base.metadata, schema=schema)
    data_id = Column(BigInteger, id_seq,
                     server_default=id_seq.next_value(), primary_key=True,
                     comment="Unique identifier for a data.")

    TABLE_NAME = Column(String(255), nullable=False, comment='The name of the table in which a change occurred that this entry records.')
    EVENT_TYPE = Column(CHAR(1), nullable=False, comment='The type of event captured by this entry.')


class CodeEventType(Codebooks):
    __tablename__ = 'c_event_type'

    id_seq = Sequence('event_type_id_seq', metadata=Codebooks.metadata, schema=schema)
    id = Column(BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True)
    event_id = Column(CHAR(1), nullable=False, unique=True)
    description = Column(Text)
    sha = Column(String(64))

    def __eq__(self, other):
        return self.sha == other.sha


def create_tables():
    Base.metadata.drop_all(database.engine)
    Base.metadata.create_all(database.engine)

    Codebooks.metadata.drop_all(database.engine)
    Codebooks.metadata.create_all(database.engine)


def update_codebooks():
    hash_columns = ['event_id', 'description']
    event.listen(CodeEventType, "before_insert", partial(functions.db_hash, cols=hash_columns))

    event_type_codes = [CodeEventType(event_id='I', description='insert'),
                        CodeEventType(event_id='U', description='update'),
                        CodeEventType(event_id='D', description='delete'),
                        CodeEventType(event_id='R', description='reloading the entire table to the node')
                        ]

    with database.start_session() as db_session:
        db_session.add_all(event_type_codes)
        db_session.commit()


if __name__ == '__main__':
    database = db.Database('maindb', check=True)

    create_tables()
    update_codebooks()