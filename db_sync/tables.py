from sqlalchemy import *
from sqlalchemy.orm import declarative_base
from db_sync.utilities import database as db
from db_sync.utilities import functions

schema = 'public'
Base = declarative_base()
Codebooks = declarative_base()


class Data(Base):
    __tablename__ = 'syn_data'

    id_seq = Sequence('data_id_seq', metadata=Base.metadata, schema=schema)
    data_id = Column(BigInteger, id_seq,
                     server_default=id_seq.next_value(), primary_key=True,
                     comment="Unique identifier for a data.")

    table_name = Column(String(255), nullable=False, comment='''The name of the table in which a change occurred that 
    this entry records.''')

    event_type = Column(CHAR(1), nullable=False, comment='The type of event captured by this entry.')

    row_data = Column(Text, comment='''The captured data change from the synchronized table. 
    The column values are stored in comma-separated values (CSV) format.''')

    pk_data = Column(Text, comment='''The primary key values of the captured data change from the synchronized table. 
    This data is captured for updates and deletes. The primary key values are stored in comma-separated values 
    (CSV) format.''')

    old_data = Column(Text, comment='''The captured data values prior to the update. 
    The column values are stored in CSV format.''')


class CodeEventType(Codebooks):
    __tablename__ = 'syn_event_type'

    id_seq = Sequence('event_type_id_seq', metadata=Codebooks.metadata, schema=schema)
    id = Column(BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True)
    event_id = Column(CHAR(1), nullable=False, unique=True)
    description = Column(Text)
    sha = Column(String(64))

    def __eq__(self, other):
        return self.sha == other.sha

    def __hash__(self):
        return hash((self.event_id, self.description,))


def create_tables():
    Base.metadata.drop_all(postgres_db.engine)
    Base.metadata.create_all(postgres_db.engine)

    Codebooks.metadata.drop_all(postgres_db.engine)
    Codebooks.metadata.create_all(postgres_db.engine)


def update_codebooks():
    hash_columns = ['event_id', 'description']
    event_type_codes = [CodeEventType(event_id='I', description='insert'),
                        CodeEventType(event_id='U', description='update'),
                        CodeEventType(event_id='D', description='delete'),
                        CodeEventType(event_id='R', description='reloading the entire table to the node')]
    functions.db_hash(event_type_codes, hash_columns)

    with postgres_db.start_session() as db_session:
        postgres_db.merge(event_type_codes, db_session, CodeEventType, delete_=True)


if __name__ == '__main__':
    postgres_db = db.Database('db.pstg.postgres', check=True)
    # datastore_db = db.Database('db.mssql.datastore.ad', check=True)  # mssql domain login
    datastore_db = db.Database('db.mssql.datastore', check=True)  # mssql user/password login

    import sqlalchemy

    x = sqlalchemy.schema.CreateSchema(schema, if_not_exists=True)
    print(x)
    # conn = datastore_db.engine.connect()
    # conn.execute(sqlalchemy.schema.CreateSchema(schema, if_not_exists=True))
    # print(conn.dialect.get_schema_names(conn))
    # if schema not in [x.lower() for x in conn.dialect.get_schema_names(conn)]:


    # with datastore_db.start_session() as ms_session:
    #     records = ms_session.execute(text("select * from persons")).all()
    #     print(records)