from sqlalchemy import *
from db_sync import manager
from sqlalchemy.orm import declarative_base


StaticData = declarative_base(metadata=MetaData(schema=manager.api.db_schema))


class EventType(StaticData):
    __tablename__ = 'event_type'

    id_seq = Sequence('event_type_id_seq', metadata=MetaData(schema=manager.api.db_schema), minvalue=1, increment=1)
    id = Column(BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True)
    event_id = Column(CHAR(1), nullable=False, unique=True)
    description = Column(Text)
    sha = Column(String(64))

    def __eq__(self, other):
        return self.sha == other.sha

    def __hash__(self):
        return hash((self.event_id, self.description,))


class ManageTables(StaticData):
    __tablename__ = 'manage_tables'

    id_seq = Sequence('manage_tables_id_seq', metadata=MetaData(schema=manager.api.db_schema), minvalue=1, increment=1)
    id = Column(BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True)
    server_id = Column(String(128), index=True)
    database_name = Column(String(64), nullable=False)
    database_type = Column(String(64), nullable=False)
    schema_name = Column(String(64), nullable=False)
    empty_schema = Column(Numeric(1,0), server_default=text("0"))
    table_name = Column(String(128), index=True)
    exclude = Column(Numeric(1,0), server_default=text("1"))
    exclude_columns = Column(Text, comment="column names separated by coma, reqexp() can be also used to exclude columns based on pattern")
    sha = Column(String(64), nullable=False)

    def __eq__(self, other):
        return self.sha == other.sha

    def __hash__(self):
        return hash((self.server_id, self.database_name, self.database_type, self.schema_name, self.table_name,))
