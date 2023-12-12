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
