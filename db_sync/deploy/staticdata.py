from db_sync.deploy import tables
from db_sync.utilities import functions


def event_type_data():
    hash_columns = ['event_id', 'description']
    event_type_codes = [tables.EventType(event_id='I', description='insert'),
                        tables.EventType(event_id='U', description='update'),
                        tables.EventType(event_id='D', description='delete'),
                        tables.EventType(event_id='R', description='reloading the entire table to the node')]
    functions.db_hash(event_type_codes, hash_columns)
    return event_type_codes


def sync_tables_data():
    # todo individual sync selection example
    raise NotImplementedError("not implemented yet")


def create():
    return [event_type_data()]
