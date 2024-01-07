from db_sync import deploy
from db_sync.utilities import functions


def event_type_data():
    hash_columns = ['event_id', 'description']
    event_type_codes = [deploy.tables.EventType(event_id='I', description='insert'),
                        deploy.tables.EventType(event_id='U', description='update'),
                        deploy.tables.EventType(event_id='D', description='delete'),
                        deploy.tables.EventType(event_id='R', description='reloading the entire table to the node')]
    functions.db_hash(event_type_codes, hash_columns)
    return event_type_codes


def database_table_data(records: list[dict]):
    hash_columns = ['server_id', 'database_name', 'database_type', 'schema_name', 'table_name']
    [record.update({'empty_schema': 1}) for record in records if record.get('table_name') is None]
    db_tables = [deploy.tables.ManageTables(**record) for record in records]
    functions.db_hash(db_tables, hash_columns)
    return db_tables


def sync_tables_data():
    # todo individual sync selection example
    raise NotImplementedError("not implemented yet")


def create():
    return [event_type_data()]
