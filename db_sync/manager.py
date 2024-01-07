from db_sync import deploy
from db_sync.utilities import env
from db_sync.utilities import database as db


class Manager:
    def __init__(self):
        env.check()
        self.db_schema = 'db_sync'
        self.sync_servers = ['db.mssql.datastore.ad','db.pstg.postgres']

    def prepare(self, drop_create=False):
        """
        :param drop_create: it will recreate all schema, tables, triggers and add data
        :return:
        """
        for sync_server in self.sync_servers:
            # creating db_sync tables
            sync_db = db.Database(sync_server, check=True)
            sync_db.create_tables(deploy.tables.StaticData, drop=drop_create)

            with sync_db.start_session() as db_session:
                records = deploy.staticdata.event_type_data()
                sync_db.merge(records, db_session, delete_=True)

                db_tables = sync_db.get_all_tables()

                records = deploy.staticdata.database_table_data(db_tables)
                sync_db.merge(records, db_session, delete_=True)


api = Manager()
if __name__ == '__main__':
    api.prepare()