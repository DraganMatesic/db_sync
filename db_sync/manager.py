from db_sync.utilities import env
from db_sync.deploy import tables
from db_sync.deploy import staticdata
from db_sync.utilities import database as db


class Manager:
    def __init__(self):
        env.check()
        self.db_schema = 'db_sync'
        self.sync_servers = ['db.mssql.datastore.ad','db.pstg.postgres']

    def prepare(self):
        for sync_server in self.sync_servers:
            # creating db_sync tables
            sync_db = db.Database(sync_server, check=True)
            sync_db.create_tables(tables.StaticData)

            with sync_db.start_session() as db_session:
                for records in staticdata.create():
                    sync_db.merge(records, db_session, delete_=True)

            sync_db.map_all_tables()


api = Manager()
if __name__ == '__main__':
    api.prepare()