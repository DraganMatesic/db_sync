import os
import cx_Oracle
from sqlalchemy import *
from db_sync.utilities import env
from sqlalchemy.orm import sessionmaker


class Postgres:
    def __init__(self, dbname, driver):
        common = '{username}:{password}@{servername}/{databasename}'
        self.default = f'postgresql+psycopg2://{common}'
        self.libname = driver
        self.params = ['username', 'password', 'servername', 'databasename']
        params = {x: os.getenv(f"{dbname}_{x}") for x in self.params}
        self.conn_string = getattr(self, driver).format(**params)


class MsSQL:
    def __init__(self, dbname, driver):
        common = '{username}:{password}@{servername}:{port}/{databasename}'
        self.default = f'mssql+pymssql://{common}'
        self.libname = driver
        self.params = ['username', 'password', 'servername', 'port', 'databasename']
        params = {x: os.getenv(f"{dbname}_{x}") for x in self.params}
        self.conn_string = getattr(self, driver).format(**params)


class Oracle:
    def __init__(self, dbname, driver):
        common = '{username}:{password}@{serverip}:{serverport}/{sidname}'
        self.default = f'oracle://{common}'
        self.libname = driver
        self.params = ['username', 'password', 'serverip', 'serverport', 'sidname']
        params = {x: os.getenv(f"{dbname}_{x}") for x in self.params}
        self.conn_string = getattr(self, driver).format(**params)


class Database:

    def __init__(self, dbname,  driver='default', check=False, oracle=False, **kwargs):
        """
        :param dbname: prefix of environment variable for database we want to use for
                       example: dcdb, check in .env
        :param driver: specifies driver we're going to use psycopg2, cx_oracle, pymssql
        :param check: flag for checking if environment variables are loaded.
                      Set to True if running module as standalone
        :param oracle: if true it will load instant client from loction specified in
                       oracle_client env variable
        """
        if check:
            # checks if env variables are loaded
            env.check()

        if oracle is True:
            # initialize oracle lib
            try:
                cx_Oracle.init_oracle_client(os.getenv('oracle_client'))
            except cx_Oracle.ProgrammingError:
                pass

        # connection attributes
        self.engine = self.engine_construct(dbname, driver, **kwargs)

    @staticmethod
    def engine_construct(dbname, driver='default', **kwargs):
        dbname_type = os.getenv(f"{dbname}_type")
        if dbname_type is not None:
            database = globals().get(dbname_type)(dbname, driver)
            return create_engine(database.conn_string, pool_size=20, max_overflow=0, **kwargs)
        else:
            err_msg = f'''can't find {dbname}_type in env variables. Do you have this variable name in .env?
            Did you load env file?'''
            raise AttributeError(err_msg)

    def start_session(self):
        return sessionmaker(self.engine)()

