import json
import os
import sqlalchemy
from sqlalchemy import *
from functools import partial
from datetime import datetime
from db_sync.utilities import env
from sqlalchemy.orm import sessionmaker
from db_sync.utilities import functions
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.decl_api import DeclarativeMeta


class Options:
    def __init__(self):
        self.trusted_conn = False


class Postgres:
    def __init__(self, db_config, driver, options: Options, **kwargs):
        """
        connection object for creating connection string for Microsoft SQL Server
        :param dict db_config: dictionary that contains all connection params from .env
        :param str driver: name of python db library that will be used (default is psycopg2)
        :param options: options available in Options object
        :param kwargs: all other params that can be added to connection string
        """
        self.libname = driver
        common = '{username}:{password}@{servername}/{databasename}'
        self.default = f'postgresql+psycopg2://{common}'
        self.conn_string = getattr(self, driver).format(**db_config)


class MsSQL:
    def __init__(self, db_config: dict, driver, options: Options, **kwargs):
        """
        connection object for creating connection string for Microsoft SQL Server
        :param dict db_config: dictionary that contains all connection params from .env
        :param str driver: name of python db library that will be used (default is pymmsql)
        :param options: options available in Options object
        :param kwargs: all other params that can be added to connection string
        """
        self.libname = driver
        if options.trusted_conn is False:
            common = '{username}:{password}@{servername}:{port}/{databasename}'
        else:
            common = '{servername}/{databasename}'
        self.default = f'mssql+pymssql://{common}'
        self.conn_string = getattr(self, driver).format(**db_config)


class Database:

    def __init__(self, db_node,  driver='default', check=False, **kwargs):
        """
        :param db_node: name of db node that contains database configuration in .env
        :param driver: specifies driver we're going to use psycopg2, cx_oracle, pymssql
        :param check: flag for checking if environment variables are loaded.
                      Set to True if running module as standalone
        """

        self.postgres = Postgres
        self.mssql = MsSQL
        self.options = Options()
        self.db_node = db_node
        self.db_config = dict()

        if check:
            # checks if env variables are loaded
            env.check()

        db_config = os.getenv(db_node)
        assert db_config is not None

        # convert to dict
        db_config = json.loads(db_config)
        assert type(db_config) is dict

        self.db_config = db_config

        # connection attributes
        self.engine = self.engine_construct(db_config, driver, **kwargs)

    def engine_construct(self, db_config, driver='default', **kwargs):
        dbtype = db_config.get('dbtype')
        assert dbtype is not None

        if dbtype not in self.__dict__:
            err_msg = f'''Can't find '{dbtype}' in env variables. Do you have this variable name in .env? Did you load env file properly?'''
            raise AttributeError(err_msg)

        [setattr(self.options, k, v) for k, v in db_config.items() if k in self.options.__dict__]

        database = getattr(self, dbtype)(db_config, driver, self.options)
        return create_engine(database.conn_string, pool_size=20, max_overflow=0, pool_pre_ping=True, **kwargs)

    def start_session(self) :
        return sessionmaker(self.engine)()

    @staticmethod
    def merge(data: list, session: sqlalchemy.orm.session.Session,
              filters=text(''), primary_key='id', archive_column='archive', archive=True, delete_=False, insert_=True):
        """
        Method that compares new data that is not yet in database with data that is in database.
        If database data does not exist in new data then it will be archived or deleted from specified database table.

        :param list data: list of sqlalchemy orm objects that we want to compare with records in database table
        :param session: database session from Database object
        :param filters: sqlalchemy filter that will be applied to orm table object
        :param primary_key: name of column that is considered non-composed primary key
        :param archive_column: name of column where timestamp when record was closed
        :param bool archive: if archive is True and delete is False existing data in database table will be archived
        :param bool delete_: if delete is True existing data in database table will be deleted
        :param bool insert_: if insert is True data will be inserted
        :return:
        """

        orm_obj = data[0].__class__

        # case when no filter is provided and archive is True, filter only unarchived table records
        if filters.text == '' and delete_ is False and archive is True:
            filters = and_(getattr(orm_obj, archive_column) == None)

        db_data = session.query(orm_obj).filter(filters).all()

        if delete_ is True or archive is True:
            archive_records = functions.set_compare(db_data, data, primary_key)
            for archive_record in archive_records:
                if delete_ is True:
                    session.delete(archive_record)
                if archive is True and delete_ is False:
                    setattr(archive_record, archive_column, datetime.now())
            session.commit()

        new_records = functions.set_compare(data, db_data, primary_key)
        if insert_ is True:
            session.add_all(new_records)
            session.commit()

    def create_schema(self, schema_name):
        """
        creates schema in database if it doesn't exist
        :param str schema_name: schema name that we want to create
        """
        dialect_name = self.engine.dialect.name
        with self.engine.connect() as conn:
            if dialect_name == 'postgresql':
                create_sql = schema.CreateSchema(schema_name, if_not_exists=True)
                conn.execute(create_sql)
                conn.commit()
            elif dialect_name == 'mssql':
                if schema_name not in conn.dialect.get_schema_names(conn):
                    conn.execute(schema.CreateSchema(schema_name))
                    conn.commit()
            else:
                raise NotImplementedError(f"dialect {dialect_name} not implemented")

    def drop_schema(self, schema_name):
        """
        drops schema in database if exist
        :param str schema_name: schema name that will be dropped
        """
        dialect_name = self.engine.dialect.name
        with self.engine.connect() as conn:
            if dialect_name == 'postgresql':
                create_sql = schema.DropSchema(schema_name, if_exists=True)
                conn.execute(create_sql)
                conn.commit()
            elif dialect_name == 'mssql':
                if schema_name in conn.dialect.get_schema_names(conn):
                    conn.execute(schema.DropSchema(schema_name))
                    conn.commit()
            else:
                raise NotImplementedError(f"dialect {dialect_name} not implemented")

    def get_schema_names(self):
        with self.engine.connect() as conn:
            return conn.dialect.get_schema_names(conn)

    def create_tables(self, declare_base, drop=True):
        """
        # creates one or more tables with same inherited declarative_base
        :param DeclarativeMeta declare_base: it is orm declarative_base
        :param bool drop: boolean if schema and tables should be dropped before create
        """

        if drop is True:
            declare_base.metadata.drop_all(self.engine)
            self.drop_schema(declare_base.metadata.schema)

        self.create_schema(declare_base.metadata.schema)
        declare_base.metadata.create_all(self.engine)

    def get_tables(self, cls, tablename, table, tablelist: list = None):
        tablelist.append({'server_id': self.db_node,
                          'database_name': self.engine.url.database,
                          'database_type': self.engine.name,
                          'schema_name': table.schema,
                          'table_name': table.name})

    def get_all_tables(self):
        map_tables = list()

        # todo https://docs.sqlalchemy.org/en/20/orm/extensions/automap.html
        db_schemas = self.get_schema_names()
        for db_schema in db_schemas:
            current_size = map_tables.__len__()
            base = automap_base()
            base.prepare(autoload_with=self.engine, schema=db_schema, modulename_for_table=partial(self.get_tables, tablelist=map_tables))

            if map_tables.__len__() == current_size:
                map_tables.append({'server_id': self.db_node, 'database_name': self.engine.url.database,
                                   'database_type': self.engine.name,'schema_name': db_schema,
                                   'table_name': None})
        return map_tables
