from urllib.parse import quote_plus
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as db
from sqlalchemy.sql import text
from sqlalchemy.pool import QueuePool
from settings.config import SQL_DW_SERVER, SQL_DW_DATABASE, SQL_DW_USERNAME, SQL_DW_PASSWORD


odbc_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + SQL_DW_SERVER + ';DATABASE=' + SQL_DW_DATABASE \
           + ';UID=' + SQL_DW_USERNAME + ';PWD={%s};' % SQL_DW_PASSWORD


class DBConnect:

    def __init__(self, connection_string):
        # Define the engine
        self.engine = sqlalchemy.create_engine(connection_string, echo=True, poolclass=QueuePool, pool_recycle=3600,
                                               pool_pre_ping=True)
        self.session = sessionmaker(bind=self.engine)()

    def close(self):
        self.session.close()

    def dispose(self):
        self.engine.dispose()

    def insert(self, record):
        self.session.add(record)
        self.session.commit()

    def bulk_insert(self, record_list):
        self.session.bulk_save_objects(record_list)
        self.session.commit()

    def bulk_update(self, mapper, record_list):
        self.session.bulk_update_mappings(mapper, record_list)
        self.session.commit()

    def query_result(self, query, params=None):
        r = None
        if params is not None:
            r = self.session.execute(text(query), params).fetchall()
        else:
            r = self.session.execute(text(query)).fetchall()
        return r

    def execute(self, query="SELECT 1"):
        return self.session.execute(query).fetchall()


def get_synapse_connection():
    connection_string = 'mssql+pyodbc:///?odbc_connect=' + quote_plus(odbc_str)
    db_conn = DBConnect(connection_string)
    return db_conn


class RawData(declarative_base()):
    __tablename__ = 'raw_data'

    id = db.Column(db.INT, primary_key=True, autoincrement=True)
    file_name = db.Column(db.String(500))
    blob_name = db.Column(db.String(1000))
    created = db.Column(db.DateTime)
    last_modified = db.Column(db.DateTime)
    gs_uri = db.Column(db.String(1000))
    parquet_file_name = db.Column(db.String(1000))
    parser_status = db.Column(db.String(100))
    processing_attempts = db.Column(db.Integer)
    project_id = db.Column(db.String(200))
    dataset = db.Column(db.String(100))
