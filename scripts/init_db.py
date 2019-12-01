import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class InitStockDB:

    DEFAULT_DB_USER = 'postgres'
    DEFAULT_DB_PASS = 'postgres'
    DEFAULT_DB_HOST = 'localhost'
    DEFAULT_DB_NAME = 'stockservice'
    DEFAULT_DB_CONFIG = {
        'user': DEFAULT_DB_USER,
        'password': DEFAULT_DB_PASS,
        'host': DEFAULT_DB_HOST,
        'dbname': DEFAULT_DB_NAME
    }
    DEFAULT_TABLES = {
        'events':
            {
                'transaction_id': 'uuid PRIMARY KEY',
                'event_type': 'VARCHAR',
                'date': 'TIMESTAMP',
                'store_number': 'INTEGER',
                'item_number': 'INTEGER',
                'value': 'INTEGER'
            },
        'stock':
            {
                'item_number': 'INTEGER NOT NULL',  # primary: item and store number
                'store_number': 'INTEGER NOT NULL',
                'PRIMARY KEY': '(item_number, store_number)',
                'current_value': 'INTEGER',
                'last_update': 'TIMESTAMP'
            }
    }

    def __init__(self):
        self.db_conn = None
        self.cursor = None
        self.db_config = self.DEFAULT_DB_CONFIG

    def create_db(self):
        main_connection = {
            'user': self.db_config['user'],
            'password': self.db_config['password'],
            'host': self.db_config['host']}
        main_db = psycopg2.connect(**main_connection)
        main_db.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        main_cursor = main_db.cursor()
        main_cursor.execute('CREATE DATABASE {};'.format(self.DEFAULT_DB_NAME))
        main_db.close()

    def create_tables(self):
        for table_name, columns in self.DEFAULT_TABLES.items():
            table_sql = 'CREATE TABLE {} ('.format(table_name)
            column_sql = []
            for column_name, column_type in columns.items():
                column_sql.append('{} {}'.format(column_name, column_type))
            table_sql += ', '.join(column_sql)
            table_sql += ');'
            logger.info(table_sql)
            try:
                self.cursor.execute(table_sql)
            except psycopg2.errors.DuplicateTable:
                logger.warning('Table "{}" already exists'.format(table_name))
                self.db_conn.rollback()
        self.db_conn.commit()

    def initialize(self):
        connection_params = {**self.db_config}
        try:
            self.db_conn = psycopg2.connect(**connection_params)
        except psycopg2.OperationalError:
            self.create_db()
            self.db_conn = psycopg2.connect(**connection_params)
        logger.debug(self.db_conn.info)
        self.cursor = self.db_conn.cursor()
        self.create_tables()
        self.db_conn.close()


if __name__ == '__main__':
    init_db = InitStockDB()
    init_db.initialize()
