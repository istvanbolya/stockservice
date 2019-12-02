import psycopg2
from psycopg2.errors import UniqueViolation

from tools import logger
logger = logger.get_logger(__name__)


class StockServiceDBException(BaseException):
    pass


class StockServiceDB:

    DEFAULT_DB_USER = 'postgres'
    DEFAULT_DB_PASS = 'postgres'
    DEFAULT_DB_HOST = 'localhost'
    DEFAULT_DB_NAME = 'stockservice'
    DEFAULT_CONFIG = {
        'user': DEFAULT_DB_USER,
        'password': DEFAULT_DB_PASS,
        'host': DEFAULT_DB_HOST,
        'dbname': DEFAULT_DB_NAME
    }

    def __init__(self, config=None):
        self.connection = None
        self.config = config if config else self.DEFAULT_CONFIG
        self.cursor = None
        self.table = None
        self.fields = None
        self.values = None
        self.where = None
        self.query_result = None
        self._connect()

    def _connect(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
        except psycopg2.OperationalError:
            raise StockServiceDBException('Cannot connect to database! {}'.format(self.config['host']))

    def query(self):
        if not self.table:
            return
        query_sql = 'SELECT '
        query_sql += ', '.join(self.fields) if self.fields else '* '
        query_sql += ' from {} '.format(self.table)
        if self.where:
            query_sql += 'WHERE %s' % self.where
        query_sql += ';'
        logger.debug(query_sql)
        self.cursor.execute(query_sql)
        result = self.cursor.fetchall()
        self.query_result = result[0] if result else []

    def insert(self):
        if not self.table:
            return
        insert_sql = 'INSERT INTO '
        insert_sql += '{}({})'.format(self.table, ', '.join(self.fields))
        insert_sql += ' VALUES ({});'.format(','.join(['%s'] * len(self.values)))
        logger.debug(insert_sql)
        try:
            self.cursor.execute(insert_sql, self.values)
        except UniqueViolation as exc:
            self.connection.rollback()
            raise StockServiceDBException('Insert failed! {}'.format(exc))
        else:
            self.connection.commit()

    def update(self):
        if any([not self.table, not self.where]):
            return
        update_sql = 'UPDATE {} '.format(self.table)
        update_sql += "SET {}='{}' ".format(self.fields[0], self.values[0])
        update_sql += 'WHERE %s' % self.where
        logger.debug(update_sql)
        self.cursor.execute(update_sql, self.values)
        self.connection.commit()
