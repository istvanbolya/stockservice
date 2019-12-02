from unittest import TestCase
import testing.postgresql
import psycopg2

from database.database import StockServiceDB, StockServiceDBException


def init_test_db(postgresql):
    conn = psycopg2.connect(**postgresql.dsn())
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE events (transaction_id uuid PRIMARY KEY, event_type VARCHAR, date TIMESTAMP, store_number INTEGER, item_number INTEGER, value INTEGER);")
    cursor.execute(
        "INSERT INTO events values('7c71fb42-1f5e-45e1-be16-7d4d772d1aab', 'sale', '2018-12-03T23:57:40Z', 9, 12, 116)")
    cursor.close()
    conn.commit()
    conn.close()


Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True,
                                                  on_initialized=init_test_db)


def tearDownModule():
    Postgresql.clear_cache()


class StockServiceDBTest(TestCase):

    def setUp(self):
        self.postgresql = Postgresql()
        self.db = StockServiceDB(config=self.postgresql.dsn())

    def test_connect(self):
        self.db.config = {'user': '', 'password': '', 'host': '', 'dbname': ''}
        with self.assertRaises(StockServiceDBException) as context:
            self.db._connect()
        self.assertTrue('Cannot connect to database' in str(context.exception))
        self.db.config = self.postgresql.dsn()
        self.db._connect()
        self.assertIs(type(self.db.connection), psycopg2.extensions.connection)

    def test_query(self):
        raw_sql = 'SELECT transaction_id from events WHERE store_number=9;'
        self.db.cursor.execute(raw_sql)
        raw_result = self.db.cursor.fetchall()
        self.db.fields = ['transaction_id']
        self.db.table = 'events'
        self.where = 'store_number=9'
        self.db.query()
        self.assertEqual(self.db.query_result, raw_result[0])

    def test_insert(self):
        transaction_id = '8947695b-7f19-44b6-96b6-7f8ed041fe57'
        self.db.table = 'events'
        self.db.fields = ['transaction_id',
                          'event_type',
                          'date',
                          'store_number',
                          'item_number',
                          'value']
        self.db.values = [transaction_id,
                          'incoming',
                          '2019-04-14T03:47:13Z',
                          '5',
                          '12',
                          '400'
                          ]
        self.db.insert()
        raw_sql = "SELECT transaction_id from events WHERE transaction_id='{}';".format(transaction_id)
        self.db.cursor.execute(raw_sql)
        raw_result = self.db.cursor.fetchone()
        self.assertEqual(raw_result[0], transaction_id)

        with self.assertRaises(StockServiceDBException) as context:
            self.db.insert()
        self.assertTrue('Insert failed' in str(context.exception))

    def test_update(self):
        transaction_id = '7c71fb42-1f5e-45e1-be16-7d4d772d1aab'
        update_value = 666
        self.db.table = 'events'
        self.db.fields = ['value']
        self.db.values = [update_value]
        self.db.where = "transaction_id='{}'".format(transaction_id)
        self.db.update()
        raw_sql = "SELECT value from events WHERE transaction_id='{}';".format(transaction_id)
        self.db.cursor.execute(raw_sql)
        raw_result = self.db.cursor.fetchone()
        self.assertEqual(raw_result[0], update_value)

    def tearDown(self):
        self.postgresql.stop()
