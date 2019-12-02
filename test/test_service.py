from unittest import TestCase, mock
import testing.postgresql
import psycopg2

from service.service import StockService, StockServiceException
from database.database import StockServiceDB


def init_test_db(postgresql):
    conn = psycopg2.connect(**postgresql.dsn())
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE events (transaction_id uuid PRIMARY KEY, event_type VARCHAR, date TIMESTAMP, store_number INTEGER, item_number INTEGER, value INTEGER);")
    cursor.execute(
        "INSERT INTO events values('7c71fb42-1f5e-45e1-be16-7d4d772d1aab', 'sale', '2018-12-03T23:57:40Z', 9, 12, 116)")
    cursor.execute(
        "CREATE TABLE stock (item_number INTEGER NOT NULL, store_number INTEGER NOT NULL, PRIMARY KEY (item_number, store_number), current_value INTEGER, last_update TIMESTAMP);")
    cursor.execute(
        "INSERT INTO stock values(12, 9, 116, '2018-12-03T23:57:40Z')")
    cursor.close()
    conn.commit()
    conn.close()


Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True,
                                                  on_initialized=init_test_db)


def tearDownModule():
    Postgresql.clear_cache()


class StockServiceTest(TestCase):

    def setUp(self):
        mock_kafka_client = mock.patch('kafka_client.client.StockKafkaClient')
        self.postgresql = Postgresql()
        self.test_service_db = StockServiceDB(config=self.postgresql.dsn())
        self.service = StockService(database=self.test_service_db,
                                    kafka_client=mock_kafka_client)

    def test_check_event_fields(self):
        acceptable_event = {'transaction_id': '407f9c78-13a1-4745-a491-84c4bb09468c',
                            'event_type': 'incoming',
                            'date': '2019-06-06T19:14:48Z',
                            'store_number': '2',
                            'item_number': '10',
                            'value': '400'}
        self.service.event = acceptable_event
        self.assertIsNone(self.service._check_event_fields())
        event1_to_reject = {'transaction_id': '8947695b-7f19-44b6-96b6-7f8ed041fe57'}
        self.service.event = event1_to_reject
        with self.assertRaises(StockServiceException) as context:
            self.service._check_event_fields()
        self.assertTrue('Event fields are different' in str(context.exception))
        event2_to_reject = {}
        self.service.event = event2_to_reject
        with self.assertRaises(StockServiceException) as context:
            self.service._check_event_fields()
        self.assertTrue('Cannot get event fields' in str(context.exception))

    def test_check_json_format(self):
        self.service.event = '{"transaction_id": "d2a531fa-a417-4983-ab7d-4bb1411c6250", "event_type": "incoming", ' \
                             '"date": "2019-08-07T05:48:12Z", "store_number": 9, "item_number": 12, "value": "400"}'
        self.assertIsNone(self.service._check_json_format())
        self.service.event = {'transaction_id': 'd2a531fa-a417-4983-ab7d-4bb1411c6250',
                              'event_type': 'incoming',
                              'date': '2019-08-07T05:48:12Z',
                              'store_number': 9,
                              'item_number': 12,
                              'value': '400'}
        self.service.event = '<note>That is not a JSON</note>'
        with self.assertRaises(StockServiceException) as context:
            self.service._check_json_format()
        self.assertTrue('Event is not a valid JSON!' in str(context.exception))
        with self.assertRaises(StockServiceException) as context:
            self.service._check_json_format()
        self.assertTrue('Event is not a valid JSON!' in str(context.exception))

    def test_insert_transaction(self):
        transaction_id = 'd2a531fa-a417-4983-ab7d-4bb1411c6250'
        self.service.event = {'transaction_id': transaction_id,
                              'event_type': 'incoming',
                              'date': '2019-08-07T05:48:12Z',
                              'store_number': '4',
                              'item_number': '3',
                              'value': '400'}
        self.service._insert_transaction()
        raw_sql = "SELECT transaction_id from events WHERE transaction_id='{}';".format(transaction_id)
        self.test_service_db.cursor.execute(raw_sql)
        raw_result = self.test_service_db.cursor.fetchone()
        self.assertEqual(raw_result[0], transaction_id)

    def test_get_item(self):
        self.service.event = {'transaction_id': 'd2a531fa-a417-4983-ab7d-4bb1411c6250',
                              'event_type': 'incoming',
                              'date': '2019-08-07T05:48:12Z',
                              'store_number': 9,
                              'item_number': 12,
                              'value': '400'}
        self.service._get_item()
        raw_sql = "SELECT * from stock WHERE item_number={} AND store_number={};".format(
            self.service.event['item_number'],
            self.service.event['store_number']
        )
        self.test_service_db.cursor.execute(raw_sql)
        raw_result = self.test_service_db.cursor.fetchone()
        self.assertEqual(raw_result, self.service.item_in_db)
        self.service.event = {'transaction_id': 'd2a531fa-a417-4983-ab7d-4bb1411c6250',
                              'event_type': 'incoming',
                              'date': '2019-08-07T05:48:12Z',
                              'store_number': 999,
                              'item_number': 999,
                              'value': '400'}
        self.service._get_item()
        self.assertFalse(self.service.item_in_db)

    def test_insert_item(self):
        self.service.event = {'transaction_id': '407f9c78-13a1-4745-a491-84c4bb09468c',
                              'event_type': 'incoming',
                              'date': '2019-06-06T19:14:48Z',
                              'store_number': '2',
                              'item_number': '10',
                              'value': '400'}
        item_values = (int(self.service.event['item_number']),
                       int(self.service.event['store_number']),
                       int(self.service.event['value']),)
        self.service._insert_item()
        raw_sql = "SELECT * from stock WHERE item_number={} AND store_number={};".format(
            self.service.event['item_number'],
            self.service.event['store_number']
        )
        self.test_service_db.cursor.execute(raw_sql)
        raw_result = self.test_service_db.cursor.fetchone()
        self.assertEqual(raw_result[:3], item_values)

    def test_update_item(self):
        update_value = 999
        self.service.event = {'transaction_id': '407f9c78-13a1-4745-a491-84c4bb09468c',
                              'event_type': 'incoming',
                              'date': '2019-06-06T19:14:48Z',
                              'store_number': '9',
                              'item_number': '12',
                              'value': update_value}
        self.service._update_item(update_value)
        raw_sql = "SELECT * from stock WHERE item_number={} AND store_number={};".format(
            self.service.event['item_number'],
            self.service.event['store_number']
        )
        self.test_service_db.cursor.execute(raw_sql)
        raw_result = self.test_service_db.cursor.fetchone()
        self.assertEqual(raw_result[2], update_value)

    def test_set_stock_value(self):
        self.service.event = {'store_number': 9,
                              'item_number': 12,
                              'value': 116}
        self.service._get_item()
        self.service.event = {'transaction_id': '407f9c78-13a1-4745-a491-84c4bb09468c',
                              'event_type': 'incoming',
                              'date': '2019-06-06T19:14:48Z',
                              'store_number': 9,
                              'item_number': 12,
                              'value': 34}
        self.service._set_stock_value()
        raw_sql = "SELECT * from stock WHERE item_number={} AND store_number={};".format(
            self.service.event['item_number'],
            self.service.event['store_number']
        )
        self.test_service_db.cursor.execute(raw_sql)
        raw_result = self.test_service_db.cursor.fetchone()
        self.assertEqual(raw_result[2], 150)

        self.service.event = {'transaction_id': 'cc855c81-7ada-45ed-be6b-4e466df1fad2',
                              'event_type': 'sale',
                              'date': '2019-06-06T19:14:48Z',
                              'store_number': 9,
                              'item_number': 12,
                              'value': 160}
        with self.assertRaises(StockServiceException) as context:
            self.service._set_stock_value()
        self.assertTrue('Cannot run out of stock' in str(context.exception))

    def test_check_item_date(self):
        self.service.event = {'store_number': 9,
                              'item_number': 12,
                              'date': '2018-12-03T23:57:40Z'}
        self.service._get_item()
        self.service.event = {'date': '2019-06-06T19:14:48Z'}
        is_valid_date = self.service._check_item_date()
        self.assertTrue(is_valid_date)
        self.service.event = {'date': '2018-06-06T19:14:48Z'}
        is_valid_date = self.service._check_item_date()
        self.assertFalse(is_valid_date)

    def tearDown(self):
        self.postgresql.stop()
