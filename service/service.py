import datetime
import json

from kafka_client.client import StockKafkaClient, StockKafkaClientException
from database.database import StockServiceDB, StockServiceDBException

from tools import logger
logger = logger.get_logger(__name__)


class StockServiceException(BaseException):
    pass


class StockService:

    DEFAULT_EVENT_FIELDS = ['transaction_id',
                            'event_type',
                            'date',
                            'store_number',
                            'item_number',
                            'value']

    def __init__(self, database=None, kafka_client=None):
        self.kafka_client = kafka_client if kafka_client else StockKafkaClient()
        self.database = database if database else StockServiceDB()
        self.event = None
        self.event_fields = self.DEFAULT_EVENT_FIELDS
        self.item_in_db = None
        self.updated_item_count = 0

    def _insert_transaction(self):
        self.database.table = 'events'
        self.database.fields = ['transaction_id',
                                'event_type',
                                'date',
                                'store_number',
                                'item_number',
                                'value']
        self.database.values = [self.event['transaction_id'],
                                self.event['event_type'],
                                self.event['date'],
                                self.event['store_number'],
                                self.event['item_number'],
                                self.event['value']]
        self.database.insert()

    def _get_item(self):
        self.database.table = 'stock'
        self.database.fields = ['item_number',
                                'store_number',
                                'current_value',
                                'last_update']
        self.database.where = "item_number={} AND store_number={}".format(self.event['item_number'],
                                                                          self.event['store_number'])
        self.database.query()
        logger.debug(self.database.query_result)
        self.item_in_db = self.database.query_result

    def _insert_item(self):
        if self.event['event_type'] != 'incoming':
            return
        self.database.table = 'stock'
        self.database.fields = ['item_number',
                                'store_number',
                                'current_value',
                                'last_update']
        self.database.values = [self.event['item_number'],
                                self.event['store_number'],
                                self.event['value'],
                                self.event['date']
                                ]
        self.database.insert()

    def _update_item(self, value):
        self.database.table = 'stock'
        self.database.fields = ['current_value']
        self.database.values = [value]
        self.database.where = "item_number={} AND store_number={}".format(self.event['item_number'],
                                                                          self.event['store_number'])
        self.database.update()
        # Set new date
        self.database.fields = ['last_update']
        self.database.values = [self.event['date']]
        self.database.update()

    def _check_item_date(self):
        item_date_in_db = self.item_in_db[3]
        event_date = datetime.datetime.strptime(self.event['date'], '%Y-%m-%dT%H:%M:%SZ')
        logger.debug('item_date_in_db: {}; event_date: {}'.format(item_date_in_db, event_date))
        if item_date_in_db > event_date:
            logger.warning('Cannot process event. Event date is before last update of the item!')
            return False
        return True

    def _set_stock_value(self):
        item_stock_value = self.item_in_db[2]
        event_stock_value = int(self.event['value'])
        if self.event['event_type'] == 'incoming':
            item_stock_value += event_stock_value
        elif self.event['event_type'] == 'sale':
            if (item_stock_value - event_stock_value) < 0:
                raise StockServiceException('Cannot run out of stock! Transaction ID: {}'.format(
                    self.event['transaction_id']))
            item_stock_value -= event_stock_value
        self._update_item(item_stock_value)

    def _check_event_fields(self):
        current_event_fields = self.event.keys()
        if not current_event_fields:
            raise StockServiceException('Cannot get event fields!')
        if list(current_event_fields) != self.event_fields:
            raise StockServiceException('Event fields are different. Event ID: {}'.format(self.event['transaction_id']))

    def _check_json_format(self):
        try:
            self.event = json.loads(self.event)
        except (json.decoder.JSONDecodeError, TypeError, ):
            raise StockServiceException('Event is not a valid JSON!')

    def process_event(self):
        try:
            self._check_json_format()
        except StockServiceException as exc:
            logger.warning(exc)
            return
        try:
            self._check_event_fields()
        except StockServiceException as exc:
            logger.warning(exc)
            return
        logger.debug('Processing event: {}'.format(self.event))
        try:
            self._insert_transaction()
        except StockServiceDBException as exc:
            logger.warning(exc)
            return
        self._get_item()
        if not self.item_in_db:
            self._insert_item()
            self.updated_item_count += 1
            return
        if not self._check_item_date():
            return
        try:
            self._set_stock_value()
        except StockServiceException as exc:
            logger.error(exc)
            return
        self.updated_item_count += 1

    def get_events(self):
        try:
            self.kafka_client.get_consumer()
        except StockKafkaClientException as exc:
            logger.critical(exc)
            return
        for message in self.kafka_client.consumer:
            self.event = message.value.decode('utf-8')
            logger.debug('self.event: {}'.format(self.event))
            self.process_event()
