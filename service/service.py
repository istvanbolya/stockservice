import csv
import datetime
import json
import os

from kafka_client.client import StockKafkaClient, StockKafkaClientException
from database.database import StockServiceDB, StockServiceDBException

from tools import logger
logger = logger.get_logger(__name__)


class StockServiceException(BaseException):
    pass


class StockService:

    def __init__(self):
        self.kafka_client = StockKafkaClient()
        self.db = StockServiceDB()
        self.event = None
        self.item_in_db = None
        self.updated_item_count = 0

    def _insert_transaction(self):
        self.db.table = 'events'
        self.db.fields = ['transaction_id',
                          'event_type',
                          'date',
                          'store_number',
                          'item_number',
                          'value']
        self.db.values = [self.event['transaction_id'],
                          self.event['event_type'],
                          self.event['date'],
                          self.event['store_number'],
                          self.event['item_number'],
                          self.event['value']]
        self.db.insert()

    def _get_item(self):
        self.db.table = 'stock'
        self.db.fields = ['item_number',
                          'store_number',
                          'current_value',
                          'last_update']
        self.db.where = "item_number={} AND store_number={}".format(self.event['item_number'],
                                                                    self.event['store_number'])
        self.db.query()
        logger.debug(self.db.query_result)
        self.item_in_db = self.db.query_result

    def _insert_item(self):
        if self.event['event_type'] != 'incoming':
            return
        self.db.table = 'stock'
        self.db.fields = ['item_number',
                          'store_number',
                          'current_value',
                          'last_update']
        self.db.values = [self.event['item_number'],
                          self.event['store_number'],
                          self.event['value'],
                          self.event['date']
                          ]
        self.db.insert()

    def _check_item_date(self):
        item_date_in_db = self.item_in_db[3]
        event_date = datetime.datetime.strptime(self.event['date'], '%Y-%m-%dT%H:%M:%SZ')
        logger.debug('item_date_in_db: {}; event_date: {}'.format(item_date_in_db, event_date))
        if item_date_in_db > event_date:
            logger.warning('Cannot process event. Event date is before last update of the item!')
            return False
        return True

    def _update_item(self, value):
        self.db.table = 'stock'
        self.db.fields = ['current_value']
        self.db.values = [value]
        self.db.where = "item_number={} AND store_number={}".format(self.event['item_number'],
                                                                    self.event['store_number'])
        self.db.update()
        # Set new date
        self.db.fields = ['last_update']
        self.db.values = [self.event['date']]
        self.db.update()

    def _set_stock_value(self):
        item_stock_value = self.item_in_db[2]
        event_stock_value = int(self.event['value'])
        if self.event['event_type'] == 'incoming':
            item_stock_value += event_stock_value
        elif self.event['event_type'] == 'sale':
            if (item_stock_value - event_stock_value) < 0:
                logger.error('Cannot run out of stock! Transaction ID: {}'.format(self.event['transaction_id']))
                return
            item_stock_value -= event_stock_value
        self._update_item(item_stock_value)

    def process_event(self):
        try:
            self.event = json.loads(self.event)
        except json.decoder.JSONDecodeError:
            logger.warning('Event "{}" is not a valid JSON!'.format(self.event))
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
        self._set_stock_value()
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
