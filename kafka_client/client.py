import json
from pykafka import KafkaClient
from pykafka.exceptions import NoBrokersAvailableError


class StockKafkaClientException(BaseException):
    pass


class StockKafkaClient:

    DEFAULT_TOPIC = 'events'
    DEFAULT_KAFKA_HOST = "127.0.0.1:9092"

    def __init__(self):
        self.host = self.DEFAULT_KAFKA_HOST
        self.client = None
        self.topic = self.DEFAULT_TOPIC
        self.response = None
        self.consumer = None
        self._set_host()

    def _set_host(self):
        try:
            self.client = KafkaClient(hosts=self.host)
        except NoBrokersAvailableError:
            self.client = None

    @staticmethod
    def _convert_for_sending(event):
        return bytes(json.dumps(event), encoding='utf-8')

    def send_event(self, event):
        if not self.client:
            raise StockKafkaClientException('No broker available!')
        topic = self.client.topics[self.topic]
        with topic.get_producer() as producer:
            self.response = producer.produce(self._convert_for_sending(event))

    def get_consumer(self):
        if not self.client:
            raise StockKafkaClientException('No broker available!')
        topic = self.client.topics[self.topic]
        self.consumer = topic.get_simple_consumer()
