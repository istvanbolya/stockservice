from pykafka.client import KafkaClient
from pykafka.protocol.message import Message
from pykafka.simpleconsumer import SimpleConsumer
from unittest import TestCase
import os

from kafka_client.client import StockKafkaClient, StockKafkaClientException


class StockKafkaClientTest(TestCase):

    def setUp(self):
        self.kafka = StockKafkaClient()

    # TODO: Honestly, I'm confused here, I don't know, how to properly test services. Inside a Docker img? Mocking?
    #  In my understanding, service availability is a different thing, but, ofc, I use codes depending ot that.
    #  Anyway, I'll keep my local tests here.

    def test_kafka_running(self):
        status = os.system('systemctl status kafka > /dev/null')
        self.assertEqual(status, 0)

    def test_set_host(self):
        # FYI: This test is for local dev. env with default config! TODO: enviroment check/switch
        self.kafka.host = '127.0.0.1:9092'
        self.kafka._set_host()
        self.assertIs(type(self.kafka.client), KafkaClient)

        # Invalid host
        self.kafka.host = '127.0.0.1:9999'
        self.kafka._set_host()
        self.assertIsNone(self.kafka.client)

    def test_convert_for_sending(self):
        dummy_data = {"transaction_id": "8947695b-7f19-44b6-96b6-7f8ed041fe57"}
        converted_data = b'{"transaction_id": "8947695b-7f19-44b6-96b6-7f8ed041fe57"}'
        self.assertEqual(self.kafka._convert_for_sending(dummy_data), converted_data)

    def test_send_event(self):
        dummy_event = {"transaction_id": "8947695b-7f19-44b6-96b6-7f8ed041fe57"}
        # Send OK
        self.kafka.host = '127.0.0.1:9092'
        self.kafka._set_host()
        self.kafka.send_event(dummy_event)
        self.assertIs(type(self.kafka.response), Message)
        # No host
        self.kafka.host = '127.0.0.1:9999'
        self.kafka._set_host()
        with self.assertRaises(StockKafkaClientException) as context:
            self.kafka.send_event(dummy_event)
        self.assertTrue('No broker available!' in str(context.exception))

    def test_get_consumer(self):
        self.kafka.host = '127.0.0.1:9092'
        self.kafka._set_host()
        self.kafka.get_consumer()
        self.assertIs(type(self.kafka.consumer), SimpleConsumer)
        # No host
        self.kafka.host = '127.0.0.1:9999'
        self.kafka._set_host()
        with self.assertRaises(StockKafkaClientException) as context:
            self.kafka.get_consumer()
        self.assertTrue('No broker available!' in str(context.exception))
