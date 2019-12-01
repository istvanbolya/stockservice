from unittest import TestCase

from service.service import StockService, StockServiceException


class StockImporterTest(TestCase):

    def setUp(self):
        self.service = StockService()

    def test_check_event_format(self):
        acceptable_event = {'transaction_id': '407f9c78-13a1-4745-a491-84c4bb09468c',
                            'event_type': 'incoming',
                            'date': '2019-06-06T19:14:48Z',
                            'store_number': '2',
                            'item_number': '10',
                            'value': '400'}
        self.service.event = acceptable_event
        self.assertIsNone(self.service._check_event_format())
        event1_to_reject = {'transaction_id': '8947695b-7f19-44b6-96b6-7f8ed041fe57'}
        self.service.event = event1_to_reject
        with self.assertRaises(StockServiceException) as context:
            self.service._check_event_format()
        self.assertTrue('Event fields are different' in str(context.exception))
        event2_to_reject = {}
        self.service.event = event2_to_reject
        with self.assertRaises(StockServiceException) as context:
            self.service._check_event_format()
        self.assertTrue('Cannot get event fields' in str(context.exception))

    def test_get_events(self):
        pass
