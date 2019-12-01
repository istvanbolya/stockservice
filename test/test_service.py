from unittest import TestCase

from service.service import StockService


class StockImporterTest(TestCase):

    def setUp(self):
        self.importer = StockService()

    def test_get_events(self):
        pass
