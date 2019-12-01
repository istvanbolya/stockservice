from unittest import TestCase

from database.database import StockServiceDB


class StockServiceDBTest(TestCase):

    def setUp(self):
        self.db = StockServiceDB()
