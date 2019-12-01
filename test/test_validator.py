from unittest import TestCase

from importer.validator import StockRecordValidator, StockValidatorException


class StockImporterTest(TestCase):

    def setUp(self):
        simple_config = {'transaction_id': '_check_uuid'}
        self.validator = StockRecordValidator(config=simple_config)

    def test_check_uuid(self):
        uuid = '7c71fb42-1f5e-45e1-be16-7d4d772d1aab'
        self.assertIsNone(self.validator._check_uuid(uuid))
        uuid_bad = 'notanuuid'
        with self.assertRaises(StockValidatorException) as context:
            self.validator._check_uuid(uuid_bad)
        self.assertTrue('Not a valid UUID' in str(context.exception))

    def test_check_datetime(self):
        datetime_str = '2018-12-03T23:57:40Z'
        self.assertIsNone(self.validator._check_datetime(datetime_str))
        datetime_str_bad = 'notadatetime'
        with self.assertRaises(StockValidatorException) as context:
            self.validator._check_datetime(datetime_str_bad)
        self.assertTrue('Not a valid datetime string' in str(context.exception))

    def test_check_integer(self):
        integer_str = '23'
        self.assertIsNone(self.validator._check_integer(integer_str))
        integer_str_bad = 'notanint'
        with self.assertRaises(StockValidatorException) as context:
            self.validator._check_integer(integer_str_bad)
        self.assertTrue('Not a valid integer' in str(context.exception))

    def test_check_line_length(self):
        line_len = 6
        self.assertIsNone(self.validator._check_line_length(line_len))
        line_len_bad = 5
        with self.assertRaises(StockValidatorException) as context:
            self.validator._check_line_length(line_len_bad)
        self.assertTrue('Not a proper line length' in str(context.exception))

    def test_validate(self):
        self.validator.line_length = 1
        valid_record = {"transaction_id": "7a21e465-3ae3-4546-b2fa-e87812e4018c"}
        self.assertTrue(self.validator.validate(valid_record))
        invalid_record = {"transaction_id": "again_not_an_uuid"}
        self.assertFalse(self.validator.validate(invalid_record))
