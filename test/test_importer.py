from unittest import TestCase, mock

from importer.importer import StockImporter, StockImporterException


class StockImporterTest(TestCase):

    def setUp(self):
        self.importer = StockImporter()

    def test_set_filepath(self):
        self.importer.filename = 'csv_all_incoming.csv'
        self.importer._set_filepath()
        self.assertEqual('samples/csv_all_incoming.csv', self.importer.import_file_path)
        self.importer.filename = 'not_a_file'
        with self.assertRaises(StockImporterException) as context:
            self.importer._set_filepath()
        self.assertTrue('No file on path' in str(context.exception))
        self.importer.filename = None
        with self.assertRaises(StockImporterException) as context:
            self.importer._set_filepath()
        self.assertTrue('Filename is not set' in str(context.exception))

    @mock.patch("kafka_client.client.StockKafkaClient")
    @mock.patch("importer.validator.StockRecordValidator")
    def test_process(self, mock_kafka_client, mock_validator):
        mock_kafka_client.return_value.send_event.return_value = 'SENT'
        self.importer.kafka_client = mock_kafka_client
        mock_validator.return_value.validate.return_value = True
        self.importer.validator = mock_validator
        # Valid file
        self.importer.filename = 'csv_all_incoming.csv'
        self.importer._set_filepath()
        self.importer.process()
        self.assertEqual(self.importer.processed_lines, 1000)
        # Invalid file
        self.importer.filename = 'csv_all_incoming.zip'
        self.importer._set_filepath()
        with self.assertRaises(StockImporterException) as context:
            self.importer.process()
        self.assertTrue('Cannot process file' in str(context.exception))

    def test_get_files(self):
        file_list = ['csv_all_incoming.csv',
                     'csv_sample_1_mod.csv',
                     'csv_sample_3.csv',
                     'csv_sample_1.csv',
                     'all_csv_merged.csv',
                     'csv_sample_2.csv']
        self.assertEqual(self.importer.get_files(), file_list)
