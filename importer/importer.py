import csv
import os

from kafka_client.client import StockKafkaClient, StockKafkaClientException
from .validator import StockRecordValidator

from tools import logger
logger = logger.get_logger(__name__)


class StockImporterException(BaseException):
    pass


class StockImporter:

    DEFAULT_INPUT_DIR = 'samples'
    DEFAULT_FILE_SUFFIX = '.csv'
    DEFAULT_DELIMITER = ','
    DEFAULT_VALIDATOR_CONFIG = {
        'transaction_id': '_check_uuid',
        'event_type': ['incoming', 'sale'],
        'date': '_check_datetime',
        'store_number': '_check_integer',
        'item_number': '_check_integer',
        'value': '_check_integer'}

    def __init__(self):
        self.filename = None
        self.input_dir = self.DEFAULT_INPUT_DIR
        self.file_suffix = self.DEFAULT_FILE_SUFFIX
        self.delimiter = self.DEFAULT_DELIMITER
        self.validator = StockRecordValidator(config=self.DEFAULT_VALIDATOR_CONFIG)
        self.kafka_client = StockKafkaClient()
        self.import_file_path = None
        self.processed_lines = None

    def _set_filepath(self):
        if not self.filename:
            raise StockImporterException('Filename is not set!')
        filepath = os.path.join(self.input_dir, self.filename)
        if not os.path.isfile(filepath):
            raise StockImporterException('No file on path: {}. Check path/filename'.format(filepath))
        self.import_file_path = filepath
        logger.debug('Filepath: {}'.format(self.import_file_path))

    def process(self):
        self._set_filepath()
        logger.info('Processing: {}'.format(self.import_file_path))
        validated_lines = 0
        with open(self.import_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=self.delimiter)
            row_index = 0
            while True:
                try:
                    current_line = next(csv_reader)
                except StopIteration:
                    break
                except UnicodeDecodeError:
                    raise StockImporterException('Cannot process file: {}'.format(self.import_file_path))
                if self.validator.validate(current_line):
                    validated_line = {name: value for name, value in current_line.items()}
                    logger.debug('validated_line: {}'.format(validated_line))
                    validated_lines += 1
                    try:
                        self.kafka_client.send_event(validated_line)
                    except StockKafkaClientException as exc:
                        logger.critical(exc)
                        break
                row_index += 1
            self.processed_lines = row_index
            logger.info('Validated {} lines.'.format(validated_lines))
            logger.info('Processed {} lines.'.format(self.processed_lines))

    def get_files(self):
        filenames = os.listdir(self.input_dir)
        return [filename for filename in filenames if filename.endswith(self.file_suffix)]

    def process_all(self):
        for filename in self.get_files():
            self.filename = filename
            self.process()
