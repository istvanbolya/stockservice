from dateutil.parser import parse as parse_date
from uuid import UUID

from tools import logger
logger = logger.get_logger(__name__)


class StockValidatorException(BaseException):
    pass


class StockRecordValidator:

    DEFAULT_RECORD_LINE_LENGTH = 6

    def __init__(self, config):
        self.config = config
        self.line_length = self.DEFAULT_RECORD_LINE_LENGTH

    @staticmethod
    def _check_uuid(uuid_string):
        try:
            UUID(uuid_string, version=4)
        except ValueError:
            raise StockValidatorException('Not a valid UUID: "{}"'.format(uuid_string))

    @staticmethod
    def _check_datetime(datetime_string):
        try:
            parse_date(datetime_string)
        except ValueError:
            raise StockValidatorException('Not a valid datetime string: "{}"'.format(datetime_string))

    @staticmethod
    def _check_integer(int_string):
        try:
            int(int_string)
        except ValueError:
            raise StockValidatorException('Not a valid integer: "{}"'.format(int_string))

    def _check_line_length(self, line_length):
        if line_length != self.line_length:
            raise StockValidatorException('Not a proper line length ! Current {}'.format(line_length))

    def validate(self, record_line):
        try:
            self._check_line_length(len(record_line))
        except StockValidatorException as exc:
            logger.error(exc)
            return False

        is_valid = True
        for column_name, value in record_line.items():
            if not value:
                logger.error('No value for {}'.format(column_name))
                is_valid = False
                break
            check_type = self.config.get(column_name, None)
            logger.debug('check_type: {}; value: {}'.format(check_type, value))
            if isinstance(check_type, str):
                try:
                    _check_method = getattr(self, check_type)
                except AttributeError:
                    logger.error('Unknown validation parameter: "{}". Check config!'.format(check_type))
                    is_valid = False
                    break
                try:
                    _check_method(value)
                except StockValidatorException as exc:
                    logger.error(exc)
                    is_valid = False
                    break
            elif isinstance(check_type, list):
                if value.lower() not in check_type:
                    logger.error('"{}" is not a valid choice from: "{}"'.format(value, check_type))
                    is_valid = False
                    break
            else:
                logger.error('Unknown validation type: "{}". Check config!'.format(check_type))
                is_valid = False
                break
        return is_valid
