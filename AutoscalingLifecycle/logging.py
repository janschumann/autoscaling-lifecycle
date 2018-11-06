import json
from logging import Formatter as BaseFormatter
from logging import Handler

from . import MessageFormatter
from .clients import SnsClient


class Formatter(BaseFormatter):

    def __init__(self, fmt = None, datefmt = None, style = '%'):
        self.message_formatter = MessageFormatter()
        super().__init__(fmt, datefmt, style)


    def format(self, record):
        record.msg = '%s: ' + record.msg
        record.args = self.message_formatter.format_args(record.name, record.args)

        return super().format(record)


class SnsHandler(Handler):
    """
    A handler class which writes formatted logging records to sns.
    """


    def __init__(self, sns_client: SnsClient, region):
        self.sns_client = sns_client
        self.region = region
        super().__init__()


    def emit(self, record):
        log_entry = self.format(record)

        message = json.dumps({
            'default': log_entry,
            'sms': log_entry,
            'email': log_entry
        }, indent = 4, sort_keys = True, ensure_ascii = False)

        return self.sns_client.publish(log_entry, message, self.region)
