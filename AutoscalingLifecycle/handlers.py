import json
import logging

from botocore.client import BaseClient


class SnsHandler(logging.Handler):
    """
    A handler class which writes formatted logging records to sns.
    """


    def __init__(self, sns_client: BaseClient, arn):
        self.sns_client = sns_client
        self.arn = arn
        super().__init__()


    def emit(self, record):
        log_entry = self.format(record)

        message = json.dumps({
            'default': log_entry,
            'sms': log_entry,
            'email': log_entry
        }, indent = 4, sort_keys = True, ensure_ascii = False)

        return self.sns_client.publish(
            TargetArn = self.arn,
            Message = message,
            MessageStructure = 'json'
        )
