import datetime
import json
import logging

from botocore.client import BaseClient


class MessageFormatter(object):

    def __init__(self, name: str = ''):
        self.name = name


    def format(self, message: str, args) -> str:
        args = tuple([self.name] + list(self.format_args(args)))
        return ('%s: ' + message) % args


    def format_args(self, args) -> tuple:
        if not args or len(args) == 0:
            return tuple()

        if type(args) is not tuple and type(args) is not list:
            args = [args]

        if type(args) is tuple:
            args = list(args)

        formatted_args = []
        for arg in args:
            if type(arg) is not str:
                try:
                    arg = self.to_str(arg)

                except Exception:
                    arg = repr(arg)

            formatted_args.append(arg)

        return tuple(formatted_args)


    def to_str(self, data):
        return json.dumps(
            data,
            sort_keys = True,
            indent = None,
            ensure_ascii = True,
            default = self.__json_convert
        )


    def __json_convert(self, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()


    def get_error(self, error_type, message: str, *args):
        """
        Returns a error type that can directly be used with raise()

        :type error_type: class
        :param error_type: The error type

        :type message: str
        :param message: The message with placeholders

        :type args: str
        :param args: A list of placeholder values

        :rtype Exception
        :return: The error object
        """
        return error_type(self.format(message, args))


class Logging(object):

    def __init__(self, name, level):
        self.formatter = MessageFormatter(name)
        self.logger = logging.getLogger()
        # remove handlers from root logger
        for h in self.logger.handlers:
            self.logger.removeHandler(h)
        self.logger = logging.getLogger(name)
        # remove handlers from our logger
        for h in self.logger.handlers:
            self.logger.removeHandler(h)
        self.logger.setLevel(level)


    def add_handler(self, ch: logging.Handler, log_format: str):
        formatter = Formatter(log_format)
        formatter.set_formatter(self.formatter)
        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)
        self.logger.addHandler(ch)


    def get_logger(self) -> logging.Logger:
        return self.logger


    def get_formatter(self) -> MessageFormatter:
        return self.formatter


class Formatter(logging.Formatter):
    """
    :type formatter: MessageFormatter
    :param formatter: A message formatter instance
    """
    formatter = None


    def set_formatter(self, formatter: MessageFormatter):
        self.formatter = formatter


    def format(self, record):
        if self.formatter is None:
            raise RuntimeError("No formatter is set")

        record.args = self.formatter.format_args(record.args)

        return super().format(record)


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
