import logging

from .formatter import MessageFormatter


class LoggerFactory(object):

    def __init__(self, name, level):
        self.formatter = MessageFormatter(name)
        self.logger = logging.getLogger(name)
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

        original_msg = record.msg
        original_args = record.args

        record.msg = '%s: ' + record.msg
        record.args = self.formatter.format_args(record.name, record.args)

        s = super().format(record)

        record.msg = original_msg
        record.args = original_args

        return s
