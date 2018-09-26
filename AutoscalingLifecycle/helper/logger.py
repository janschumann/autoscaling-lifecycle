import json
from logging import DEBUG
from logging import Logger


class LifecycleLogger(object):
	"""
	Proxy for info/error/warning etc calls to logger
	The message is prefixed with an lifecycle name
	and the data is ensured to be string by json.dumps()
	As we need to prefix error messages with the name prefix
	there is also a method get_error, which creates an error type and
	adds the prefix to the message
	"""


	def __init__(self, name: str, logger: Logger):
		"""

		:type name: str
		:param name:
		:type logger: Logger
		:param logger:
		"""
		self.name = name
		self.logger = logger


	def set_name(self, name):
		self.name = name


	def get_name(self):
		return self.name


	def set_debug(self):
		self.logger.setLevel(DEBUG)


	def info(self, message: str, *args):
		self.logger.info(self.get_formatted_message(message, args))


	def error(self, message: str, *args):
		self.logger.error(self.get_formatted_message(message, args))


	def warning(self, message: str, *args):
		self.logger.warning(self.get_formatted_message(message, args))


	def debug(self, message: str, *args):
		self.logger.debug(self.get_formatted_message(message, args))


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
		return error_type(self.get_formatted_message(message, args))


	def get_formatted_message(self, message: str, args) -> str:
		the_args = [self.name]

		args = list(args)
		for arg in args:
			if type(arg) is not str:
				arg = json.dumps(arg, ensure_ascii = False)

			the_args.append(arg)

		return ('%s: ' + message) % tuple(the_args)
