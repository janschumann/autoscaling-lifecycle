import datetime
import json


class SnsClient(object):

	def __init__(self, client_eu_central, client_eu_west, waiters, logger, topic_arn, account, env):
		self.client_eu_central = client_eu_central
		self.client_eu_west = client_eu_west
		self.waiters = waiters
		self.logger = logger
		self.topic_arn = topic_arn
		self.account = account
		self.env = env


	def publish_autoscaling_activity(self, action, activity, region = "eu-central-1"):
		subject = self.logger.get_formatted_message("A node %s in %s", [action, self.env])
		result = json.dumps(activity, indent = 4, sort_keys = True, ensure_ascii = False,
							default = self.__json_convert)
		message = json.dumps({
			'default': result,
			'sms': subject,
			'email': subject + ":\n\n" + result
		}, indent = 4, sort_keys = True, ensure_ascii = False)
		self.__do_publish(subject, message, region)


	def publish_error(self, exception, action, region = "eu-central-1"):
		subject = self.logger.get_formatted_message(
			'Error while performing %s in environment %s',
			[action, self.env]
		)
		result = subject + "\n\n " + repr(exception)
		message = json.dumps({
			'default': result,
		}, indent = 4, sort_keys = True, ensure_ascii = False)
		self.__do_publish(subject, message, region)


	def __do_publish(self, subject, message, region):
		if self.topic_arn != "":
			if region == "eu-west-1":
				client = self.client_eu_west
			else:
				client = self.client_eu_central
			client.publish(
				TargetArn = self.topic_arn,
				Message = message,
				Subject = subject,
				MessageStructure = 'json'
			)
		else:
			self.logger.warning('Cannot send report. No topic provided.')


	def __json_convert(self, o):
		if isinstance(o, datetime.datetime):
			return o.__str__()
