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


	def publish(self, action, activity, region = "eu-central-1"):
		if self.topic_arn != "":
			result = json.dumps(activity, indent=4, sort_keys=True, ensure_ascii=False)
			subject = self.logger.get_formatted_message("A node %s in %s", [action, self.env])
			message = json.dumps({
				'default': result,
				'sms': subject,
				'email': subject + ":\n\n" + result
			}, indent=4, sort_keys=True, ensure_ascii=False)
			if region == "eu-west-1":
				client = self.client_eu_west
			else:
				client = self.client_eu_central
			client.publish(
				TargetArn=self.topic_arn,
				Message=message,
				Subject=subject,
				MessageStructure='json'
			)

