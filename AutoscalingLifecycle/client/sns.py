class SnsClient(object):

	def __init__(self, client, waiters, logger, topic_arn):
		self.client = client
		self.waiters = waiters
		self.logger = logger
		self.topic_arn = topic_arn


	def publish(self, message):
		if self.topic_arn != "":
			self.client.publish(
				TargetArn=self.topic_arn,
				Message=message
			)
