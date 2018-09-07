class SsmClient(object):

	def __init__(self, client, waiters, logger):
		self.client = client
		self.waiters = waiters
		self.logger = logger


	def send_command(self, instance_id, comment, commands):
		self.logger.debug('Sending command "%s" to instance %s: %s', comment, instance_id, commands)

		self.logger.debug('Waiting for ssm agent to become ready.')
		# can be replaced when https://github.com/boto/botocore/pull/1502 will be accepted
		# waiter = ssm.get_waiter['AgentIsOnline']
		self.waiters.get('AgentIsOnline').wait(
			Filters = [{ 'Key': 'InstanceIds', 'Values': [instance_id] }]
		)

		command_id = self.client.send_command(
			InstanceIds = [instance_id],
			DocumentName = 'AWS-RunShellScript',
			Comment = self.logger.get_name() + ' : ' + comment,
			Parameters = {
				'commands': commands
			}
		).get('Command').get('CommandId')
		self.logger.debug('Command "%s" on instance %s is running: %s', comment, instance_id, command_id)

		return command_id
