from logging import Logger

from boto3 import Session

from AutoscalingLifecycle.client.autoscaling import AutoscalingClient
from AutoscalingLifecycle.client.dynamodb import DynamoDbClient
from AutoscalingLifecycle.client.factory import ClientFactory
from AutoscalingLifecycle.client.route53 import Route53Client
from AutoscalingLifecycle.client.ssm import SsmClient
from AutoscalingLifecycle.client.sns import SnsClient
from AutoscalingLifecycle.helper.logger import LifecycleLogger
from AutoscalingLifecycle.helper.waiters import Waiters
from AutoscalingLifecycle.repository.command import CommandRepository
from AutoscalingLifecycle.repository.node import NodeRepository


class EventAction(object):
	"""
	Abstract class to respond to a cloudwatch event

	This class handles:
	- load event data
	- prepare clients and waiters
	- call ssm scripts


	:type logger: LifecycleLogger
	:param logger: A logger instance
	:type event: dict
	:param event: The event to care about
	:type event_details: dict
	:param event_details: The event details
	:type mandatory_event_keys: list
	:param mandatory_event_keys: a list of items needed to process an event
	"""
	logger = None
	session = None
	event = { }
	event_details = { }
	mandatory_event_keys = [
		'id',
		'detail-type',
		'source',
		'account',
		'time',
		'region',
		'resources',
		'detail'
	]


	def __init__(self, name: str, event: dict, session: Session, logger: Logger, notification_arn, account, env):
		"""
		Create a new action

		:type name: str
		:param name: A name to identify this action. Will be used in log statements and errors
		:type event: dict
		:param event: A dictionary representing a cloudwatch event. Example:
		{
			"version": "0",
			"id": "6a7e8feb-b491-4cf7-a9f1-bf3703467718",
			"detail-type": "EC2 Instance-launch Lifecycle Action",
			"source": "aws.autoscaling",
			"account": "123456789012",
			"time": "2015-12-22T18:43:48Z",
			"region": "eu-central-1",
			"resources": [
				"arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:59fcbb81-bd02-485d-80ce-563ef5b237bf:autoScalingGroupName/sampleASG"
			],
			"detail": {
				"LifecycleActionToken": "c613620e-07e2-4ed2-a9e2-ef8258911ade",
				"AutoScalingGroupName": "sampleASG",
				"LifecycleHookName": "SampleLifecycleHook-12345",
				"EC2InstanceId": "i-0c683c9448b801b8b",
				"LifecycleTransition": "autoscaling:EC2_INSTANCE_LAUNCHING",
				"NotificationMetadata": {}
			}
		}

		OR

		{
			"version": "0",
			"id": "51c0891d-0e34-45b1-83d6-95db273d1602",
			"detail-type": "EC2 Command Status-change Notification",
			"source": "aws.ssm",
			"account": "123456789012",
			"time": "2016-07-10T21:51:32Z",
			"region": "us-east-1",
			"resources": [
				"arn:aws:ec2:us-east-1:123456789012:instance/i-abcd1111",
				"arn:aws:ec2:us-east-1:123456789012:instance/i-abcd2222"
			],
			"detail": {
				"command-id": "e8d3c0e4-71f7-4491-898f-c9b35bee5f3b",
				"document-name": "AWS-RunPowerShellScript",
				"expire-after": "2016-07-14T22:01:30.049Z",
				"parameters": {
					"executionTimeout": [
						"3600"
					],
					"commands": [
						"date"
					]
				},
				"requested-date-time": "2016-07-10T21:51:30.049Z",
				"status": "Cancelled"
			}
		}


		:type session: Session
		:param session: A boto3 session object.
		:type logger: Logger
		:param logger: A logger instance
		"""

		log_name = name + '::' + account + "::" + env
		self.logger = LifecycleLogger(name = log_name.upper(), logger = logger)
		self.__create_clients(name, session, notification_arn, account, env)
		self._populate_event_data(event)


	def __CALL__(self):
		"""
		Executes the action. Needs to be implemented by concrete implementations
		"""
		raise NotImplementedError()


	def get_action_info(self) -> str:
		"""
		:rtype: str
		:return: Get a string describing the current action
		"""

		return self.logger.get_formatted_message(' %s :: %s', [self.event.get('source'), self.event.get('detail-type')])


	def _populate_event_data(self, event: dict):
		"""
		Populates event data. Implementers can override this to load other or additional data
		"""
		self.event = event
		self.logger.debug('event data: %s', self.event)

		for r in self.mandatory_event_keys:
			if r not in self.event:
				raise self.logger.get_error(TypeError,
											'Validation error. The event does not seem to be a cloudwatch event.')

		self.event_details = self.event.get('detail')

		self.logger.debug('raw event detail: %s', self.event_details)


	def call_ssm_script(self, instance_id: str, comment: str, commands: list, metadata: dict):
		"""
		Initiate a ssm run command of type AWS-RunShellScript.
		We will also store the command metadata to the state.

		:type instance_id: str
		:param instance_id: The instance id to call the script on

		:type comment: str
		:param comment: The comment to display

		:type commands: list
		:param commands: A list of commands to execute

		:type metadata: dict
		:param metadata: A dictionary of metadata to store alongside with the command
		"""

		metadata.update({ 'RunningOn': instance_id })
		metadata.update({ 'Comment': comment })
		metadata.update({ 'Commands': ','.join(commands) })

		try:
			command_id = self.ssm_client.send_command(instance_id, comment, commands)
			self.command_repository.register(command_id, metadata)
		except Exception as e:
			self.ssm_client.send_command(instance_id, 'ABANDON NODE due to an error. See logs for details.', ['exit 1'])
			raise self.logger.get_error(RuntimeError,
										'Could not send command %s. Node will be abandoned. Error was: %s',
										comment, repr(e))


	def __create_clients(self, name: str, session: Session, notification_arn, account, env):
		self.logger.debug('Creating clients ...')
		client_factory = ClientFactory(session = session, logger = self.logger)
		waiters = Waiters(clients = client_factory, logger = self.logger)
		self.dynamodb_client = DynamoDbClient(
			client = client_factory.get('dynamodb'),
			state_table = name.lower() + '-state',
			logger = self.logger,
			waiters = waiters
		)
		self.node_repository = NodeRepository(self.dynamodb_client, self.logger)
		self.command_repository = CommandRepository(self.dynamodb_client, self.logger)
		self.ssm_client = SsmClient(client_factory.get('ssm'), waiters, self.logger)
		self.autoscaling_client = AutoscalingClient(client_factory.get('autoscaling'), waiters, self.logger)
		self.route53_client = Route53Client(client_factory.get('route53'), waiters, self.logger)
		self.sns = SnsClient(
			client_factory.get('sns'),
			client_factory.get('sns', 'eu-west-1'),
			waiters,
			self.logger,
			notification_arn,
			account,
			env
		)


	def report_activity(self, action, group, instance_id):
		activity = self.autoscaling_client.get_autoscaling_activity(group, instance_id)
		self.logger.info('Reporting activity: node %s: %s', action, activity)
		self.sns.publish(action, activity, 'eu-west-1')
