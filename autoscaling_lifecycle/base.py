import json
import logging

import boto3
import botocore.client

from autoscaling_lifecycle.waiters import Waiters


class EventAction(object):
	"""
	Abstract class to respond to a cloudwatch event

	:type name: str
	:type clients: dict
	:type is_debug: bool
	:type logger: logging.Logger
	:type session: boto3.Session
	:type event: dict
	:type event_details: dict
	:type mandatory_event_keys: dict
	:type transition: str
	"""
	name = None
	clients = {}
	is_debug = False
	logger = None
	session = None
	event = {}
	event_details = {}
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
	transition = None


	def __init__(self, name: str, event: dict, session: boto3.Session, logger: logging.Logger):
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


		:type session: boto3.Session
		:param session: A boto3 session object.
		:type logger: logging.Logger
		:param logger: A logger instance
		"""

		self.name = name

		self.logger = logger
		self.session = session
		self.waiters = Waiters(self)

		self.event = event
		self.__validate_event()
		self.event_details = self.event.get('detail')

		self.debug('event data: %s', json.dumps(self.event, ensure_ascii=False))
		self.debug('event details: %s', json.dumps(self.event_details, ensure_ascii=False))

		self.load_event_specific_data()


	def __validate_event(self):
		"""
		Validate the event. Will raise an error on failure
		"""
		for r in self.mandatory_event_keys:
			if r not in self.event:
				raise self.get_error(TypeError, 'Validation error. The event does not seem to be a cloudwatch event.')


	def load_event_specific_data(self):
		"""
		Give specific actions the ability to load additional data. Does nothing by default
		"""


	def __call__(self):
		"""
		Executes the action. Needs to be implemented by concrete implementations
		"""
		raise NotImplementedError()


	def get_node_type(self) -> str:
		"""
		:rtype: str
		:return: The type of the current node
		"""

		return ''


	def get_name(self) -> str:
		"""
		:rtype: str
		:return: Returns the name of this action.
		"""

		return self.name.upper()


	def get_action_info(self) -> str:
		"""
		:rtype: str
		:return: Get a string describing the current action
		"""

		return self.get_name() + '::' + self.event.get('source') + '::' + self.event.get('detail-type')


	def get_client(self, name: str):
		"""
		Get a boto client. Clients will be cached locally.
		E.g. get_client('ssm') will return boto3.client('ssm')

		:type name: str
		:param name: The name of the client to create

		:rtype: botocore.client.BaseClient
		:return: Service client instance
		"""
		client = self.clients.get(name, None)
		if client is None:
			self.debug('Client %s not created. Creating ...', name)
			client = self.session.client(name)
			self.clients.update({name: client})

		return client


	def get_state_table(self) -> str:
		"""
		:rtype: str
		:return: the name of the dynamodb state table for this action
		"""

		return self.get_name().lower() + '-state'


	def is_launching(self) -> bool:
		"""
		:rtype: bool
		:return: Whether we react on a launch event
		"""

		return self.transition == 'autoscaling:EC2_INSTANCE_LAUNCHING'


	def is_terminating(self):
		"""
		:rtype: bool
		:return: Whether we react on a terminate event
		"""

		return self.transition == 'autoscaling:EC2_INSTANCE_TERMINATING'


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
		self.info('Calling "%s" on "%s"', comment, instance_id)
		self.__wait_for_ssm_agent_to_become_ready(instance_id)
		command_id = self.get_client('ssm').send_command(
			InstanceIds=[instance_id],
			DocumentName='AWS-RunShellScript',
			Comment=comment,
			Parameters={
				'commands': commands
			}
		).get('Command').get('CommandId')

		item = self.build_dynamodb_item(command_id, 'command', metadata)
		self.info('Storing command %s with data: %s', comment, json.dumps(item, ensure_ascii=False))
		_ = self.get_client('dynamodb').put_item(
			TableName=self.get_state_table(),
			Item=item
		)


	def build_dynamodb_item(self, id: str, type: str, data: dict) -> dict:
		"""
		Build a node item to be used with put_item()

		:type id: str
		:param id: The identifier for the item

		:type type: str
		:param type: The type of the item

		:type data: dict
		:param data: The item data

		:rtype: dict
		:return: The item
		"""

		data.update({'ItemType': type})

		item = self.convert_dict_to_dynamodb_map(data)
		item.update(self.build_dynamodb_key(id))

		return item


	def build_dynamodb_key(self, id):
		"""

		:param id:
		:return:
		"""

		return {'Ident': {'S': id}}


	def build_dynamodb_value(self, value, log=True):
		"""

		:param id:
		:return:
		"""

		if type(value) is str:
			return {'S': value}

		elif type(value) is dict:
			return {'M': self.convert_dict_to_dynamodb_map(value, log)}

		else:
			self.logger.warning('Cannot convert type %s to a dynamodb equivalent. Value will be empty. Valid types are str, dict. Value: %s', type(value), json.dumps(value, ensure_ascii=False))

		return {'S': ''}


	def convert_dict_to_dynamodb_map(self, data: dict, log=True) -> dict:
		"""
		Convert a dict to a dynamodb map. Valid types:
		- str -> 'S'
		- dict -> 'M'

		:type data: dict
		:param data: The data to convert

		:rtype: dict
		:return: The converted dynamodb map
		"""
		if log:
			self.debug('Converting dict to dynamodb item: %s', json.dumps(data, ensure_ascii=False))

		dynamodb_map = {}
		for key, value in data.items():
			dynamodb_map.update({key: self.build_dynamodb_value(value, False)})

		if log:
			self.debug('Result: %s', json.dumps(dynamodb_map, ensure_ascii=False))

		return dynamodb_map


	def convert_dynamodb_map_to_dict(self, map: dict, log=True) -> dict:
		"""
		Convert a dynamodb map to dict. Convertable types:
		- 'S' -> str
		- 'M' -> dict

		:type map: dict
		:param map:

		:rtype: dict
		:return: The converted data
		"""
		if log:
			self.debug('Converting dynamodb item to dict: %s', json.dumps(map, ensure_ascii=False))

		data = {}
		for key, value in map.items():
			if value.get('S', None) is not None:
				data.update({key: value.get('S')})
			elif value.get('M', None) is not None:
				data.update({key: self.convert_dynamodb_map_to_dict(value.get('M'), False)})
			else:
				self.logger.warning('Cannot convert %s. Ignoring. Valid types are M,S. Value: %s', key,
									json.dumps(value, ensure_ascii=False))

		if log:
			self.debug('Result: %s', json.dumps(data, ensure_ascii=False))

		return data


	def get_error(self, type, message: str, *args):
		"""
		Returns a error type that can directly be used with raise()

		:type type: class
		:param type: The error type

		:type message: str
		:param message: The message with placeholders

		:type args: str
		:param args: A list of placeholder values

		:rtype Exception
		:return: The error object
		"""
		return type(self.__get_formatted_message(message, args))


	def info(self, message: str, *args):
		self.logger.info(self.__get_formatted_message(message, args))


	def error(self, message: str, *args):
		self.logger.error(self.__get_formatted_message(message, args))


	def warning(self, message: str, *args):
		self.logger.warning(self.__get_formatted_message(message, args))


	def debug(self, message: str, *args):
		self.logger.debug(self.__get_formatted_message(message, args))


	def __get_formatted_message(self, message: str, args) -> str:
		args = list(args)
		args = [self.get_name()] + args
		return ('%s: ' + message) % tuple(args)


	def __wait_for_ssm_agent_to_become_ready(self, instance_id: str):
		self.debug('Waiting for ssm agemt to become ready.')
		# can be replaced when https://github.com/boto/botocore/pull/1502 will be accepted
		# waiter = ssm.get_waiter['AgentIsOnline']
		self.waiters.get('AgentIsOnline').wait(
			Filters=[{'Key': 'InstanceIds', 'Values': [instance_id]}]
		)
