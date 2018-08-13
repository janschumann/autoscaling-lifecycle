import json
import logging

from AutoscalingLifecycle.base import EventAction


class OnSsmEvent(EventAction):
	"""
	An action to respond to aws.ssm event

	:type default_result: str
	:type status_map: dict
	:type command_data: dict
	:type metadata: dict
	"""
	default_result = 'ABANDON'
	status_map = {
		'Cancelled': 'ABANDON',
		'Failed': 'ABANDON',
		'Success': 'CONTINUE',
		'TimedOut': 'ABANDON'
	}
	command_data = { }
	metadata = { }


	def load_event_specific_data(self):
		super().load_event_specific_data()

		if self.event.get('source') != 'aws.ssm':
			raise TypeError(self.get_name() + ': Event is not aws.ssm')

		self.command_data = self.fetch_command_data(self.event_details.get('command-id'))
		if self.command_data == { }:
			raise TypeError(self.get_name() + ': Data for command "' + self.event_details.get(
				'command-id') + '" could not be found.')

		self.debug('Command data: %s', json.dumps(self.command_data, ensure_ascii = False))

		self.metadata = self.command_data.get('metadata')
		if self.metadata.get('debug', 'false') == 'true':
			self.is_debug = True
			self.logger.setLevel(logging.DEBUG)

		self.debug('metadata: %s', json.dumps(self.metadata, ensure_ascii = False))

		self.default_result = self.status_map.get(self.event_details.get('status'))

		self.debug('Result: %s', self.default_result)

		self.transition = self.command_data.get('LifecycleTransition')
		if self.transition != 'autoscaling:EC2_INSTANCE_LAUNCHING' and self.transition != 'autoscaling:EC2_INSTANCE_TERMINATING':
			raise self.get_error(TypeError, 'Unknown autoscaling transition %s', self.transition)

		self.debug('Transition: %s', self.transition)


	def __call__(self):
		self.debug('Removing command %s from db', self.event_details.get('command-id'))
		_ = self.get_client('dynamodb').delete_item(
			TableName = self.get_state_table(),
			Key = self.build_dynamodb_key(self.event_details.get('command-id'))
		)

		if self.is_launching():
			self.info('Completing lifecycle action on launch')
			self._on_launch()

		elif self.is_terminating():
			self.info('Completing lifecycle action on termination')
			self._on_terminate()


	def _on_launch(self):
		"""
		What to do on launch. Needs to be implemented by specific actions
		"""
		raise NotImplementedError()


	def _on_terminate(self):
		"""
		What to do on termination. Needs to be implemented by specific actions
		"""
		raise NotImplementedError()


	def fetch_command_data(self, command_id: str) -> dict:
		"""
		Load command metadata from state table

		:type command_id: str
		:param command_id: The command id to load data from

		:rtype: dict
		:return: The command data
		"""
		self.info('Loading command data.')

		item = self.get_client('dynamodb').get_item(
			TableName = self.get_state_table(),
			Key = self.build_dynamodb_key(command_id)
		).get('Item')

		return self.convert_dynamodb_map_to_dict(item)


	def complete_lifecycle_action(self, instance_id, token, result):
		"""
		Complete an autoscaling action

		:type instance_id: str
		:param instance_id: The instance to perform complete on

		:type token: str
		:param token: The lifecycle token

		:type result: str
		:param result: The lifecycle result (CONTINUE or ABANDON)
		"""
		self.info('Completing lifecycle action on %s: %s', self.get_node_type(), instance_id)

		_ = self.get_client('autoscaling').complete_lifecycle_action(
			LifecycleHookName = self.command_data.get('LifecycleHookName'),
			AutoScalingGroupName = self.command_data.get('AutoScalingGroupName'),
			LifecycleActionToken = token,
			LifecycleActionResult = result.upper(),
			InstanceId = instance_id
		)


	def mark_as_ready(self, instance_id: str):
		"""
		Mark an instance item as ready in the db

		:type instance_id: str
		:param instance_id: The instance to mark as ready
		"""
		_ = self.get_client('dynamodb').update_item(
			TableName = self.get_state_table(),
			Key = self.build_dynamodb_key(instance_id),
			UpdateExpression = "SET ItemStatus = :item_status",
			ExpressionAttributeValues = {
				':item_status': self.build_dynamodb_value('ready')
			}
		)


	def wait_for_instances_in_service(self, instance_ids):
		"""
		Wait for instances to get healthy within an autoscaling group

		:type instance_ids: list
		:param instance_ids: The instances to wait for
		"""
		self.debug('Waiting for instances become in service.')
		self.waiters.get('InstancesInService').wait(
			InstanceIds = instance_ids
		)
