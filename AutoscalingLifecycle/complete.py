from AutoscalingLifecycle.base import EventAction
from AutoscalingLifecycle.entity.node import Node


class OnSsmEvent(EventAction):
	"""
	An action to respond to aws.ssm event

	:type default_result: str
	:type status_map: dict
	:type command_data: dict
	"""
	default_result = 'ABANDON'
	status_map = {
		'Cancelled': 'ABANDON',
		'Failed': 'ABANDON',
		'Success': 'CONTINUE',
		'TimedOut': 'ABANDON'
	}
	command_data = { }


	def _populate_event_data(self, event: dict):
		self.logger.set_name(self.logger.get_name() + '::Complete')
		self.logger.info('Preparing event data ...')

		if event.get('source', '') != 'aws.ssm':
			raise self.logger.get_error(TypeError, 'Event is not aws.ssm: %s', event.get('source', ''))

		super()._populate_event_data(event)

		self.logger.info('Loading command data %s', self.event_details.get('command-id'))
		self.command_data = self.command_repository.get(self.event_details.get('command-id'))
		self.logger.debug('Command data: %s', self.command_data)
		if type(self.command_data) is not dict:
			raise self.logger.get_error(TypeError, 'Data for command %s could not be found.',
										self.event_details.get('command-id'))

		if self.command_data.get('NotificationMetadata').get('debug', 'false') == 'true':
			self.logger.set_debug()

		self.default_result = self.status_map.get(self.event_details.get('status'))
		self.logger.debug('Default Result: %s', self.default_result)

		self.autoscaling_client.set_transition(self.command_data.get('LifecycleTransition'))


	def __call__(self):
		"""
		This is the main method to organize the basic logic/workflow
		Implementers only have to implement the specific stuff that should happen
		when an instance is launching or terminating
		"""
		self.logger.info('Executing %s ...', self.get_action_info())

		if self.event_details.get('status') != 'Success':
			self.logger.warning('The command %s has ended with a %s status. Instance willbe abandoned.',
								self.command_data.get('Comment'),
								self.event_details.get('status'))
			self.__gracefull_complete()

		else:
			self.logger.info('Loading node %s', self.command_data.get('EC2InstanceId'))
			try:
				self.node = self.node_repository.get(self.command_data.get('EC2InstanceId'))
			except TypeError as e:
				self.logger.error('Could not load node: %s. Trying to complete the lifecycle action. Removing command.',
								  repr(e))
				self.__gracefull_complete()

			if type(self.node) is Node:
				try:
					self.logger.debug('Loaded node data: %s', self.node.to_dict())

					if self.autoscaling_client.is_launching():
						self.logger.set_name(self.logger.get_name() + '::Launch:: ')
						self.logger.info('Completing lifecycle action on launch')
						self._on_launch()

					elif self.autoscaling_client.is_terminating():
						self.logger.set_name(self.logger.get_name() + '::Terminate:: ')
						self.logger.info('Completing lifecycle action on termination')
						self._on_terminate()

					else:
						raise self.logger.get_error(RuntimeError, 'Instance transition could not be determined.')
				except Exception as e:
					self.logger.error(
						'Something went wrong; %s. Now trying to at least complete the lifecycle action...', repr(e))
					self.__gracefull_complete()

		self.command_repository.delete(self.event_details.get('command-id'))


	def __gracefull_complete(self):
		try:
			if not hasattr(self, 'node') or type(self.node) is None:
				self.node = Node(self.command_data.get('EC2InstanceId'), 'unknown')

			self.node.set_status('terminating')
			if self.node.get_property('LifecycleActionToken') is None and self.command_data.get('LifecycleActionToken',
																								None) is not None:
				self.node.set_property('LifecycleActionToken', self.command_data.get('LifecycleActionToken'))

			self.complete_lifecycle_action(self.node.get_id(), self.node.get_property('LifecycleActionToken'),
										   'ABANDON')
			self.node_repository.delete(self.node)
		except Exception as e:
			self.logger.error('Failed to gracefully complete the action: %s', repr(e))

		self.node = None


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

		self.autoscaling_client.complete_lifecycle_action(
			self.command_data.get('LifecycleHookName'),
			self.command_data.get('AutoScalingGroupName'),
			token,
			result,
			instance_id
		)

		self.dynamodb_client.unset(instance_id, ['LifecycleActionToken'])
