import json
import time

from AutoscalingLifecycle.base import EventAction


class OnAutoscalingEvent(EventAction):
	"""
	An abstract action to respond to aws.autoscaling event
	"""


	def _populate_event_data(self, event: dict):
		self.logger.set_name(self.logger.get_name() + '::Scale')
		self.logger.info('Loading data ...')

		if event.get('source', '') != 'aws.autoscaling':
			e = self.logger.get_error(TypeError, 'Event is not aws.autoscaling: %s', event.get('source', ''))
			self.sns.publish_error(e, 'populate event data', 'eu-west-1')
			raise e

		super()._populate_event_data(event)

		self.event_details.update(
			{ 'NotificationMetadata': json.loads(self.event_details.get('NotificationMetadata')) })
		if self.event_details.get('NotificationMetadata').get('debug', 'false') == 'true':
			self.logger.set_debug()

		self.logger.debug('event details updated: %s', self.event_details)

		self.autoscaling_client.set_transition(self.event_details.get('LifecycleTransition'))


	def __call__(self):
		self.logger.info('Executing %s ...', self.get_action_info())

		try:
			if self.autoscaling_client.is_launching():
				self.report_autoscaling_activity('is launching', self.event_details.get('AutoScalingGroupName'),
												 self.event_details.get('EC2InstanceId'))

				self.logger.set_name(self.logger.get_name() + '::OUT:: ')

				self.logger.info('Determine node type ...')
				node_type = self._determine_node_type()

				self.logger.info('Launching %s: %s', node_type, self.event_details.get('EC2InstanceId'))

				self.logger.info('Registering %s: %s', node_type, self.event_details.get('EC2InstanceId'))
				data = self._get_registration_data()
				self.logger.info('Data: %s', data)
				self.node = self.node_repository.register(
					self.event_details.get('EC2InstanceId'),
					node_type,
					data
				)
				self.logger.debug('Registered node data: %s', self.node.to_dict())

				self.logger.debug('Waiting for cloud-init to finish ...')
				time.sleep(60)

				# delegate to specific event
				self._on_launch()

			elif self.autoscaling_client.is_terminating():
				self.logger.set_name(self.logger.get_name() + '::IN:: ')

				self.report_autoscaling_activity('is terminating', self.event_details.get('AutoScalingGroupName'),
												 self.event_details.get('EC2InstanceId'))

				self.logger.info('Loading node %s from the db.', self.event_details.get('EC2InstanceId'))
				try:
					self.node = self.node_repository.get(self.event_details.get('EC2InstanceId'))
				except TypeError as e:
					self.logger.exception('Could not load node. Trying to complete the lifecycle action.')
					self.__gracefull_complete()
					return e

				self.logger.info('Setting node status to "terminating"')
				self.node_repository.update(self.node, {
					'ItemStatus': 'terminating',
					'LifecycleActionToken': self.event_details.get('LifecycleActionToken')
				})

				self.logger.info('Terminating %s: %s', self.node.get_type(), self.node.get_id())

				# delegate to specific event
				self._on_terminate()

			else:
				raise self.logger.get_error(RuntimeError, 'Instance transition could not be determined.')

		except Exception as e:
			self.sns.publish_error(e, 'scale', 'eu-west-1')
			raise e


	def _determine_node_type(self) -> str:
		"""
		The implementer needs to identify the node type of the node in progress

		:rtype: str
		:return: The node type
		"""
		raise NotImplementedError()


	def _get_registration_data(self) -> dict:
		"""
		Give the implementer the ability to add specific data to the node on registration. Empty by default

		:rtype: dict
		:return: A dict of specific data
		"""
		return { }


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


	def __gracefull_complete(self):
		try:
			self.autoscaling_client.complete_lifecycle_action(
				self.event_details.get('LifecycleHookName'),
				self.event_details.get('AutoScalingGroupName'),
				self.event_details.get('LifecycleActionToken'),
				'ABANDON',
				self.event_details.get('EC2InstanceId')
			)
		except Exception as e:
			self.logger.error('Failed to gracefully complete the lifecycle: %s', repr(e))
