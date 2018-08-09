import json
import logging
import time

from autoscaling_lifecycle.base import EventAction


class OnAutoscalingEvent(EventAction):
	"""
	An action to respond to aws.autoscaling event

	:type metadata: dict
	"""
	metadata = {}


	def load_event_specific_data(self):
		super().load_event_specific_data()

		if self.event.get('source') != 'aws.autoscaling':
			raise TypeError(self.get_name() + ': Event is not aws.autoscaling')

		self.metadata = json.loads(self.event_details.get('NotificationMetadata'))
		if self.metadata.get('debug', 'false') == 'true':
			self.is_debug = True
			self.logger.setLevel(logging.DEBUG)

		self.debug('metadata: %s', json.dumps(self.metadata, ensure_ascii=False))

		self.transition = self.event_details.get('LifecycleTransition')
		if self.transition != 'autoscaling:EC2_INSTANCE_LAUNCHING' and self.transition != 'autoscaling:EC2_INSTANCE_TERMINATING':
			raise self.get_error(TypeError, 'Unknown autoscaling transition %s', self.transition)

		self.debug('transition: %s', self.transition)


	def __call__(self):
		if self.is_launching():
			self.info('Launching %s: %s', self.get_node_type(), self.event_details.get('EC2InstanceId'))

			# add pending node to db
			self.__register_node()

			# wait for cloud-init to finish
			time.sleep(60)

			# delegate to specific event
			self._on_launch()

		elif self.is_terminating():
			self.info('Terminating %s: %s', self.get_node_type(), self.event_details.get('EC2InstanceId'))

			# remove the node from the db as soon as possible, so that it
			# cannot be found by subsequent events
			self.__remove_node()

			# delegate to specific event
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


	def _get_registration_data(self):
		"""
		Fetch data to store with an item on registration. Can be overridden by specific classes.

		:rtype: dict
		:return: A dictionary containing the data to store while registering a node
		"""
		data = {}
		data.update({'EC2InstanceId': self.event_details.get('EC2InstanceId')})
		data.update({'ItemStatus': 'pending'})

		return data


	def __register_node(self):
		self.info('Register %s node to db %s', self.get_node_type(), self.event_details.get('EC2InstanceId'))

		_ = self.get_client('dynamodb').put_item(
			TableName=self.get_state_table(),
			Item=self.build_dynamodb_item(
				self.event_details.get('EC2InstanceId'),
				self.get_node_type(),
				self._get_registration_data()
			)
		)


	def __remove_node(self):
		instance_id = self.event_details.get('EC2InstanceId')
		_ = self.get_client('dynamodb').delete_item(
			TableName=self.get_state_table(),
			Key=self.build_dynamodb_key(instance_id)
		)
