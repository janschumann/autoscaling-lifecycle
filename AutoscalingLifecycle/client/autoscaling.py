from AutoscalingLifecycle.helper.logger import LifecycleLogger
from AutoscalingLifecycle.helper.waiters import Waiters


class AutoscalingClient(object):
	"""
	:type waiters: Waiters
	"""
	waiters = None
	transition = None


	def __init__(self, client, waiters: Waiters, logger: LifecycleLogger):
		self.client = client
		self.waiters = waiters,
		self.logger = logger


	def set_transition(self, transition):
		self.transition = transition
		if self.transition != 'autoscaling:EC2_INSTANCE_LAUNCHING' and self.transition != 'autoscaling:EC2_INSTANCE_TERMINATING':
			raise self.logger.get_error(TypeError, 'Unknown autoscaling transition %s', self.transition)

		self.logger.debug('Transition: %s', self.transition)


	def is_launching(self) -> bool:
		"""
		:rtype: bool
		:return: Whether we react on a launch event
		"""
		self.ensure_transition_is_set()

		return self.transition == 'autoscaling:EC2_INSTANCE_LAUNCHING'


	def is_terminating(self) -> bool:
		"""
		:rtype: bool
		:return: Whether we react on a terminate event
		"""
		self.ensure_transition_is_set()

		return self.transition == 'autoscaling:EC2_INSTANCE_TERMINATING'


	def ensure_transition_is_set(self):
		if self.transition is None:
			raise self.logger.get_error(TypeError, 'Transition not set')


	def complete_lifecycle_action(self, hook_name, group_name, token, result, instance_id):
		self.logger.debug('Copleting lifecycle action for %s with %s', instance_id, result)
		_ = self.client.complete_lifecycle_action(
			LifecycleHookName = hook_name,
			AutoScalingGroupName = group_name,
			LifecycleActionToken = token,
			LifecycleActionResult = result,
			InstanceId = instance_id
		)


	def wait_for_instances_in_service(self, instance_ids):
		"""
		Wait for instances to get healthy within an autoscaling group

		:type instance_ids: list
		:param instance_ids: The instances to wait for
		"""
		self.logger.debug('Autoscaling: Waiting for instances become in service.')
		self.waiters.get('InstancesInService').wait(
			InstanceIds = instance_ids
		)


	def prevent_instances_to_scale_in(self, instance_ids, group_name):
		self.logger.info("Preventing instances from scale in.")
		self.wait_for_instances_in_service(instance_ids)

		_ = self.client.set_instance_protection(
			InstanceIds = instance_ids,
			AutoScalingGroupName = group_name,
			ProtectedFromScaleIn = True
		)


	def get_autoscaling_activity(self, group, action, instance_id):
		activities = self.client.describe_scaling_activities(
			AutoScalingGroupName = group
		)['Activities']

		if action == "is launching" or action == "has launched":
			desc = "Launching a new EC2 instance: " + instance_id
		else:
			desc = "Terminating EC2 instance: " + instance_id

		for activity in activities:
			if activity.get('Description') == desc:
				return activity

		return {}
