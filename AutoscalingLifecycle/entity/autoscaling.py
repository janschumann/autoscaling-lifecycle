import json

from AutoscalingLifecycle.entity.event import Event


class AutoscalingEvent(Event):

	def __init__(self, event):
		super().__init__(event)

		self.__metadata = json.loads(self.detail.get('NotificationMetadata')) or { }


	@property
	def token(self) -> str:
		return self.detail.get('LifecycleActionToken')


	@property
	def group_name(self) -> str:
		return self.detail.get('AutoScalingGroupName')


	@property
	def hook_name(self) -> str:
		return self.detail.get('LifecycleHookName')


	@property
	def instance_id(self) -> str:
		return self.detail.get('EC2InstanceId')


	@property
	def transition(self) -> str:
		return self.detail.get('LifecycleTransition')


	@property
	def metadata(self) -> dict:
		return self.__metadata
