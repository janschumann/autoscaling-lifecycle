class Event(object):
    __LAUNCHING = 'autoscaling:EC2_INSTANCE_LAUNCHING'
    __TERMINATING = 'autoscaling:EC2_INSTANCE_TERMINATING'
    _event = { }


    def __init__(self, event: dict):
        self._event = event


    def get_event(self) -> dict:
        return self._event


    def get_source(self) -> str:
        return self._event.get('source')


    def is_launching(self) -> bool:
        """
        :rtype: bool
        :return: Whether we react on a launch event
        """
        return self.get_lifecycle_transition() == self.__LAUNCHING


    def is_terminating(self) -> bool:
        """
        :rtype: bool
        :return: Whether we react on a terminate event
        """
        return self.get_lifecycle_transition() == self.__TERMINATING


    def get_lifecycle_action_token(self) -> str:
        raise NotImplementedError()


    def get_lifecycle_transition(self) -> str:
        raise NotImplementedError()


    def get_lifecycle_hook_name(self) -> str:
        raise NotImplementedError()


    def get_autoscaling_group_name(self) -> str:
        raise NotImplementedError()


    def get_instance_id(self) -> str:
        raise NotImplementedError()


    def get_metadata(self) -> dict:
        raise NotImplementedError()


class AutoscalingEvent(Event):
    def get_lifecycle_action_token(self) -> str:
        return self._event.get('detail').get('LifecycleActionToken')


    def get_lifecycle_transition(self) -> str:
        return self._event.get('detail').get('LifecycleTransition')


    def get_lifecycle_hook_name(self) -> str:
        return self._event.get('detail').get('LifecycleHookName')


    def get_autoscaling_group_name(self) -> str:
        return self._event.get('detail').get('AutoScalingGroupName')


    def get_instance_id(self) -> str:
        return self._event.get('detail').get('EC2InstanceId')


    def get_metadata(self) -> dict:
        return self._event.get('detail').get('NotificationMetadata')


class SsmEvent(Event):
    __command = { }


    def __init__(self, event: dict, command: dict):
        super().__init__(event)
        self.__command = command


    def get_lifecycle_action_token(self) -> str:
        return self.__command.get('LifecycleActionToken')


    def get_lifecycle_transition(self) -> str:
        return self.__command.get('LifecycleTransition')


    def get_lifecycle_hook_name(self) -> str:
        return self.__command.get('LifecycleHookName')


    def get_autoscaling_group_name(self) -> str:
        return self.__command.get('AutoScalingGroupName')


    def get_instance_id(self) -> str:
        return self.__command.get('EC2InstanceId')


    def get_metadata(self) -> dict:
        return self.__command.get('NotificationMetadata')

