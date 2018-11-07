import json
from logging import Logger

import botocore.client
import botocore.waiter
from boto3 import Session

from .logging import Formatter
from .logging import MessageFormatter
from .logging import SnsHandler


class Event(object):
    __LAUNCHING = 'autoscaling:EC2_INSTANCE_LAUNCHING'
    __TERMINATING = 'autoscaling:EC2_INSTANCE_TERMINATING'

    _CONTINUE = 'CONTINUE'
    _ABANDON = 'ABANDON'

    _event = { }


    def __init__(self, event: dict):
        self._event = event


    def get_event(self) -> dict:
        return self._event


    def get_command_metadata(self) -> dict:
        return self.get_event()


    def get_detail(self) -> dict:
        return self._event.get('detail')


    def get_source(self) -> str:
        return self._event.get('source')


    def is_successful(self) -> bool:
        return True


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


    def get_lifecycle_result(self) -> str:
        if self.is_terminating():
            return self._CONTINUE

        return self._ABANDON


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

    def __init__(self, event: dict):
        super().__init__(event)

        metadata = self._event.get('detail').get('NotificationMetadata')
        if type(metadata) is not dict:
            self._event.get('detail').update({
                'NotificationMetadata': json.loads(metadata)
            })


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


    def get_lifecycle_result(self) -> str:
        if self.is_terminating() or self.is_successful():
            return self._CONTINUE

        return self._ABANDON


    def get_event(self):
        e = super().get_event()
        e.update({'CommandMetadata': self.get_command_metadata()})
        return e


    def is_successful(self) -> bool:
        return self._event.get('detail').get('status') == 'Success'


    def get_command_metadata(self) -> dict:
        return self.__command


    def get_lifecycle_action_token(self) -> str:
        return self.__command.get('detail').get('LifecycleActionToken')


    def get_lifecycle_transition(self) -> str:
        return self.__command.get('detail').get('LifecycleTransition')


    def get_lifecycle_hook_name(self) -> str:
        return self.__command.get('detail').get('LifecycleHookName')


    def get_autoscaling_group_name(self) -> str:
        return self.__command.get('detail').get('AutoScalingGroupName')


    def get_instance_id(self) -> str:
        return self.__command.get('detail').get('EC2InstanceId')


    def get_metadata(self) -> dict:
        return self.__command.get('detail').get('NotificationMetadata')


class Node(object):
    id = None
    type = None
    status = 'new'
    data = { }
    mandatory_propertoes = [
        'EC2InstanceId',
        'ItemType',
        'ItemStatus',
    ]
    readonly_propertoes = [
        'EC2InstanceId',
        'ItemType',
    ]


    def __init__(self, id, node_type):
        if id == "" or id is None or node_type == "" or node_type is None:
            raise TypeError("id and node_type must not be empty")

        self.data = { }
        self.id = id
        self.data.update({ 'EC2InstanceId': self.id })
        self.type = node_type
        self.data.update({ 'ItemType': self.type })
        self.data.update({ 'ItemStatus': self.status })


    def get_id(self):
        return self.id


    def get_type(self):
        return self.type


    def set_type(self, node_type):
        self.type = node_type
        self.data.update({ 'ItemType': self.type })


    def get_status(self):
        return self.status


    def set_status(self, status):
        self.status = status
        self.data.update({ 'ItemStatus': self.status })


    def get_property(self, property, default = None):
        return self.data.get(property, default)


    def set_property(self, property, value):
        if property in self.readonly_propertoes:
            raise TypeError(property + ' is read only.')

        if property == 'ItemStatus':
            self.set_status(value)
        else:
            self.data.update({ property: value })


    def unset_property(self, property):
        if property in self.mandatory_propertoes:
            raise TypeError(property + ' cannot be unset.')

        _ = self.data.pop(property)


    def is_valid(self):
        return self.id != ''


    def to_dict(self):
        return {
            'id': self.id,
            'type': self.type,
            'status': self.status,
            'data': self.data
        }


    def set_state(self, dest):
        self.set_status(dest)


    def get_state(self) -> str:
        return self.status


    def is_new(self) -> bool:
        return self.status == 'new'


    def set_id(self, ident):
        self.id = ident
        self.data.update({ 'EC2InstanceId': self.id })


class ClientFactory(object):
    """
    A simple class that creates boto3 service clients. Each client will be created only once
    and than returned from local cache
    """


    def __init__(self, session: Session, logger: Logger):
        """

        :param session: A boto3 Session instamce
        :type session: Session
        :param logger: A LifecycleLogger instance
        :type logger: LifecycleLogger
        """
        self.session = session
        self.logger = logger
        self.clients = { }


    def get(self, name: str, region_name: str = 'eu-central-1'):
        """
        Get a boto client. Clients will be cached locally.
        E.g. get_client('ssm') will return boto3.client('ssm')

        :type name: str
        :param name: The name of the client to create

        :type region_name: str
        :param region_name: The region this client will be created in

        :rtype: BaseClient
        :return: Service client instance
        """

        self.logger.info('Retrieving client %s in region %s', name, region_name)
        key = name + '_' + region_name
        client = self.clients.get(key, None)
        if client is None:
            self.logger.debug('Client %s in region %s not created. Creating ...', name, region_name)
            client = self.session.client(name, region_name = region_name)
            self.clients.update({ key: client })

        return client


class CustomWaiters(object):
    model_configs = {
        'ScanCountGt0': {
            'client': 'dynamodb',
            'model': {
                "version": 2,
                "waiters": {
                    "ScanCountGt0": {
                        "delay": 15,
                        "operation": "Scan",
                        "maxAttempts": 40,
                        "acceptors": [
                            {
                                "expected": True,
                                "matcher": "path",
                                "state": "success",
                                "argument": "length(Items[]) > `0`"
                            },
                            {
                                "expected": True,
                                "matcher": "path",
                                "state": "retry",
                                "argument": "length(Items[]) == `0`"
                            }
                        ]
                    }
                }
            }
        },
        'InstancesInService': {
            'client': 'autoscaling',
            'model': {
                "version": 2,
                "waiters": {
                    "InstancesInService": {
                        "delay": 5,
                        "operation": "DescribeAutoScalingInstances",
                        "maxAttempts": 10,
                        "acceptors": [
                            {
                                "expected": "InService",
                                "matcher": "pathAny",
                                "state": "success",
                                "argument": "AutoScalingInstances[].LifecycleState"
                            }
                        ]
                    }
                }
            }
        },
        'AgentIsOnline': {
            'client': 'ssm',
            'model': {
                "version": 2,
                "waiters": {
                    "AgentIsOnline": {
                        "delay": 10,
                        "operation": "DescribeInstanceInformation",
                        "maxAttempts": 20,
                        "acceptors": [
                            {
                                "expected": "Online",
                                "matcher": "pathAny",
                                "state": "success",
                                "argument": "InstanceInformationList[].PingStatus"
                            },
                            {
                                "expected": "ConnectionLost",
                                "matcher": "pathAny",
                                "state": "retry",
                                "argument": "InstanceInformationList[].PingStatus"
                            },
                            {
                                "expected": "Inactive",
                                "matcher": "pathAny",
                                "state": "failure",
                                "argument": "InstanceInformationList[].PingStatus"
                            }
                        ]
                    }
                }
            }
        }
    }
    waiters = { }


    def __init__(self, clients: ClientFactory, logger: Logger):
        """
        :type clients: ClientFactory
        :param clients:
        :type logger: LifecycleLogger
        :param logger:
        """
        self.clients = clients
        self.logger = logger
        self.message_formatter = MessageFormatter(logger.name)


    def get_waiter_names(self):
        """
        :rtype: list
        :return: A list of waiter names
        """
        return self.model_configs.keys()


    def get(self, name):
        """
        :type name: str
        :param name: The name of the waiter

        :rtype: botocore.waiter.Waiter
        :return: The waiter object.
        """

        if not self.__has(name):
            config = self.model_configs.get(name)
            model = botocore.waiter.WaiterModel(config.get('model'))
            client = self.clients.get(config.get('client'))
            self.__create(name, model, client)

        return self.waiters.get(name)


    def get_dynamodb_scan_count_is(self, size):
        """
        :type size: int or str
        :param size: The number of expected scan items to find

        :rtype: botocore.waiter.Waiter
        :return: The waiter object.
        """
        name = "ScanCountIs" + str(size)

        if not self.__has(name):
            model = botocore.waiter.WaiterModel({
                "version": 2,
                "waiters": {
                    name: {
                        "delay": 15,
                        "operation": "Scan",
                        "maxAttempts": 40,
                        "acceptors": [
                            {
                                "expected": True,
                                "matcher": "path",
                                "state": "success",
                                "argument": "length(Items[]) == " f"`{size}`"
                            }
                        ]
                    }
                }
            })
            self.__create(name, model, self.clients.get('dynamodb'))

        return self.get(name)


    def __has(self, name: str):
        return name in self.waiters.keys()


    def __create(self, name: str, model: botocore.waiter.WaiterModel, client: botocore.client.BaseClient):
        if name not in model.waiter_names:
            raise self.message_formatter.get_error(KeyError, 'Waiter %s does not exist', name)

        self.waiters.update({ name: botocore.waiter.create_waiter_with_client(name, model, client) })
