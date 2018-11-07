import json
import time
from logging import DEBUG
from logging import Logger

from boto3 import Session

from . import ClientFactory
from . import CustomWaiters
from . import Node
from .clients import AutoscalingClient
from .clients import DynamoDbClient
from .clients import Route53Client
from .clients import SnsClient
from .clients import SsmClient
from .logging import Logging
from .repository import CommandRepository
from .repository import NodeRepository


class EventAction(object):
    """
    Abstract class to respond to a cloudwatch event

    This class handles:
    - load event data
    - prepare clients and waiters
    - call ssm scripts


    :type logger: Logger
    :param logger: A logger instance
    :type event: dict
    :param event: The event to care about
    :type event_details: dict
    :param event_details: The event details
    :type mandatory_event_keys: list
    :param mandatory_event_keys: a list of items needed to process an event
    :type node: Node
    :param node: The node in charge
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
    node = None


    def __init__(self, name: str, event: dict, session: Session, logging: Logging, notification_arn,
                 account, env):
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

        self.logger = logging.get_logger()
        self.formatter = logging.get_formatter()
        self.__create_clients(name, session, logging, notification_arn, account, env)
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

        return self.formatter.format(' %s :: %s', [self.event.get('source'), self.event.get('detail-type')])


    def _populate_event_data(self, event: dict):
        """
        Populates event data. Implementers can override this to load other or additional data
        """
        self.event = event
        self.logger.debug('event data: %s', self.event)

        for r in self.mandatory_event_keys:
            if r not in self.event:
                raise self.formatter.get_error(TypeError,
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
            raise self.formatter.get_error(RuntimeError,
                                           'Could not send command %s. Node will be abandoned. Error was: %s',
                                           comment, repr(e))


    def __create_clients(self, name: str, session: Session, logger_factory: Logging, notification_arn, account,
                         env):
        self.logger.debug('Creating clients ...')
        client_factory = ClientFactory(session = session, logger = self.logger)
        waiters = CustomWaiters(clients = client_factory, logger = self.logger)
        self.dynamodb_client = DynamoDbClient(
            client = client_factory.get('dynamodb'),
            state_table = name.lower() + '-state',
            logging = logger_factory,
            waiters = waiters
        )
        self.node_repository = NodeRepository(self.dynamodb_client, self.logger)
        self.command_repository = CommandRepository(self.dynamodb_client, self.logger)
        self.ssm_client = SsmClient(client_factory.get('ssm'), waiters, logger_factory)
        self.autoscaling_client = AutoscalingClient(client_factory.get('autoscaling'), waiters, logger_factory)
        self.route53_client = Route53Client(client_factory.get('route53'), waiters, logger_factory)
        self.sns = SnsClient(
            client_factory.get('sns'),
            waiters,
            logger_factory,
            client_factory.get('sns', 'eu-west-1'),
            notification_arn,
            account,
            env
        )


    def report_autoscaling_activity(self, action, group, instance_id):
        activity = self.autoscaling_client.get_autoscaling_activity(group, action, instance_id)
        self.logger.info('Reporting activity: node %s: %s', action, activity)
        self.sns.publish_autoscaling_activity(activity, 'eu-west-1')


    def report_activity(self, action, instance_id):
        self.logger.info('Reporting activity: %s on %s', action, instance_id)
        self.sns.publish_activity(action, instance_id, 'eu-west-1')


class OnAutoscalingEvent(EventAction):
    """
    An abstract action to respond to aws.autoscaling event
    """


    def _populate_event_data(self, event: dict):
        self.logger.info('Loading data ...')

        if event.get('source', '') != 'aws.autoscaling':
            e = self.formatter.get_error(TypeError, 'Event is not aws.autoscaling: %s', event.get('source', ''))
            self.sns.publish_error(e, 'populate event data', 'eu-west-1')
            raise e

        super()._populate_event_data(event)

        self.event_details.update(
            { 'NotificationMetadata': json.loads(self.event_details.get('NotificationMetadata')) })
        if self.event_details.get('NotificationMetadata').get('debug', 'false') == 'true':
            self.logger.setLevel(DEBUG)

        self.logger.debug('event details updated: %s', self.event_details)

        self.autoscaling_client.set_transition(self.event_details.get('LifecycleTransition'))


    def __call__(self):
        self.logger.info('Executing %s ...', self.get_action_info())

        try:
            if self.autoscaling_client.is_launching():
                self.report_autoscaling_activity('is launching', self.event_details.get('AutoScalingGroupName'),
                                                 self.event_details.get('EC2InstanceId'))

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
                time.sleep(45)

                # delegate to specific event
                self._on_launch()

            elif self.autoscaling_client.is_terminating():
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
                raise self.formatter.get_error(RuntimeError, 'Instance transition could not be determined.')

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
        self.logger.info('Preparing event data ...')

        if event.get('source', '') != 'aws.ssm':
            e = self.formatter.get_error(TypeError, 'Event is not aws.ssm: %s', event.get('source', ''))
            self.sns.publish_error(e, 'populate event data', 'eu-west-1')
            raise e

        super()._populate_event_data(event)

        self.logger.info('Loading command data %s', self.event_details.get('command-id'))
        self.command_data = self.command_repository.get(self.event_details.get('command-id'))
        self.logger.debug('Command data: %s', self.command_data)
        if type(self.command_data) is not dict:
            e = self.formatter.get_error(TypeError, 'Data for command %s could not be found.',
                                         self.event_details.get('command-id'))
            self.sns.publish_error(e, 'populate event data', 'eu-west-1')
            raise e
        if self.command_data.get('NotificationMetadata').get('debug', 'false') == 'true':
            self.logger.setLevel(DEBUG)

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

        try:
            if self.event_details.get('status') != 'Success':
                if self.command_data.get('action', 'autoscaling') == 'autoscaling':
                    self.logger.warning('The command %s has ended with a %s status. Instance will be abandoned.',
                                        self.command_data.get('Comment'),
                                        self.event_details.get('status')
                                        )
                    self.__gracefull_complete()
                else:
                    self.logger.error('The command %s ended with a %s status.',
                                      self.command_data.get('Comment'),
                                      self.event_details.get('status')
                                      )

                raise self.formatter.get_error(RuntimeError,
                                               self.formatter.format('The command %s has ended with a %s status',
                                                                     [self.command_data,
                                                                      self.event_details.get('status')]
                                                                     )
                                               )

            else:
                if self.command_data.get('action', 'autoscaling') == 'autoscaling':
                    self.logger.info('Loading node %s', self.command_data.get('EC2InstanceId'))
                    try:
                        self.node = self.node_repository.get(self.command_data.get('EC2InstanceId'))
                    except TypeError as e:
                        self.logger.exception(
                            'Could not load node: %s. Trying to complete the lifecycle action. Removing command.',
                            repr(e)
                        )
                        self.__gracefull_complete()
                        raise e

                    if type(self.node) is Node:
                        try:
                            self.logger.debug('Loaded node data: %s', self.node.to_dict())

                            if self.autoscaling_client.is_launching():
                                self.logger.info('Completing lifecycle action on launch')
                                self._on_launch()
                                self.report_autoscaling_activity(
                                    'has launched',
                                    self.command_data.get('AutoScalingGroupName'),
                                    self.command_data.get('EC2InstanceId')
                                )

                            elif self.autoscaling_client.is_terminating():
                                self.logger.info('Completing lifecycle action on termination')
                                self._on_terminate()
                                self.report_autoscaling_activity(
                                    'has terminated',
                                    self.command_data.get('AutoScalingGroupName'),
                                    self.command_data.get('EC2InstanceId')
                                )
                            else:
                                raise self.formatter.get_error(RuntimeError,
                                                               'Instance transition could not be determined.')
                        except Exception as e:
                            self.logger.exception(
                                'Something went wrong; %s. Now trying to at least complete the lifecycle action...',
                                repr(e)
                            )
                            self.__gracefull_complete()
                            raise e
                else:
                    self.report_activity(
                        self.command_data.get('Comment'),
                        self.command_data.get('RunningOn')
                    )

        except Exception as e:
            self.sns.publish_error(e, 'complete', 'eu-west-1')
            self.command_repository.delete(self.event_details.get('command-id'))
            raise e

        self.command_repository.delete(self.event_details.get('command-id'))


    def __gracefull_complete(self):
        try:
            if not hasattr(self, 'node') or type(self.node) is None:
                self.logger.warning("Node has not been loaded. Using basic ")
                self.node = Node(self.command_data.get('EC2InstanceId'), 'unknown')
            else:
                try:
                    self.node_repository.update(self.node, {
                        'ItemStatus': 'terminating'
                    })
                except Exception:
                    self.logger.warning("Node status could not be updated while gracefully completing lifecycle for %s",
                                        self.node.get_id())

            node_id = self.node.get_id()
            if self.node.get_property('LifecycleActionToken') is not None:
                token = self.node.get_property('LifecycleActionToken')
            else:
                token = self.command_data.get('LifecycleActionToken')

            try:
                self.node_repository.delete(self.node)
            except Exception:
                self.logger.warning("Node could not be deleted while gracefully completing lifecycle for %s",
                                    self.node.get_id())

            self.complete_lifecycle_action(
                node_id,
                token,
                'ABANDON'
            )
        except Exception as e:
            self.logger.exception('Failed to gracefully complete the action %s: %s', self.command_data, repr(e))

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
