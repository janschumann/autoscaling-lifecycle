from logging import Logger

import botocore.waiter as waiter
from boltons.tbutils import ExceptionInfo
import boto3
from botocore.client import BaseClient as BotoClient
from botocore.exceptions import WaiterError, ClientError

from .logging import Logging
from .logging import MessageFormatter


class ClientFactory(object):
    """
    A simple class that creates boto3 service clients. Each client will be created only once
    and than returned from local cache
    """


    def __init__(self, session: boto3.Session, default_region: str, logger: Logger):
        """

        :param session: A boto3 boto3.Session instamce
        :type session: boto3.Session
        :param default_region: The default region to create clients in
        :type default_region: str
        :param logger: A LifecycleLogger instance
        :type logger: LifecycleLogger
        """
        self.session = session
        self.logger = logger
        self.default_region = default_region
        self.clients = {}


    def get(self, name: str, region_name: str = ''):
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

        if region_name == '':
            region_name = self.default_region

        self.logger.debug('Retrieving client %s in region %s', name, region_name)
        key = name + '_' + region_name
        client = self.clients.get(key, None)
        if client is None:
            self.logger.debug('Client %s in region %s not created. Creating ...', name, region_name)
            client = self.session.client(name, region_name = region_name)
            self.clients.update({key: client})

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
    waiters = {}


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
            model = waiter.WaiterModel(config.get('model'))
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
            model = waiter.WaiterModel({
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


    def get_autoscaling_complete_for(self, instance_id, is_launching):
        """
        :rtype: botocore.waiter.Waiter
        :return: The waiter object.
        """

        if is_launching:
            desc = "Launching a new EC2 instance: " + instance_id
            name = "AutoscalingCompleteForLaunching" + instance_id
        else:
            desc = "Terminating EC2 instance: " + instance_id
            name = "AutoscalingCompleteForTerminating" + instance_id

        if not self.__has(name):
            model = waiter.WaiterModel({
                "version": 2,
                "waiters": {
                    name: {
                        "delay": 20,
                        "operation": "DescribeScalingActivities",
                        "maxAttempts": 6,
                        "acceptors": [
                            {
                                "expected": True,
                                "matcher": "path",
                                "state": "failure",
                                "argument": "length(Activities[?contains(Description, '" f"{desc}""')]) < `1`"
                            },
                            {
                                "expected": True,
                                "matcher": "path",
                                "state": "retry",
                                "argument": "Activities[?contains(Description, '" f"{desc}""')] | [0].Progress < `100`"
                            },
                            {
                                "expected": True,
                                "matcher": "path",
                                "state": "success",
                                "argument": "Activities[?contains(Description, '" f"{desc}""')] | [0].Progress == `100`"
                            }
                        ]
                    }
                }
            })
            self.__create(name, model, self.clients.get('autoscaling'))

        return self.get(name)


    def __has(self, name: str):
        return name in self.waiters.keys()


    def __create(self, name: str, model: waiter.WaiterModel, client: BotoClient):
        if name not in model.waiter_names:
            raise self.message_formatter.get_error(KeyError, 'Waiter %s does not exist', name)

        self.waiters.update({name: waiter.create_waiter_with_client(name, model, client)})


class Clients(object):
    """
    lazy load clients when needed
    :param __client_specs: A dict of client specifications
    :type __client_specs: dict
    :param __clients: A dict of client instances
    :type __clients: dict
    """
    __client_specs = dict()
    __clients = dict()


    def __init__(self, client_factory: ClientFactory, waiters: CustomWaiters, logging: Logging):
        self.client_factory = client_factory
        self.waiters = waiters
        self.logging = logging


    def add_client_spec(self, name, cls, *args):
        self.__client_specs.update({
            name: {
                'class': cls,
                'args': args
            }
        })


    def get(self, name, region = ''):
        spec = self.__client_specs.get(name, None)
        if spec is None:
            raise RuntimeError("no specs for %s found" % name)

        cname = '{}-{}'.format(name, region)
        client = self.__clients.get(cname, None)
        if client is None:
            client = spec.get('class')(self.client_factory.get(name, region), self.waiters, self.logging, *spec.get('args'))
            self.__clients.update({cname: client})

        return client


class BaseClient(object):
    """
    :type client: BotoClient
    :param client: A botocore client instance
    :type waiters: CustomWaiters
    :param waiters: A collection of custom waiters
    :type logger: Logger
    :param logger: A logger instance
    :type formater: MessageFormatter
    :param formater: A formatter instance
    """

    client = None
    waiters = None
    logger = None
    formatter = None


    def __init__(self, client: BotoClient, waiters: CustomWaiters, logging: Logging, *args):
        self.client = client
        self.waiters = waiters
        self.logger = logging.get_logger()
        self.formatter = logging.get_formatter()


class Ec2Client(BaseClient):

    def __init__(self, client: BotoClient, waiters: CustomWaiters, logging: Logging, *args):
        super().__init__(client, waiters, logging)
        self.resource = boto3.resource('ec2')


    def find_instances_by_name(self, name) -> list:
        response = self.client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [
                        name,
                    ]
                },
                {
                    'Name': 'instance-state-name',
                    'Values': [
                        'running',
                    ]
                },
            ]
        )

        instances = []
        for reservation in response.get('Reservations', []):
            for instance in reservation.get('Instances', []):
                instances.append(instance)

        return instances


    def get_instance(self, instance_id) -> dict:
        response = self.client.describe_instances(
            InstanceIds=[instance_id],
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': [
                        'running',
                    ]
                },
            ]
        )

        for reservation in response.get('Reservations', []):
            for instance in reservation.get('Instances', []):
                return instance

        return dict()


    def create_snapshot(self, description, volume_id, tags: list):
        response = self.client.create_snapshot(
            Description=description,
            VolumeId=volume_id,
        )

        self.client.get_waiter('snapshot_completed').wait(
            SnapshotIds=[response.get('SnapshotId')],
        )

        if len(tags) > 0:
            snapshot = self.resource.Snapshot(response.get('SnapshotId'))
            snapshot.create_tags(Tags=tags)


class AutoscalingClient(BaseClient):

    def complete_lifecycle_action(self, hook_name, group_name, token, result, instance_id):
        self.logger.debug('Completing lifecycle action for %s with %s', instance_id, result)
        try:
            _ = self.client.complete_lifecycle_action(
                LifecycleHookName = hook_name,
                AutoScalingGroupName = group_name,
                LifecycleActionToken = token,
                LifecycleActionResult = result,
                InstanceId = instance_id
            )
        except ClientError as e:
            self.logger.exception("Failed to complete lifecycle action: %s", repr(e))


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

        return { }


    def get_activity(self, group, is_launching, instance_id):
        activities = self.client.describe_scaling_activities(
            AutoScalingGroupName = group
        )['Activities']

        if is_launching:
            desc = "Launching a new EC2 instance: " + instance_id
        else:
            desc = "Terminating EC2 instance: " + instance_id

        for activity in activities:
            if activity.get('Description') == desc:
                return activity

        return { }


    def wait_for_activity_to_complete(self, group: str, is_launching: bool, instance_id: str):
        self.logger.debug('Autoscaling: Waiting for autoscaling activity to complete.')

        try:
            self.waiters.get_autoscaling_complete_for(instance_id, is_launching).wait(
                AutoScalingGroupName = group
            )
        except WaiterError:
            msg = 'Autoscaling: Error while waiting for autoscaling activity to complete: Activity not found for %s in %s'
            self.logger.exception(msg, instance_id, group)


class DynamoDbClient(BaseClient):
    """
    Proxy for get_item, delete_item, scan etc. calls to the dynamodb service client
    Parameters and returned data is transfomed from/to dynamodb data structure automatically
    """


    def __init__(self, client: BotoClient, waiters: CustomWaiters, logging: Logging, *args):
        super().__init__(client, waiters, logging)
        state_table, = args
        self.state_table = state_table


    def get_state_table(self) -> str:
        return self.state_table


    def convert_expression_attribute_values(self, attribute_values: dict) -> dict:
        converted_values = { }
        for k, v in attribute_values.items():
            converted_values.update({ k: self.__build_dynamodb_value(v) })

        return converted_values


    def scan(self, expression: str, attribute_values: dict):
        converted_items = []

        items = self.client.scan(
            TableName = self.state_table,
            FilterExpression = expression,
            ExpressionAttributeValues = self.convert_expression_attribute_values(attribute_values)
        ).get('Items')

        for item in items:
            converted_items.append(self.__convert_dynamodb_map_to_dict(item))

        return converted_items


    def get_item(self, id):
        try:
            item = self.client.get_item(
                TableName = self.state_table,
                Key = self.__build_dynamodb_key(id)
            ).get('Item')
        except Exception as e:
            self.logger.warning('Could not get item %s; %s', id, repr(e))
            return { }

        if type(item) is dict:
            return self.__convert_dynamodb_map_to_dict(item)

        return { }


    def delete_item(self, id):
        self.logger.info('Removing item %s from db', id)
        _ = self.client.delete_item(
            TableName = self.state_table,
            Key = self.__build_dynamodb_key(id)
        )


    def put_item(self, id, item_type, data):
        self.logger.info('Put %s item to db %s with values %s', item_type, id, data)
        _ = self.client.put_item(
            TableName = self.state_table,
            Item = self.__build_dynamodb_item(id, item_type, data)
        )


    def update_item(self, id: str, expression: str, values: dict = None):
        self.logger.info('Updating item %s with %s', id, values)

        if type(values) is dict:
            for k, v in values.items():
                values.update({ k: self.__build_dynamodb_value(v) })

        _ = self.client.update_item(
            TableName = self.state_table,
            Key = self.__build_dynamodb_key(id),
            UpdateExpression = expression,
            ExpressionAttributeValues = values
        )


    def unset(self, id: str, properties: list):
        self.logger.debug('Removing %s from instance %s', properties, id)
        _ = self.client.update_item(
            TableName = self.state_table,
            Key = self.__build_dynamodb_key(id),
            UpdateExpression = 'REMOVE ' + ','.join(properties)
        )


    def __build_dynamodb_item(self, ident: str, item_type: str, data: dict) -> dict:
        """
        Build a node item to be used with put_item()

        :type ident: str
        :param ident: The identifier for the item

        :type item_type: str
        :param item_type: The type of the item

        :type data: dict
        :param data: The item data

        :rtype: dict
        :return: The item
        """

        data.update({ 'ItemType': item_type })

        item = self.__convert_dict_to_dynamodb_map(data)
        item.update(self.__build_dynamodb_key(ident))

        return item


    def __build_dynamodb_key(self, id):
        """

        :param id:
        :return:
        """

        return { 'Ident': { 'S': id } }


    def __build_dynamodb_value(self, value, log = True):
        """

        :param id:
        :return:
        """

        if type(value) is str:
            if value is '':
                return None
            return { 'S': value }

        elif type(value) is dict:
            value = self.__convert_dict_to_dynamodb_map(value, log)
            if value is { }:
                return None
            return { 'M': value }

        value = repr(value)
        if value is '':
            return None

        return { 'S': value }


    def __convert_dict_to_dynamodb_map(self, data: dict, log = True) -> dict:
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
            self.logger.debug('Converting dict to dynamodb item: %s', data)

        dynamodb_map = { }
        for key, value in data.items():
            value = self.__build_dynamodb_value(value, False)
            if value is not None:
                dynamodb_map.update({ key: value })

        if log:
            self.logger.debug('Result: %s', dynamodb_map)

        return dynamodb_map


    def __convert_dynamodb_map_to_dict(self, dynamodb_map: dict, log = True) -> dict:
        """
        Convert a dynamodb map to dict. Convertable types:
        - 'S' -> str
        - 'M' -> dict

        :type dynamodb_map: dict
        :param dynamodb_map:

        :rtype: dict
        :return: The converted data
        """
        if log:
            self.logger.debug('Converting dynamodb item to dict: %s', dynamodb_map)

        data = { }
        for key, value in dynamodb_map.items():
            if value.get('S', None) is not None:
                data.update({ key: value.get('S') })
            elif value.get('M', None) is not None:
                data.update({ key: self.__convert_dynamodb_map_to_dict(value.get('M'), False) })
            else:
                self.logger.warning('Cannot convert %s. Ignoring. Valid types are M,S. Value: %s', key, value)

        if log:
            self.logger.debug('Result: %s', data)

        return data


    def wait_for_scan_count_is(self, size: int, expression: str, attribute_values: dict):
        self.logger.debug('Waiting for scan %s to return %s items.', expression, size)
        self.waiters.get_dynamodb_scan_count_is(size).wait(
            TableName = self.get_state_table(),
            FilterExpression = expression,
            ExpressionAttributeValues = self.convert_expression_attribute_values(attribute_values)
        )


    def wait_for_scan_count_gt0(self, expression: str, attribute_values: dict):
        self.logger.debug('Waiting for scan %s to return at leat one item.', expression)
        self.waiters.get('ScanCountGt0').wait(
            TableName = self.get_state_table(),
            FilterExpression = expression,
            ExpressionAttributeValues = self.convert_expression_attribute_values(attribute_values)
        )


class Route53Client(BaseClient):
    dns_change_set = []


    def reset_dns_change_set(self):
        self.dns_change_set = []


    def add_dns_change_set(self, name: str, records: list, ttl: int, action: str = 'UPSERT'):
        self.logger.info('Add dns entry %s with %s to change set.', name, records)
        self.dns_change_set.append({
            'Action': action,
            'ResourceRecordSet': {
                'Name': name,
                'Type': 'A',
                'TTL': ttl,
                'ResourceRecords': records
            }
        })


    def apply_dns_change_set(self, zone_id):
        self.logger.debug("Updating DNS records in zone %s: %s", zone_id, self.dns_change_set)
        _ = self.client.change_resource_record_sets(
            HostedZoneId = zone_id,
            ChangeBatch = { 'Changes': self.dns_change_set }
        )
        self.reset_dns_change_set()


class SnsClient(BaseClient):

    def __init__(self, client: BotoClient, waiters: CustomWaiters, logging: Logging, *args):
        super().__init__(client, waiters, logging)
        client_eu_west, topic_arn, account, env = args
        self.client_eu_west = client_eu_west
        self.topic_arn = topic_arn
        self.account = account
        self.env = env


    def publish_autoscaling_activity(self, activity, region = "eu-central-1"):
        severity = 'INFO'
        if activity.get('StatusCode') == 'Successful':
            severity = "SUCCESS"

        subject = self.formatter.format("%s : %s in %s", [severity, activity.get('Description'), self.env])
        result = self.formatter.to_str(activity)
        message = self.formatter.to_str({
            'default': result,
            'sms': subject,
            'email': subject + ":\n\n" + result
        })
        self.publish(subject, message, region)


    def publish_activity(self, status, subject, detail, region = "eu-central-1"):
        subject = self.formatter.format("%s : %s", [status, subject])
        message = self.formatter.to_str({
            'default': detail,
            'sms': detail,
            'email': detail
        })
        self.publish(subject, message, region)


    def publish_error(self, exception, action, region = "eu-central-1"):
        subject = self.formatter.format(
            'ERROR : while performing %s in environment %s: %s',
            [repr(action), self.env, repr(exception)]
        )
        result = self.formatter.to_str(ExceptionInfo.from_current().to_dict())
        message = self.formatter.to_str({ 'default': result })
        self.publish(subject, message, region)


    def publish(self, subject, message, region):
        if self.topic_arn != "":
            if region == "eu-west-1":
                client = self.client_eu_west
            else:
                client = self.client
            return client.publish(
                TargetArn = self.topic_arn,
                Message = message,
                Subject = subject[:100],
                MessageStructure = 'json'
            )
        else:
            self.logger.warning('Cannot send report. No topic provided.')
            return False


class SsmClient(BaseClient):

    def send_command(self, instance_ids, comment, commands, timeout_in_seconds = 60):
        if type(instance_ids) is not list:
            instance_ids = [instance_ids]

        self.logger.debug('Sending command "%s" to instance %s: %s', comment, instance_ids, commands)

        self.logger.debug('Waiting for ssm agent to become ready.')
        # can be replaced when https://github.com/boto/botocore/pull/1502 will be accepted
        # waiter = ssm.get_waiter['AgentIsOnline']
        self.waiters.get('AgentIsOnline').wait(
            Filters = [{ 'Key': 'InstanceIds', 'Values': instance_ids }]
        )

        command_id = self.client.send_command(
            InstanceIds = instance_ids,
            DocumentName = 'AWS-RunShellScript',
            Comment = self.logger.name + ' : ' + comment,
            Parameters = {
                'commands': commands
            },
            TimeoutSeconds = timeout_in_seconds
        ).get('Command').get('CommandId')
        self.logger.debug('Command "%s" on instance %s is running: %s', comment, instance_ids, command_id)

        return command_id


class SecretsmanagerClient(BaseClient):

    def get_current_secret_string(self, secret_id):
        self.logger.debug('Secretsmanager: Fetch secret: %s', secret_id)
        secret = self.client.get_secret_value(SecretId=secret_id)
        secret_string = secret.get("SecretString", "")
        if secret_string == "":
            self.logger.warning('Secretsmanager: Could not fetch secret: %s', secret_id)

        return secret_string
