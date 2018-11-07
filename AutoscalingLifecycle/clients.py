from logging import Logger

from boltons.tbutils import ExceptionInfo
from botocore.client import BaseClient as BotoClient

from . import CustomWaiters
from . import ClientFactory
from .logging import Logging
from .logging import MessageFormatter


class Clients(object):
    """
    lazy load clients when needed
    :param __client_specs: A dict of client specifications
    :type __client_specs: dict
    :param __clients: A dict of client instances
    :type __clients: dict
    """
    __client_specs = {}
    __clients = {}


    def __init__(self, cfactory: ClientFactory, waiters: CustomWaiters, logging: Logging):
        self.cfactory = cfactory
        self.waiters = waiters
        self.logging = logging


    def add_client_spec(self, name, cls, *args):
        self.__client_specs.update({name: {
            'class': cls,
            'args': args
        }})


    def get(self, name):
        spec = self.__client_specs.get(name, None)
        if spec is None:
            raise RuntimeError("no specs for %s found" % name)

        client = self.__clients.get('name', None)
        if client is None:
            client = spec.get('class')(self.cfactory.get(name), self.waiters, self.logging, *spec.get('args'))

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


class AutoscalingClient(BaseClient):

    def set_transition(self, transition):
        self.transition = transition
        if self.transition != 'autoscaling:EC2_INSTANCE_LAUNCHING' and self.transition != 'autoscaling:EC2_INSTANCE_TERMINATING':
            raise self.formatter.get_error(TypeError, 'Unknown autoscaling transition %s', self.transition)

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
            raise self.formatter.get_error(TypeError, 'Transition not set')


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

        return { }


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


    def add_dns_change_set(self, name: str, records: list, ttl: int):
        self.logger.info('Add dns entry %s with %s to change set.', name, records)
        self.dns_change_set.append({
            'Action': 'UPSERT',
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


    def publish_activity(self, action, instance_id, region = "eu-central-1"):
        subject = self.formatter.format("SUCCESS : Finished %s on %s", [action, instance_id])
        message = self.formatter.to_str({
            'default': subject,
            'sms': subject,
            'email': subject
        })
        self.publish(subject, message, region)


    def publish_error(self, exception, action, region = "eu-central-1"):
        subject = self.formatter.format(
            'ERROR : while performing %s in environment %s: %s',
            [action, self.env, repr(exception)]
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

    def send_command(self, instance_id, comment, commands):
        self.logger.debug('Sending command "%s" to instance %s: %s', comment, instance_id, commands)

        self.logger.debug('Waiting for ssm agent to become ready.')
        # can be replaced when https://github.com/boto/botocore/pull/1502 will be accepted
        # waiter = ssm.get_waiter['AgentIsOnline']
        self.waiters.get('AgentIsOnline').wait(
            Filters = [{ 'Key': 'InstanceIds', 'Values': [instance_id] }]
        )

        command_id = self.client.send_command(
            InstanceIds = [instance_id],
            DocumentName = 'AWS-RunShellScript',
            Comment = self.logger.name + ' : ' + comment,
            Parameters = {
                'commands': commands
            }
        ).get('Command').get('CommandId')
        self.logger.debug('Command "%s" on instance %s is running: %s', comment, instance_id, command_id)

        return command_id
