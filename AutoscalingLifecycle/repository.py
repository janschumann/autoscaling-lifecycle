from logging import Logger

from . import Node
from .clients import DynamoDbClient


class Repositories(object):

    __repositoroes = {}

    def __init__(self, client: DynamoDbClient, logger: Logger):
        self.client = client
        self.logger = logger


    def add(self, name, cls):
        self.__repositoroes.update({name: cls(self.client, self.logger)})


    def get(self, name):
        repository = self.__repositoroes.get(name, None)
        if repository is None:
            raise RuntimeError("No repository %s", name)

        return repository


class CommandRepository(object):

    def __init__(self, client: DynamoDbClient, logger: Logger):
        self.client = client
        self.logger = logger


    def register(self, id: str, data: dict):
        self.client.put_item(id, 'command', data)


    def get(self, id: str):
        return self.client.get_item(id)


    def delete(self, id: str):
        self.client.delete_item(id)


class NodeRepository(object):

    def __init__(self, client: DynamoDbClient, logger: Logger):
        self.client = client
        self.logger = logger


    def register(self, id: str, node_type: str, data: dict) -> Node:
        node = Node(id, node_type)
        for k, v in data.items():
            node.set_property(k, v)

        self.put(node)

        return node


    def put(self, node):
        self.client.put_item(node.id, node.type, node.data)


    def get(self, id: str):
        item = self.client.get_item(id)
        if item == { }:
            node = Node(id, 'unknown')
        else:
            node = Node(item.pop('EC2InstanceId'), item.pop('ItemType'))
            node.set_status(item.pop('ItemStatus'))

        for k, v in item.items():
            node.set_property(k, v)

        return node


    def unset_property(self, node: Node, properties: list):
        for p in properties:
            node.unset_property(p)

        self.client.unset(node.get_id(), properties)


    def update(self, node: Node, changes: dict):
        parts = []
        values = { }
        for k, v in changes.items():
            node.set_property(k, v)
            parts.append(' ' + k + ' = :' + k)
            values.update({ ':' + k: node.get_property(k) })

        expression = 'SET' + ','.join(parts)

        self.client.update_item(node.get_id(), expression, values)


    def delete(self, node: Node):
        self.client.delete_item(node.get_id())


    def get_by_type(self, types: list, additional_filter: str = None, attribute_values: dict = None,
                    include_terminating: bool = False):
        """
        Fetch nodes by type and add custom filters.

        :param types:
        :param additional_filter:
        :param attribute_values:
        :return:
        """
        self.logger.info('Loading nodes of type %s with filter %s and values %s', types, additional_filter,
                         attribute_values)

        filter = ''
        if not include_terminating:
            filter = 'and ItemStatus <> :terminating and ItemStatus <> :removing'

        if additional_filter is None and attribute_values is not None:
            raise RuntimeError('Filter is not set but attribute values are given.')
        elif additional_filter is not None and attribute_values is None:
            raise RuntimeError('Filter is set but no attribute values are given.')
        elif additional_filter is not None and attribute_values is not None:
            filter = filter + ' and (' + additional_filter + ')'
        elif attribute_values is None:
            attribute_values = { }

        if not include_terminating:
            attribute_values.update({ ':terminating': 'terminating' })
            attribute_values.update({ ':removing': 'removing' })

        parts = []
        for index, node_type in enumerate(types):
            attribute_values.update({ ':node_type' + str(index): node_type })
            parts.append('ItemType = :node_type' + str(index))
        expression = '(' + ' or '.join(parts) + ') ' + filter

        items = self.client.scan(expression, attribute_values)

        nodes = []
        for item in items:
            node = Node(item.pop('EC2InstanceId'), item.pop('ItemType'))
            node.set_status(item.pop('ItemStatus'))
            for k, v in item.items():
                node.set_property(k, v)
            nodes.append(node)

        return nodes
