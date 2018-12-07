from logging import Logger

from .clients import DynamoDbClient


class Repository(object):
    def __init__(self, client: DynamoDbClient, logger: Logger):
        self.client = client
        self.logger = logger


class Repositories(Repository):
    __repositories = { }


    def set(self, name: str, repo: Repository):
        self.__repositories.update({ name: repo })


    def add(self, name, cls):
        self.__repositories.update({ name: cls(self.client, self.logger) })


    def get(self, name):
        repository = self.__repositories.get(name, None)
        if repository is None:
            raise RuntimeError("No repository %s", name)

        return repository


class CommandRepository(Repository):

    def register(self, id: str, data: dict):
        self.client.put_item(id, 'command', data)


    def get(self, id: str):
        return self.client.get_item(id)


    def pop(self, id: str):
        command = self.get(id)
        if command == { }:
            raise RuntimeError('Could not load command %s.' % id)
        self.delete(id)

        return command


    def delete(self, id: str):
        self.client.delete_item(id)


class Node(object):
    id = None
    data = { }


    def __init__(self, id, node_type = 'unknown'):
        if id == "" or id is None:
            raise TypeError("id must not be empty")

        self.id = id
        self.data = { }
        self.data.update({ 'ItemType': node_type })
        self.data.update({ 'ItemStatus': 'new' })


    def get_id(self):
        return self.id


    def get_type(self):
        return self.data.get('ItemType')


    def set_type(self, node_type):
        self.data.update({ 'ItemType': node_type })


    def get_status(self):
        return self.data.get('ItemStatus')


    def set_status(self, status):
        self.data.update({ 'ItemStatus': status })


    def has_property(self, property):
        return self.data.get(property, False) is not False


    def get_property(self, property, default = None):
        return self.data.get(property, default)


    def set_property(self, property, value):
        self.data.update({ property: value })


    def unset_property(self, property):
        _ = self.data.pop(property)


    def is_valid(self):
        return self.id != ''


    def to_dict(self):
        return {
            'id': self.id,
            'type': self.get_type(),
            'status': self.get_state(),
            'data': self.data
        }


    def set_state(self, dest):
        self.set_status(dest)


    def get_state(self) -> str:
        return self.get_status()


    def is_new(self) -> bool:
        return self.get_state() in ['new', 'pending', 'finished_cloud_init']


    def set_id(self, ident):
        self.id = ident


class NodeRepository(Repository):

    def put(self, node: Node):
        self.client.put_item(node.id, node.get_type(), node.data)


    def get(self, id: str):
        node = Node(id, 'unknown')
        item = self.client.get_item(id)
        if item != { }:
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
            node = Node(item.pop('Ident'), item.pop('ItemType'))
            node.set_status(item.pop('ItemStatus'))
            for k, v in item.items():
                node.set_property(k, v)
            nodes.append(node)

        return nodes
