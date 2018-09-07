from lib.client.dynamodb import DynamoDbClient
from lib.helper.logger import LifecycleLogger


class CommandRepository(object):

	def __init__(self, client: DynamoDbClient, logger: LifecycleLogger):
		self.client = client
		self.logger = logger


	def register(self, id: str, data: dict):
		self.client.put_item(id, 'command', data)


	def get(self, id: str):
		return self.client.get_item(id)


	def delete(self, id: str):
		self.client.delete_item(id)
