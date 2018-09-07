import json


class DynamoDbClient(object):
	"""
	Proxy for get_item, delete_item, scan etc. calls to the dynamodb service client
	Parameters and returned data is transfomed from/to dynamodb data structure automatically
	"""


	def __init__(self, client, state_table, logger, waiters):
		self.client = client
		self.state_table = state_table
		self.logger = logger
		self.waiters = waiters


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
			return { 'S': value }

		elif type(value) is dict:
			return { 'M': self.__convert_dict_to_dynamodb_map(value, log) }

		else:
			self.logger.warning(
				'Cannot convert type %s to a dynamodb equivalent. Value will be empty. Valid types are str, dict. Value: %s',
				type(value), json.dumps(value, ensure_ascii = False))

		return { 'S': '' }


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
			self.logger.debug('Converting dict to dynamodb item: %s', json.dumps(data, ensure_ascii = False))

		dynamodb_map = { }
		for key, value in data.items():
			dynamodb_map.update({ key: self.__build_dynamodb_value(value, False) })

		if log:
			self.logger.debug('Result: %s', json.dumps(dynamodb_map, ensure_ascii = False))

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
			self.logger.debug('Converting dynamodb item to dict: %s', json.dumps(dynamodb_map, ensure_ascii = False))

		data = { }
		for key, value in dynamodb_map.items():
			if value.get('S', None) is not None:
				data.update({ key: value.get('S') })
			elif value.get('M', None) is not None:
				data.update({ key: self.__convert_dynamodb_map_to_dict(value.get('M'), False) })
			else:
				self.logger.warning('Cannot convert %s. Ignoring. Valid types are M,S. Value: %s', key,
									json.dumps(value, ensure_ascii = False))

		if log:
			self.logger.debug('Result: %s', json.dumps(data, ensure_ascii = False))

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
