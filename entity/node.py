class Node(object):
	id = None
	type = None
	status = 'pending'
	data = {}
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
		self.data = {}
		self.id = id
		self.data.update({'EC2InstanceId': self.id})
		self.type = node_type
		self.data.update({'ItemType': self.type})
		self.data.update({'ItemStatus': self.status})


	def get_id(self):
		return self.id


	def get_type(self):
		return self.type


	def get_status(self):
		return self.status


	def set_status(self, status):
		self.status = status
		self.data.update({'ItemStatus': self.status})


	def get_property(self, property, default=None):
		return self.data.get(property, default)


	def set_property(self, property, value):
		if property in self.readonly_propertoes:
			raise TypeError(property + ' is read only.')

		if property == 'ItemStatus':
			self.set_status(value)
		else:
			self.data.update({property: value})


	def unset_property(self, property):
		if property in self.mandatory_propertoes:
			raise TypeError(property + ' cannot be unset.')

		_ = self.data.pop(property)


	def to_dict(self):
		return {
			'id': self.id,
			'type': self.type,
			'status': self.status,
			'data': self.data
		}
