import botocore.client
import botocore.waiter

from AutoscalingLifecycle.client.factory import ClientFactory
from AutoscalingLifecycle.helper.logger import LifecycleLogger


class Waiters(object):
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


	def __init__(self, clients: ClientFactory, logger: LifecycleLogger):
		"""
		:type clients: ClientFactory
		:param clients:
		:type logger: LifecycleLogger
		:param logger:
		"""
		self.clients = clients
		self.logger = logger


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
			raise self.logger.get_error(KeyError, 'Waiter %s does not exist', name)

		self.waiters.update({ name: botocore.waiter.create_waiter_with_client(name, model, client) })
