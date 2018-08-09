import botocore.waiter


class Waiters(object):
	"""
	A collection of waiter definitions

	:type action: EventAction
	:param action: An event action object

	:type models: dict
	:param models: A collection of waiter definitions
	"""
	action = None
	models = {
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
						"delay": 15,
						"operation": "DescribeAutoScalingInstances",
						"maxAttempts": 40,
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
						"delay": 15,
						"operation": "DescribeInstanceInformation",
						"maxAttempts": 40,
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


	def __init__(self, action):
		"""
		:type action: EventAction
		:param action:
		"""
		self.action = action


	def get_waiter_names(self):
		"""
		:rtype: list
		:return: A list of waiter names
		"""
		return self.models.keys()


	def get(self, name):
		"""
		:type name: str
		:param name: The name of the waiter

		:rtype: botocore.waiter.Waiter
		:return: The waiter object.
		"""
		if name not in self.models.keys():
			raise self.action.get_error(KeyError, 'Waiter %s does not exist', name)

		data = self.models.get(name)
		model = botocore.waiter.WaiterModel(data.get('model'))
		client = self.action.get_client(data.get('client'))

		return botocore.waiter.create_waiter_with_client(name, model, client)


	def get_dynamodb_scan_count_is(self, size):
		model = botocore.waiter.WaiterModel({
			"version": 2,
			"waiters": {
				"ScanCountIs" + str(size): {
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

		return botocore.waiter.create_waiter_with_client("ScanCountIs" + str(size), model,
														 self.action.get_client('dynamodb'))
