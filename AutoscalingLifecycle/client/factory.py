from boto3 import Session
from botocore.client import BaseClient

from AutoscalingLifecycle.helper.logger import LifecycleLogger


class ClientFactory(object):
	"""
	A simple class that creates boto3 service clients. Each client will be created only once
	and than returned from local cache
	"""


	def __init__(self, session: Session, logger: LifecycleLogger):
		"""

		:param session: A boto3 Session instamce
		:type session: Session
		:param logger: A LifecycleLogger instance
		:type logger: LifecycleLogger
		"""
		self.session = session
		self.logger = logger
		self.clients = { }


	def get(self, name: str, region_name: str = 'eu-central-1'):
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

		self.logger.info('Retrieving client %s in region %s', name, region_name)
		key = name + '_' + region_name
		client = self.clients.get(key, None)
		if client is None:
			self.logger.debug('Client %s in region %s not created. Creating ...', name, region_name)
			client = self.session.client(name, region_name = region_name)
			self.clients.update({ key: client })

		return client
