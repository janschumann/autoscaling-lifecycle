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

		:rtype: BaseClient
		:return: Service client instance
		"""

		self.logger.info('Retrieving client %s', name)

		client = self.clients.get(name, None)
		if client is None:
			self.logger.debug('Client %s not created. Creating ...', name)
			client = self.session.client(name)
			self.clients.update({ name: client })

		return client
