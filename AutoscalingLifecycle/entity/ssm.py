from AutoscalingLifecycle.entity.event import Event


class SsmEvent(Event):

	@property
	def command_id(self) -> str:
		return self.detail.get('command-id')


	@property
	def document_name(self) -> str:
		return self.detail.get('document-name')


	@property
	def parameters(self) -> dict:
		return self.detail.get('parameters')


	@property
	def request_time(self) -> str:
		return self.detail.get('requested-date-time')


	@property
	def expire_time(self) -> str:
		return self.detail.get('expire-time')


	@property
	def status(self) -> str:
		return self.detail.get('status')
