class Event(object):

	def __init__(self, event: dict):
		self.event = event
		self.detail = event.get('detail')


	@property
	def version(self) -> str:
		return self.event.get('version')


	@property
	def id(self) -> str:
		return self.event.get('id')


	@property
	def detail_type(self) -> str:
		return self.event.get('detail-type')


	@property
	def source(self) -> str:
		return self.event.get('source')


	@property
	def account(self) -> str:
		return self.event.get('account')


	@property
	def time(self) -> str:
		return self.event.get('time')


	@property
	def region(self) -> str:
		return self.event.get('region')


	@property
	def resources(self) -> list:
		return self.event.get('resources')
