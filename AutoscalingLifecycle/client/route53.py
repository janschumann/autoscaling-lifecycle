class Route53Client(object):
	dns_change_set = []


	def __init__(self, client, waiters, logger):
		self.client = client
		self.waiters = waiters
		self.logger = logger
		self.reset_dns_change_set()


	def reset_dns_change_set(self):
		self.dns_change_set = []


	def add_dns_change_set(self, name: str, records: list, ttl: int):
		self.logger.info('Add dns entry %s with %s to change set.', name, records)
		self.dns_change_set.append({
			'Action': 'UPSERT',
			'ResourceRecordSet': {
				'Name': name,
				'Type': 'A',
				'TTL': ttl,
				'ResourceRecords': records
			}
		})


	def apply_dns_change_set(self, zone_id):
		self.logger.debug("Updating DNS records in zone %s: %s", zone_id, self.dns_change_set)
		_ = self.client.change_resource_record_sets(
			HostedZoneId = zone_id,
			ChangeBatch = { 'Changes': self.dns_change_set }
		)
		self.reset_dns_change_set()
