import json
import logging

from transitions import EventData

from AutoscalingLifecycle.event import AutoscalingEvent
from AutoscalingLifecycle.event import SsmEvent
from AutoscalingLifecycle.state import StateHandler
from AutoscalingLifecycle.entity.node import Node
from AutoscalingLifecycle.helper.logger import LifecycleLogger


class Docker(StateHandler):

    def _get_transitions(self):
        return {
            'launching': [
                {
                    'source': 'new',
                    'dest': 'pending',
                    'operations': [
                        {
                            'name': 'register',
                            'before': [self.do_register],
                        }
                    ]
                },
                {
                    'source': 'pending',
                    'dest': 'initializing',
                    'operations': [
                        {
                            'name': 'initialize',
                            'conditions': [self.is_manager],
                            'unless': [self.is_cluster_ready],
                            'before': [self.do_initialize],
                        },
                        {
                            'name': 'join',
                            'before': [self.do_join],
                        },
                    ]
                },
                {
                    'source': 'initializing',
                    'dest': 'ready',
                    'operations': [
                        {
                            'name': 'add_labels',
                            'conditions': [self.is_worker],
                            'before': [self.do_add_labels],
                        },
                        {
                            'name': 'update_swarm_dns',
                            'before': [self.do_update_swarm_dns],
                        }
                    ]
                },
                {
                    'source': 'ready',
                    'dest': 'running',
                    'operations': [
                        {
                            'name': 'rebalance_services',
                            'conditions': [self.is_worker],
                            'before': [self.do_rebalance_services],
                        },
                        {
                            'name': 'complete',
                            'before': [self.do_complete],
                        }
                    ]
                }
            ],
            'terminating': [
                {
                    'source': 'running',
                    'dest': 'terminating',
                    'operations': [
                        {
                            'name': 'remove_from_cluster',
                            'before': [self.do_remove_from_cluster],
                        }
                    ]
                },
                {
                    'source': 'terminating',
                    'dest': 'terminated',
                    'operations': [
                        {
                            'name': 'update_swarm_dns',
                            'before': [self.do_update_swarm_dns],
                        },
                        {
                            'name': 'complete',
                            'before': [self.do_complete],
                        }
                    ]
                },
                {
                    'source': 'terminated',
                    'dest': 'removed',
                    'operations': [
                        {
                            'name': 'remove',
                            'after': [self.do_remove],
                        }
                    ]
                }
            ]
        }


    def is_manager(self, event_data: EventData):
        return event_data.args[0].get_type() == 'manager'


    def is_worker(self, event_data: EventData):
        return event_data.args[0].get_type() == 'worker'


    def do_add_labels(self, event_data: EventData):
        self.logger.info('adding labels on node %s', event_data.args[0].to_dict())


    def do_complete(self, event_data: EventData):
        self.logger.info('completing autoscaling action for node %s', event_data.args[0].to_dict())


    def do_rebalance_services(self, event_data: EventData):
        self.logger.info('rebalancing services due to autoscaling action in on node %s',
                         event_data.args[0].to_dict())


    def do_update_swarm_dns(self, event_data: EventData):
        self.logger.info('updating dns on node %s', event_data.args[0].to_dict())


    def do_remove(self, event_data: EventData):
        self.logger.info('removing node %s from db', event_data.args[0].to_dict())


    def do_remove_from_cluster(self, event_data: EventData):
        self.logger.info('removing node %s from cluster', event_data.args[0].to_dict())


    def do_register(self, event_data: EventData):
        self.logger.info('registering node %s', event_data.args[0].to_dict())


    def do_join(self, event_data: EventData):
        self.logger.info('waiting for cluster to become ready')

        self.logger.info('joining node %s', event_data.args[0].to_dict())
        self._proceed = False


    def do_initialize(self, event_data: EventData):
        self.logger.info('initializing cluster on node %s', event_data.args[0].to_dict())
        self._proceed = False


    def is_cluster_ready(self, event_data: EventData):
        self.logger.info('checking if cluster is ready')
        result = False
        return result


class NodeRepository(object):
    def __init__(self):
        self.nodes = { }
        _node = Node('existing_node', 'worker')
        _node.set_state('ready')
        self.nodes.update({ _node.id: _node })


    def get(self, id):
        if id in self.nodes.keys():
            return self.nodes.get(id)

        _node = Node(id, 'worker')
        return _node


node_repository = NodeRepository()

message = json.load(open('autoscaling_event.json', 'r'))
# message = json.load(open('ssm_event.json', 'r'))

l = logging.getLogger('docker')
l.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
l.addHandler(ch)
logger = LifecycleLogger('DOCKER-SWARM::BACKEND::QA', l)
logger.info('message %s', message)

e = None
if message.get('source') == 'aws.autoscaling':
    e = AutoscalingEvent(message)
elif message.get('source') == 'aws.ssm':
    command = {
        "LifecycleActionToken": "c613620e-07e2-4ed2-a9e2-ef8258911ade",
        "AutoScalingGroupName": "sampleASG",
        "LifecycleHookName": "SampleLifecycleHook-12345",
        "EC2InstanceId": "existing_node",
        "LifecycleTransition": "autoscaling:EC2_INSTANCE_TERMINATING",
        "NotificationMetadata": {
            "type": "worker"
        }
    }
    e = SsmEvent(message, command)

h = Docker(e, { }, { 'node': node_repository }, logger)
h()
