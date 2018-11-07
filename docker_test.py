import json
import logging

from transitions import EventData
from boto3 import Session

from AutoscalingLifecycle import ClientFactory
from AutoscalingLifecycle import CustomWaiters
from AutoscalingLifecycle import AutoscalingEvent
from AutoscalingLifecycle import Node
from AutoscalingLifecycle import SsmEvent
from AutoscalingLifecycle.logging import Logging
from AutoscalingLifecycle.logging import SnsHandler
from AutoscalingLifecycle.state import StateHandler


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
                    'dest': 'labeled',
                    'operations': [
                        {
                            'name': 'add_labels',
                            'conditions': [self.is_worker],
                            'before': [self.do_add_labels],                            'internal': True
                        }
                    ]
                },
                {
                    'source': ['initializing', 'labeled'],
                    'dest': 'online',
                    'operations': [
                        {
                            'name': 'update_swarm_dns',
                            'after': [self.do_update_swarm_dns],
                        }
                    ]
                },
                {
                    'source': 'online',
                    'dest': 'ready',
                    'operations': [
                        {
                            'name': 'complete',
                            'before': [self.do_complete],
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
                        }
                    ]
                }
            ],
            'terminating': [
                {
                    'source': '*',
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
        return self._node.get_type() == 'manager'


    def is_worker(self, event_data: EventData):
        return self._node.get_type() == 'worker'


    def do_add_labels(self, event_data: EventData):
        self.logger.info('adding labels on node %s', self._node.to_dict())
        self._proceed = False

    def do_complete(self, event_data: EventData):
        self.logger.info('completing autoscaling action for node %s', self._node.to_dict())


    def do_rebalance_services(self, event_data: EventData):
        self.logger.info('rebalancing services due to autoscaling action in on node %s',
                         self._node.to_dict())


    def do_update_swarm_dns(self, event_data: EventData):
        self.logger.info('updating dns on node %s', self._node.to_dict())


    def do_remove(self, event_data: EventData):
        self.logger.info('removing node %s from db', self._node.to_dict())


    def do_remove_from_cluster(self, event_data: EventData):
        self.logger.info('removing node %s from cluster', self._node.to_dict())


    def do_register(self, event_data: EventData):
        self.logger.info('registering node %s', self._node.to_dict())


    def do_join(self, event_data: EventData):
        self.logger.info('waiting for cluster to become ready')

        self.logger.info('node %s is joining the cluster', self._node.to_dict())
        self._proceed = False


    def do_initialize(self, event_data: EventData):
        self.logger.info('initializing cluster on node %s', self._node.to_dict())
        self._proceed = False


    def is_cluster_ready(self, event_data: EventData):
        self.logger.info('checking if cluster is ready')
        result = False
        return result


class NodeRepository(object):
    def __init__(self):
        self.nodes = { }
        _node = Node('existing_node', 'worker')
        _node.set_state('labeled')
        self.nodes.update({ _node.id: _node })


    def get(self, id):
        if id in self.nodes.keys():
            return self.nodes.get(id)

        _node = Node(id, 'manager')
        _node.set_state('pending')
        return _node


    def update(self, node: Node, changes: dict):
        parts = []
        values = { }
        for k, v in changes.items():
            node.set_property(k, v)
            parts.append(' ' + k + ' = :' + k)
            values.update({ ':' + k: node.get_property(k) })


node_repository = NodeRepository()

message = json.load(open('autoscaling_event.json', 'r'))
#message = json.load(open('ssm_event.json', 'r'))

f = Logging('DOCKER-SWARM::BACKEND::QA', logging.DEBUG)
f.add_handler(logging.StreamHandler(), "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")

client_factory = ClientFactory(Session(profile_name='7nxt-tooling-live'), f.get_logger())
waiters = CustomWaiters(client_factory, f.get_logger())

#h = SnsHandler(client_factory.get('sns', 'eu-west-1'), "arn:aws:sns:eu-west-1:676446623848:autoscaling")
#f.add_handler(h, '%(message)s')

l = f.get_logger()

l.info('message %s', message)

e = None
if message.get('source') == 'aws.autoscaling':
    e = AutoscalingEvent(message)
elif message.get('source') == 'aws.ssm':
    command = {
        "LifecycleActionToken": "c613620e-07e2-4ed2-a9e2-ef8258911ade",
        "AutoScalingGroupName": "sampleASG",
        "LifecycleHookName": "SampleLifecycleHook-12345",
        "EC2InstanceId": "existing_node",
        "LifecycleTransition": "autoscaling:EC2_INSTANCE_LAUNCHING",
        "NotificationMetadata": {
            "type": "manager"
        }
    }
    e = SsmEvent(message, command)

h = Docker(e, { }, { 'node': node_repository }, f)
h()
# h.machine.get_graph(show_roi=True).draw('my_state_diagram.png', prog='dot')
