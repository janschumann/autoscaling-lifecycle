import logging
import json
from transitions import Machine
from transitions import State
from transitions import EventData
from AutoscalingLifecycle.helper.logger import LifecycleLogger
from AutoscalingLifecycle.base import Event
from AutoscalingLifecycle.base import AutoscalingEvent
from AutoscalingLifecycle.base import SsmEvent


class Node(object):
    id = ""
    node_type = "worker"

    def __init__(self):
        self.state = 'new'

    def set_id(self, id):
        self.id = id

    def is_new(self):
        return self.state == 'new'

    def is_manager(self):
        return self.node_type == 'manager'

    def is_worker(self):
        return self.node_type == 'worker'

    def set_state(self, state: str):
        self.state = state

    def get_state(self):
        return self.state

    def to_dict(self):
        return {
            'state': self.state,
            'id': self.id
        }


class Handler(object):
    _states = {}
    _triggers = {}
    _proceed = True
    _transitions = {
        'launching': [],
        'terminationg': [],
    }

    def __init__(self, event: Event, clients: dict, repositories: dict, logger: LifecycleLogger):
        self._event = event
        self.machine = Machine(self, send_event=True, initial='new')
        self.logger = logger
        self.repositories = repositories
        self.clients = clients

    def __call__(self):
        _node = self.repositories.get('node').get(self._event.get_instance_id())
        self.machine.initial = _node.get_state()

        _transitions = []
        if self._event.is_launching():
            if self._event.get_source() is 'aws.autoscaling' and not _node.is_new():
                raise RuntimeError("Node exsists and cannot launch again.")
            _transitions = self._transitions.get('launching')

        elif self._event.is_terminating():
            if _node.is_new():
                raise RuntimeError("New nodes cannot terminate.")
            _transitions = self._transitions.get('terminating')

        for _transition in _transitions:
            for _trigger in _transition.get('transitions'):
                


        for _next in self._triggers.keys():
            if _next == _node.get_state():
                for _trigger in self._triggers.get(_next):
                    func = getattr(self, _trigger.get('name'))
                    func(_node)

    def _init_launching(self):
        raise NotImplementedError()

    def _init_terminating(self):
        raise NotImplementedError()

    def _add_transition(self, triggers, source: str, dest: str):
        if source not in self._states.keys():
            self._states.update({source: State(source)})
            self.machine.add_state(self._states.get(source))

        if dest not in self._states.keys():
            self._states.update({dest: State(dest)})
            self.machine.add_state(self._states.get(dest))

        self._triggers.update({source: triggers})
        for _trigger in triggers:
            _conditions = _trigger.get('conditions', []) + [self._do_proceed]
            _before = [self._log_before] + _trigger.get('before', [])
            _after = _trigger.get('after', []) + [
                self._update_node,
                self._log_after
            ]

            self.machine.add_transition(
                _trigger.get('name'),
                self._states.get(source),
                self._states.get(dest),
                conditions=_conditions,
                unless=_trigger.get('unless', []),
                before=_before,
                after=_after,
                prepare=_trigger.get('prepare', [])
            )

    def _do_proceed(self, event_data: EventData):
        return self._proceed

    def _log_before(self, event_data: EventData):
        self.logger.info(
            'Transitioning %s from %s to %s via %s',
            self._get_node(event_data).to_dict(),
            event_data.transition.source,
            event_data.transition.dest,
            event_data.event.name
        )

    def _log_after(self, event_data: EventData):
        self.logger.info(
            'Transitioned from %s to %s via %s: %s',
            event_data.transition.source,
            event_data.transition.dest,
            event_data.event.name,
            self._get_node(event_data).to_dict(),
        )

    def _update_node(self, event_data: EventData):
        _node = self._get_node(event_data)
        _node.set_state(event_data.transition.dest)

    def _get_node(self, event_data: EventData) -> Node:
        return event_data.args[0]


class Docker(Handler):
    transtions = {
        'launching': [
            {
                'from': 'new',
                'to': 'pending',
                'transitions': [
                    {
                        'name': 'register',
                        'before': ['do_register'],
                    }
                ]
            }

        ]
    }
    def _init_launching(self):
        self._add_transition(
            [
                {
                    'name': 'register',
                    'before': [self.do_register],
                }
            ],
            'new', 'pending'
        )

        self._add_transition(
            [
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
            ],
            'pending', 'initializing'
        )

        self._add_transition(
            [
                {
                    'name': 'add_labels',
                    'conditions': [self.is_worker],
                    'before': [self.do_add_labels],
                },
                {
                    'name': 'update_swarm_dns',
                    'before': [self.do_update_swarm_dns],
                }
            ],
            'initializing', 'ready'
        )

        self._add_transition(
            [
                {
                    'name': 'rebalance_services',
                    'conditions': [self.is_worker],
                    'before': [self.do_rebalance_services],
                },
                {
                    'name': 'complete',
                    'before': [self.do_complete],
                }
            ],
            'ready', 'running'
        )

    def _init_terminating(self):
        self._add_transition(
            [
                {
                    'name': 'remove_from_cluster',
                    'before': [self.do_remove_from_cluster],
                }
            ],
            'running', 'terminating'
        )

        self._add_transition(
            [
                {
                    'name': 'update_swarm_dns',
                    'before': [self.do_update_swarm_dns],
                },
                {
                    'name': 'complete',
                    'before': [self.do_complete],
                }
            ],
            'terminating', 'terminated'
        )

        self._add_transition(
            [
                {
                    'name': 'remove',
                    'after': [self.do_remove],
                }
            ],
            'terminated', 'removed'
        )

    def is_manager(self, event_data: EventData):
        return self._get_node(event_data).is_manager()

    def is_worker(self, event_data: EventData):
        return self._get_node(event_data).is_worker()

    def do_add_labels(self, event_data: EventData):
        self.logger.info('adding labels on node %s', self._get_node(event_data).to_dict())

    def do_complete(self, event_data: EventData):
        self.logger.info('completing autoscaling action for node %s', self._get_node(event_data).to_dict())

    def do_rebalance_services(self, event_data: EventData):
        self.logger.info('rebalancing services due to autoscaling action in on node %s', self._get_node(event_data).to_dict())

    def do_update_swarm_dns(self, event_data: EventData):
        self.logger.info('updating dns on node %s', self._get_node(event_data).to_dict())

    def do_remove(self, event_data: EventData):
        self.logger.info('removing node %s from db', self._get_node(event_data).to_dict())

    def do_remove_from_cluster(self, event_data: EventData):
        self.logger.info('removing node %s from cluster', self._get_node(event_data).to_dict())

    def do_register(self, event_data: EventData):
        self.logger.info('registering node %s', self._get_node(event_data).to_dict())

    def do_join(self, event_data: EventData):
        self.logger.info('waiting for cluster to become ready')

        self.logger.info('joining node %s', self._get_node(event_data).to_dict())
        self._proceed = False

    def do_initialize(self, event_data: EventData):
        self.logger.info('initializing cluster on node %s', self._get_node(event_data).to_dict())
        self._proceed = False

    def do_finish(self, event_data: EventData):
        self.logger.info('finalizing node %s', self._get_node(event_data).to_dict())

    def is_cluster_ready(self, event_data: EventData):
        self.logger.info('checking if cluster is ready')
        result = False
        return result


class NodeRepository(object):
    def __init__(self):
        self.nodes = {}
        _node = Node()
        _node.set_id('existing_node')
        _node.set_state('ready')
        self.nodes.update({_node.id: _node})

    def get(self, id):
        if id in self.nodes.keys():
            return self.nodes.get(id)

        _node = Node()
        _node.set_id('new_node' + id)
        return _node


node_repository = NodeRepository()

message = json.load(open('autoscaling_event.json', 'r'))
#message = json.load(open('ssm_event.json', 'r'))

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

h = Docker(e, {}, {'node': node_repository}, logger)
h()
