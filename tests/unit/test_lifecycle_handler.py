import json
import types
import unittest
from unittest import mock

from transitions.core import Condition

from AutoscalingLifecycle import LifecycleHandler
from AutoscalingLifecycle import Model
from AutoscalingLifecycle.clients import DynamoDbClient
from AutoscalingLifecycle.entity import CommandRepository
from AutoscalingLifecycle.entity import NodeRepository
from AutoscalingLifecycle.entity import Repositories
from AutoscalingLifecycle.logging import Logging


class MockModel(Model):
    __transitions = []
    __state_machine_attributes = { }


    def get_transitions(self):
        return self.__transitions


    @property
    def transitions(self):
        return self.__transitions


    @transitions.setter
    def transitions(self, value):
        self.__transitions = value
        self._seen_states = []


class MockDynamoDbClient(DynamoDbClient):
    def get_item(self, id):
        try:
            fh = open('../fixtures/' + id + '.json', 'r')
            data = json.load(fh)
            fh.close()
        except Exception:
            data = { }

        if data.get('data', None) is not None:
            data = data.get('data')

        return data


class TestLifecycleHandler(unittest.TestCase):
    model = None


    def setUp(self):
        logging = Logging("TEST", True)
        # h = StreamHandler()
        # h.setLevel(INFO)
        # logging.add_handler(h, "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")

        client = MockDynamoDbClient(mock.Mock(), mock.Mock(), logging, 'table')
        repositories = Repositories(client, logging.get_logger())
        repositories.add('node', NodeRepository)
        repositories.add('command', CommandRepository)

        self.model = MockModel(mock.Mock(), repositories, logging)


    def get_default_tansition_config(self):
        return [
            {
                'source': 'source',
                'dest': 'destination',
                'triggers': [
                    {
                        'name': 'trigger_1',
                    },
                ]
            },
        ]


    def get_handle_conditions_transition_config(self):
        return [
            {
                'source': 'pending',
                'dest': 'state2',
                'triggers': [
                    {
                        'name': 'trigger_1',
                        'conditions': [self.true_condition],
                        'before': [self.trigger_no_error]
                    },
                    {
                        'name': 'is_not_called_1',
                        'prepare': [self.trigger_raise_error]
                    },
                ]
            },
            {
                'source': 'state2',
                'dest': 'state3',
                'triggers': [
                    {
                        'name': 'is_not_called_2',
                        'unless': [self.true_condition],
                        'before': [self.trigger_raise_error]
                    },
                    {
                        'name': 'trigger_2',
                        'before': [self.trigger_no_error]
                    },
                ]
            },
            {
                'source': 'state3',
                'dest': 'state4',
                'triggers': [
                    {
                        'name': 'is_not_called_3',
                        'unless': [self.true_condition],
                        'before': [self.trigger_raise_error]
                    }
                ]
            },
            {
                'source': ['state3', 'state4'],
                'dest': 'last',
                'triggers': [
                    {
                        'name': 'last',
                    },
                ]
            },
        ]


    def get_handle_ignore_errors_transition_config(self):
        return [
            {
                'source': 'pending',
                'dest': 'ignored1',
                'triggers': [
                    {
                        'name': 'trigger_ignore_error_before',
                        'before': [self.trigger_raise_error],
                        'ignore_errors': True
                    }
                ]
            },
            {
                'source': 'ignored1',
                'dest': 'ignored2',
                'triggers': [
                    {
                        'name': 'trigger_ignore_error_after',
                        'after': [self.trigger_raise_error],
                        'ignore_errors': True
                    }
                ]
            },
            {
                'source': 'ignored2',
                'dest': 'ignored3',
                'triggers': [
                    {
                        'name': 'trigger_ignore_error_condition',
                        'conditions': [self.trigger_raise_error],
                        'ignore_errors': True
                    }
                ]
            },
            {
                'source': ['ignored2', 'ignored3'],
                'dest': 'last',
                'triggers': [
                    {
                        'name': 'trigger_ignore_error_unless',
                        'unless': [self.trigger_raise_error],
                        'ignore_errors': True
                    }
                ]
            },
            {
                'source': 'last',
                'dest': None,
                'triggers': [
                    {
                        'name': 'last',
                    },
                ]
            }
        ]


    def get_handle_failure_transition_config(self):
        return [
            {
                'source': 'pending',
                'dest': 'not_reachable',
                'triggers': [
                    {
                        'name': 'raise',
                        'before': [self.trigger_raise_error],
                    }
                ]
            },
            {
                'source': 'not_reachable',
                'dest': 'last',
                'triggers': [
                    {
                        'name': 'no_op',
                    }
                ]
            },
            {
                'source': 'failure',
                'dest': 'failure_with_error',
                'triggers': [
                    {
                        'name': 'raise_in_failure',
                        'before': [self.trigger_raise_error],
                        'ignore_errors': True
                    },

                ]
            },
            {
                'source': 'failure_with_error',
                'dest': 'last',
                'triggers': [
                    {
                        'name': 'trigger',
                        'before': [self.trigger_no_error],
                    },

                ]
            },
            {
                'source': 'last',
                'dest': None,
                'triggers': [
                    {
                        'name': 'last',
                    },
                ]
            },
        ]


    def get_handle_failure_in_failure_transition_config(self):
        return [
            {
                'source': 'pending',
                'dest': None,
                'triggers': [
                    {
                        'name': 'raise',
                        'before': [self.trigger_raise_error],
                    }
                ]
            },
            {
                'source': 'failure',
                'dest': 'last',
                'triggers': [
                    {
                        'name': 'raise_in_failure',
                        'before': [self.trigger_raise_error],
                    },

                ]
            },
            {
                'source': 'last',
                'dest': None,
                'triggers': [
                    {
                        'name': 'last',
                    },
                ]
            },
        ]


    def get_docker_transitions(self):
        return [
            {
                'source': 'failure',
                'dest': 'forced_offline',
                'triggers': [
                    {
                        'name': 'graceful_update_dns',
                        'after': [self.trigger_no_error],
                        'ignore_errors': True
                    },
                ]
            },
            {
                'source': 'new',
                'dest': 'pending',
                # a single operation for all the nodes
                # no self.wait_for_next_event() operation: more transitions can be executed
                'triggers': [
                    {
                        'name': 'register',
                        'before': [self.trigger_no_error],
                    }
                ]
            },
            {
                'source': 'pending',
                'dest': 'initializing',
                # or operation:
                # if
                #   - node is a manager and cluster does not have an initialized_manager, execute do_initialize()
                # 	- else: execute do_join()
                # 'after': [self.wait_for_next_event] causes the execution to stop: last operation
                'triggers': [
                    {
                        'name': 'initialize_cluster',
                        'conditions': [self.true_condition],
                        'unless': [self.true_condition],
                        'before': [self.trigger_no_error],
                        # 'stop_after_state_change': True
                    },
                    {
                        'name': 'join_cluster',
                        'conditions': [self.true_condition],
                        'before': [self.trigger_no_error],
                        # 'stop_after_state_change': True
                    },
                ]
            },
            {
                'source': 'initializing',
                'dest': 'labeled',
                # a single operation with condition:
                # manager nodes will never get 'labeled' state
                # 'after': [self.wait_for_next_event] causes the execution to stop: last operation
                'triggers': [
                    {
                        'name': 'add_labels',
                        'conditions': [self.true_condition],
                        'before': [self.trigger_no_error],
                        # 'stop_after_state_change': True
                    }
                ]
            },
            {
                # multiple source states: as manager nodes will never get lableled,
                # we also allow to transition from initializing
                'source': ['initializing', 'labeled'],
                'dest': 'online',
                # no self.wait_for_next_event() operation: more transitions can be executed
                'triggers': [
                    {
                        'name': 'put_online',
                        'before': [self.trigger_no_error],
                    }
                ]
            },
            {
                'source': 'online',
                'dest': 'complete',
                # no self.wait_for_next_event() operation: more transitions can be executed
                'triggers': [
                    {
                        'name': 'complete_launch',
                        'before': [self.trigger_no_error],
                    }
                ]
            },
            {
                'source': 'complete',
                'dest': 'running',
                # 'after': [self.wait_for_next_event] causes the execution to stop: last operation
                'triggers': [
                    {
                        'name': 'rebalance_services',
                        'conditions': [self.true_condition],
                        'before': [self.trigger_no_error],
                        # 'stop_after_state_change': True
                    },
                ]
            },
            {
                'source': ['complete', 'running'],
                'dest': 'ready',
                # 'after': [self.wait_for_next_event] causes the execution to stop: last operation
                'triggers': [
                    # a null operation to let also manager transition to running
                    {
                        'name': 'finish',
                        'after': [self.trigger_raise_error],
                        'last': True
                    }
                ]
            },
            {
                # allow all launching states (except new) to this transition
                'source': ['pending', 'abandoned', 'initializing', 'labeled', 'online', 'ready', 'running'],
                'dest': 'offline',
                # remove the node from dns, which makes it unreachable for workloads
                # this is considered offline
                'triggers': [
                    {
                        'name': 'put_offline',
                        # update dns AFTER state change, so that terminating nodes cannot be considered
                        # while updating records
                        'after': [self.trigger_no_error],
                    }
                ]
            },
            {
                'source': 'offline',
                'dest': 'removed_form_cluster',
                # 'after': [self.wait_for_next_event] causes the execution to stop: last operation
                'triggers': [
                    {
                        'name': 'remove_from_cluster',
                        'before': [self.trigger_no_error],
                        # 'stop_after_state_change': True
                    }
                ]
            },
            {
                'source': 'removed_form_cluster',
                'dest': 'terminating_complete',
                # no self.wait_for_next_event() operation: more transitions can be executed
                'triggers': [
                    {
                        'name': 'complete_terminate',
                        'after': [self.trigger_no_error]
                    },
                ]
            },
            {
                'source': 'terminating_complete',
                'dest': 'rebalanced',
                # or operation:
                # if
                #   - node is a worker, rebalance services
                # 	- else: remove the node
                # manager nodes will be removed immediately
                # 'after': [self.wait_for_next_event] causes the execution to stop: last operation
                'triggers': [
                    {
                        'name': 'rebalance_services',
                        'conditions': [self.true_condition],
                        'before': [self.trigger_no_error],
                        # 'stop_after_state_change': True
                    }
                ]
            },
            {
                'source': ['rebalanced', 'terminating_complete'],
                # a destination state of None will prevent the machine to update the state
                'dest': None,
                # 'after': [self.wait_for_next_event] causes the execution to stop: last operation
                'triggers': [
                    {
                        'name': 'remove',
                        'before': [self.trigger_no_error],
                        'last': True
                    },
                ]
            },
            {
                'source': 'forced_offline',
                'dest': 'gracefully_remove_from_cluster',
                'triggers': [
                    {
                        'name': 'graceful_remove_from_cluster',
                        'after': [self.trigger_raise_error],
                        # 'stop_after_state_change': True,
                        'ignore_errors': True
                    },
                ]
            },
            {
                'source': 'gracefully_remove_from_cluster',
                'dest': 'gracefully_completed',
                'triggers': [
                    {
                        'name': 'graceful_complete',
                        'after': [self.trigger_raise_error],
                        'ignore_errors': True
                    },
                ]
            },
            {
                'source': 'gracefully_completed',
                'dest': 'gracefully_rebalancing',
                'triggers': [
                    {
                        'name': 'graceful_rebalance',
                        'conditions': [self.true_condition],
                        'after': [self.trigger_raise_error],
                        # 'stop_after_state_change': True,
                        'ignore_errors': True
                    },
                ]
            },
            {
                'source': ['gracefully_completed', 'gracefully_rebalancing'],
                # a destination state of None will prevent the machine to update the state
                'dest': None,
                # 'after': [self.wait_for_next_event] causes the execution to stop: last operation
                'triggers': [
                    {
                        'name': 'graceful_remove',
                        'before': [self.trigger_raise_error],
                        'last': True,
                        'ignore_errors': True
                    },
                ]
            }
        ]


    def trigger_raise_error(self, *args, **kwargs):
        raise RuntimeError("error in trigger method")


    def trigger_no_error(self, *args, **kwargs):
        return


    def true_condition(self, *args, **kwargs):
        return True


    def test_log_before_and_after_transition(self):
        model = mock.Mock()
        model.get_transitions.return_value = self.get_default_tansition_config()

        handler = LifecycleHandler(model)
        triggers = handler.machine.get_triggers("source")
        self.assertEqual(len(triggers), 1)

        event = handler.machine.events.get(triggers[0])
        transition = event.transitions.get('source')[0]

        self.assertEqual(0, len(transition.prepare))

        self.assertEqual(1, len(transition.conditions))
        self.assertIsInstance(transition.conditions[0], Condition)
        self.assertEqual(transition.conditions[0].func.__name__, '__is_event_successful')

        self.assertEqual(1, len(transition.before))
        self.assertIsInstance(transition.before[0], types.MethodType)
        self.assertEqual(transition.before[0].__name__, '__log_before')

        self.assertEqual(1, len(transition.after))
        self.assertIsInstance(transition.after[0], types.MethodType)
        self.assertEqual(transition.after[0].__name__, '__log_after')


    def test_last_transition_flag_is_accepted(self):
        config = self.get_default_tansition_config()
        config[0].get('triggers')[0].update({ 'last': True })

        model = mock.Mock()
        model.get_transitions.return_value = config

        handler = LifecycleHandler(model)
        triggers = handler.machine.get_triggers("source")
        event = handler.machine.events.get(triggers[0])
        transition = event.transitions.get('source')[0]

        self.assertEqual(1, len(transition.prepare))
        self.assertIsInstance(transition.prepare[0], types.MethodType)
        self.assertEqual(transition.prepare[0].__name__, '__wait_for_next_event')


    def test_stop_after_state_change_flag_is_accepted(self):
        config = self.get_default_tansition_config()
        config[0].get('triggers')[0].update({ 'stop_after_state_change': True })

        model = mock.Mock()
        model.get_transitions.return_value = config

        handler = LifecycleHandler(model)
        triggers = handler.machine.get_triggers("source")
        event = handler.machine.events.get(triggers[0])
        transition = event.transitions.get('source')[0]

        self.assertIsInstance(transition.after[0], types.MethodType)
        self.assertEqual(transition.after[0].__name__, '__wait_for_next_event')


    def test_ignore_errors_flag_is_accepted(self):
        config = self.get_default_tansition_config()
        config[0].get('triggers')[0].update({ 'ignore_errors': True })

        model = mock.Mock()
        model.get_transitions.return_value = config

        handler = LifecycleHandler(model)
        triggers = handler.machine.get_triggers("source")
        event = handler.machine.events.get(triggers[0])
        transition = event.transitions.get('source')[0]

        self.assertIsInstance(transition.prepare[0], types.MethodType)
        self.assertEqual(transition.prepare[0].__name__, '__ignore_operation_failure')


    def test_ignore_errors_behavior(self):
        self.model.transitions = self.get_handle_ignore_errors_transition_config()

        fh = open('../fixtures/ssm_event.json', 'r')
        message = json.load(fh)
        fh.close()
        handler = LifecycleHandler(self.model)
        handler(message)

        self.assertEqual(4, len(self.model.seen_states))
        self.assertEqual('last', self.model.state)


    def test_failure_behavior(self):
        self.model.transitions = self.get_handle_failure_transition_config()

        fh = open('../fixtures/ssm_event.json', 'r')
        message = json.load(fh)
        fh.close()
        handler = LifecycleHandler(self.model)
        handler(message)

        self.assertEqual(3, len(self.model.seen_states))
        self.assertEqual('last', self.model.state)


    def test_failure_in_failure_handling_behavior(self):
        self.model.transitions = self.get_handle_failure_in_failure_transition_config()

        fh = open('../fixtures/ssm_event.json', 'r')
        message = json.load(fh)
        fh.close()
        handler = LifecycleHandler(self.model)

        with self.assertRaises(RuntimeError) as context:
            handler(message)
            self.assertTrue('error in trigger method' in context.exception)

        self.assertEqual(1, len(self.model.seen_states))
        self.assertEqual('failure', self.model.state)


    def test_conditions_behavior(self):
        self.model.transitions = self.get_handle_conditions_transition_config()

        fh = open('../fixtures/ssm_event.json', 'r')
        message = json.load(fh)
        fh.close()
        handler = LifecycleHandler(self.model)
        handler(message)

        self.assertEqual(3, len(self.model.seen_states))
        self.assertEqual('last', self.model.state)


    def test_docker_transitions_for_new_node(self):
        self.model.transitions = self.get_docker_transitions()

        fh = open('../fixtures/autoscaling_event.json', 'r')
        message = json.load(fh)
        fh.close()
        handler = LifecycleHandler(self.model)
        handler(message)

        self.assertEqual(12, len(self.model.seen_states))
        self.assertEqual('gracefully_rebalancing', self.model.state)
