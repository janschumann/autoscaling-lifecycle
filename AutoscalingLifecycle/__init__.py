import json
from logging import Logger
from logging import DEBUG

from transitions import EventData
from transitions import Machine

from .clients import Clients
from .entity import CommandRepository
from .entity import Node
from .entity import NodeRepository
from .entity import Repositories
from .entity import Repository
from .logging import Formatter
from .logging import Logging
from .logging import MessageFormatter


def listify(obj):
    if obj is None:
        return []
    return obj if isinstance(obj, list) else [obj]


class LifecycleData(object):

    LAUNCHING = 'autoscaling:EC2_INSTANCE_LAUNCHING'
    TERMINATING = 'autoscaling:EC2_INSTANCE_TERMINATING'

    def __init__(self, data: dict):
        self.data = data
        self.metadata = self.data.get('NotificationMetadata')
        if type(self.metadata) is not dict:
            self.data.update({
                'NotificationMetadata': json.loads(self.metadata)
            })


    def to_dict(self):
        return self.data


    def get_lifecycle_action_token(self) -> str:
        return self.data.get('LifecycleActionToken')


    def get_lifecycle_transition(self) -> str:
        return self.data.get('LifecycleTransition')


    def get_lifecycle_hook_name(self) -> str:
        return self.data.get('LifecycleHookName')


    def get_autoscaling_group_name(self) -> str:
        return self.data.get('AutoScalingGroupName')


    def get_instance_id(self) -> str:
        return self.data.get('EC2InstanceId')


    def get_metadata(self) -> dict:
        return self.data.get('NotificationMetadata')


    def is_launching(self, *args) -> bool:
        """
        *args are required to support this method within transition configs
              but not used

        :rtype: bool
        :return: Whether we react on a launch event
        """
        return self.get_lifecycle_transition() == self.LAUNCHING


    def is_terminating(self, *args) -> bool:
        """
        *args are required to support this method within transition configs
              but not used

        :rtype: bool
        :return: Whether we react on a terminate event
        """
        return self.get_lifecycle_transition() == self.TERMINATING


class Event(object):
    """
    :param _name:
    :type _name: str
    :param _event:
    :type _event: dict
    :param _lifecycle_data:
    :type _lifecycle_data: LifecycleData
    """

    __AUTOSCALING = 'aws.autoscaling'
    __COMMAND = 'aws.ssm'
    __SCHEDULED = 'aws.events'

    _CONTINUE = 'CONTINUE'
    _ABANDON = 'ABANDON'

    EVENT = 'event'
    NODE = 'node'
    COMMAND = 'command'

    name = None
    _event = None
    _lifecycle_data = None

    has_failure = False


    def __init__(self, event: dict):
        self._event = event
        if self.is_autoscaling():
            self.set_lifecycle_data(self.get_detail())

        self.name = self._event.get('detail-type')
        if self.is_command():
            if self.is_lifecycle():
                self.name = '%s for %s' % (self.name, self._lifecycle_data.get_lifecycle_transition())

        elif self.is_scheduled():
            self.name = self._event.get('resources')[0].split('/')[-1]


    def get_name(self):
        return self.name


    def set_name(self, name: str):
        if name != '':
            self.name = name


    def get_raw(self) -> dict:
        return self._event


    def get_detail(self) -> dict:
        return self._event.get('detail')


    def get_source(self) -> str:
        return self._event.get('source')


    def is_autoscaling(self, *args) -> bool:
        """
        *args are required to support this method within transition configs
              but not used
        """
        return self.get_source() == self.__AUTOSCALING


    def is_command(self, *args) -> bool:
        """
        *args are required to support this method within transition configs
              but not used
        """
        return self.get_source() == self.__COMMAND


    def is_scheduled(self, *args) -> bool:
        """
        *args are required to support this method within transition configs
              but not used
        """
        return self.get_source() == self.__SCHEDULED


    def is_lifecycle(self, *args):
        """
        *args are required to support this method within transition configs
              but not used
        """
        return self._lifecycle_data is not None


    def set_lifecycle_data(self, data: dict):
        if data != dict():
            self._lifecycle_data = LifecycleData(data)


    def get_lifecycle_data(self) -> LifecycleData:
        if not self.is_lifecycle():
            raise RuntimeError('This is not a lifecycle event.')

        return self._lifecycle_data


    def get_lifecycle_result(self) -> str:
        if not self.is_lifecycle():
            raise RuntimeError('This is not a lifecycle event.')

        if not self.has_failure and (self._lifecycle_data.is_terminating() or self.is_successful()):
            return self._CONTINUE

        return self._ABANDON


    def to_str(self):
        msg = self.get_name()

        if self.is_autoscaling() or self.is_command():
            if self.is_lifecycle():
                msg = '%s in group "%s" on instance "%s"' % (
                    msg,
                    self._lifecycle_data.get_autoscaling_group_name(),
                    self._lifecycle_data.get_instance_id()
                )

        if self.is_command():
            msg = '%s finished commands %s on %s' % (
                msg,
                ','.join(json.loads(self.get_detail().get('parameters')).get('commands')),
                ','.join([resource.split('/')[-1] for resource in self._event.get('resources')])
            )

        return msg


    def is_successful(self, *args) -> bool:
        """
        *args are required to support this method within transition configs
              but not used
        """
        if self.is_command():
            return not self.has_failure and self._event.get('detail').get('status') == 'Success'

        return not self.has_failure


    def set_has_failure(self):
        self.has_failure = True


class Model(object):
    """
    :param logger:
    :type logger: Logger
    :param formatter:
    :type formatter: MessageFormatter
    :param clients:
    :type clients: Clients
    :param repositories:
    :type repositories: Repositories
    :param _state:
    :type _state: str
    :param event:
    :type event: Event
    :param node:
    :type node: Node
    """
    logger = None
    formatter = None
    clients = None
    repositories = None

    event = None
    _node = None
    _state = None
    allow_state_updates = False
    passed_states = []

    EVENT = 'event'
    NODE = 'node'
    COMMAND = 'command'


    def __init__(self, clients: Clients, repositories: Repositories, logging: Logging):
        self.logger = logging.get_logger()
        self.repositories = repositories
        self.clients = clients
        self.formatter = logging.get_formatter()
        self.node = None
        self.state = None


    @property
    def state(self) -> str:
        return self._state


    @state.setter
    def state(self, value: str):
        # ignore state updates until an event has been loaded
        # e.g. the initial state of the machine
        # @see self.load_event()
        if self._state is None or not self.allow_state_updates:
            return

        self._state = value
        self.passed_states.append(self._state)

        if self.node is not None:
            self.repositories.get('node').update(self.node, {
                'ItemStatus': self._state
            })


    @property
    def node(self) -> Node:
        return self._node


    @node.setter
    def node(self, node: Node):
        if node is not None:
            self._node = node
            if self.node.is_new():
                self._wait_for_cloud_init()


    def initialize(self, event: Event):
        self.event = event
        if self.event.is_command():
            command = self.get_command_repository().pop(self.event.get_raw().get('detail').get('command-id'))
            self.event.set_lifecycle_data(command.get('LifecycleData', dict()))
            self.event.set_name(command.get('EventName', ''))

        if self.event.is_lifecycle():
            self.node = self.get_node_repository().get(self.event.get_lifecycle_data().get_instance_id())
            self._state = self.node.get_state()
        else:
            if self.event.is_command():
                instance_id = [resource.split('/')[-1] for resource in self.event.get_raw().get('resources')][0]
                self.node = self.get_node_repository().get(instance_id)
                self._state = self.node.get_state()
            else:
                self._state = self.event.get_name()

        self.passed_states = []


    def _wait_for_cloud_init(self):
        if self.node.get_state() != 'finished_cloud_init':
            self.logger.debug("Waiting for node to be registered and cloud init to finish ...")
            self.clients.get('dynamodb').wait_for_scan_count_is(
                1,
                'Ident = :id and ItemStatus = :status',
                {
                    ":id": self.node.get_id(),
                    ":status": 'finished_cloud_init'
                }
            )

        # fetch the node again to pick up all data probably set by cloud init
        # !! use self._node here to ensure this method is not called again
        self._node = self.repositories.get('node').get(self.node.get_id())


    def get_transitions(self):
        raise NotImplementedError()


    def report(self, direction, event_data: EventData, force_report_autoscaling_activity: bool = False):
        status = 'INFO' if self.event.is_successful() else "ERROR"
        if self.logger.level == DEBUG or status == 'ERROR':
            self.logger.debug('Reporting activity: %s', repr(event_data))
            subject = '%s from %s to %s via %s' % (
                direction,
                event_data.transition.source,
                event_data.transition.dest,
                event_data.event.name
            )
            self.clients.get('sns').publish_activity(
                status,
                subject,
                self.event.to_str() + ' : ' + repr(event_data),
                'eu-west-1'
            )

        if self.event.is_lifecycle() and (force_report_autoscaling_activity or self.event.is_autoscaling()):
            activity = self.clients.get('autoscaling').get_activity(
                self.event.get_lifecycle_data().get_autoscaling_group_name(),
                self.event.get_lifecycle_data().is_launching(),
                self.node.get_id()
            )
            self.logger.debug('Reporting activity: %s', activity)
            self.clients.get('sns').publish_autoscaling_activity(activity, 'eu-west-1')


    def _send_command(self, comment: str, commands: list, target_nodes = None, command_timeout = 60):
        if target_nodes is not None:
            target_nodes = listify(target_nodes)
        else:
            target_nodes = [self.node]

        target_node_ids = []
        for node in target_nodes:
            target_node_ids.append(node.get_id())

        metadata = {
            'RunningOn': ', '.join(target_node_ids),
            'Comment': comment,
            'Commands': ', '.join(commands),
            'EventName': self.event.get_name()
        }

        if self.event.is_lifecycle():
            metadata.update({'LifecycleData': self.event.get_lifecycle_data().to_dict()})

        command_id = self.clients.get('ssm').send_command(target_node_ids, comment, commands, command_timeout)
        self.repositories.get('command').register(command_id, metadata)

    #
    # convenience methods
    #

    def get_node_repository(self) -> NodeRepository:
        return self.repositories.get(self.NODE)


    def get_command_repository(self) -> CommandRepository:
        return self.repositories.get(self.COMMAND)

    #
    # built-in trigger functions
    #

    def do_complete_lifecycle_action(self, event_data: EventData):
        self.logger.info('completing autoscaling action for node %s', self.node.to_dict())
        self.clients.get('autoscaling').complete_lifecycle_action(
            self.event.get_lifecycle_data().get_lifecycle_hook_name(),
            self.event.get_lifecycle_data().get_autoscaling_group_name(),
            self.event.get_lifecycle_data().get_lifecycle_action_token(),
            self.event.get_lifecycle_result(),
            self.node.get_id()
        )

        if self.event.get_lifecycle_data().is_launching():
            self.clients.get('autoscaling').wait_for_activity_to_complete(
                self.event.get_lifecycle_data().get_autoscaling_group_name(),
                self.event.get_lifecycle_data().is_launching(),
                self.node.get_id()
            )

        self.report('Finished', event_data, True)


    def do_remove_from_db(self, *args):
        self.logger.info('removing node %s from db', self.node.to_dict())
        self.repositories.get('node').delete(self.node)


class ConfigurationError(RuntimeError):
    def get_message(self):
        return self.args[0]


class TriggerParameterConfigurationError(ConfigurationError):
    pass


class StopIterationAfterTrigger(Exception):
    """ Signal the end trigger from LifecycleHandler.__process(). """
    def get_message(self):
        return self.args[0]


class StopProcessingAfterStateChange(Exception):
    """ Signal the end from LifecycleHandler.__process(). """
    def get_message(self):
        return self.args[0]


class LifecycleHandler(object):
    """
    :type machine: Machine
    :type model: Model
    :type __in_failure_handling: bool
    :type __raise_on_operation_failure: bool
    """
    machine_cls = Machine
    machine = None
    model = None
    __in_failure_handling = False
    __raise_on_operation_failure = True
    __default_trigger = {
        'name': 'default',
        'prepare': [],
        'conditions': [],
        'unless': [],
        'after': [],
        'before': [],
        'stop_after_trigger': False,
        'ignore_errors': False
    }
    __default_transition = {
        'source': [],
        'dest': '',
        'triggers': [],
        'stop_after_state_change': False
    }
    __illegal_trigger_names = [
        'trigger'
    ]

    #
    # initialization
    #

    def __init__(self, model: Model):
        self.model = model
        self.machine = self.machine_cls(self.model, auto_transitions = False, send_event = True, queued = False)
        self.__add_transitions()
        # set the initial state after initializing transitions
        # to avoid duplicate destination state errors
        self.machine.initial = self.model.state
        self.model.allow_state_updates = True


    def __add_transitions(self):
        self.__get_logger().debug('initializing transitions')
        for config in self.model.get_transitions():
            transition = self.__default_transition.copy()
            transition.update(config)

            sources = listify(transition.pop('source'))
            dest = transition.pop('dest')
            triggers = transition.pop('triggers')
            stop_after_state_change = transition.pop('stop_after_state_change')

            if transition != { }:
                raise ConfigurationError(
                    'unknown options %s in transition config' % ", ".join(transition.keys()))

            if dest in self.machine.states.keys():
                raise ConfigurationError(
                    'Duplicate destination state %s. Multiple transitions with the same destination are not allowed.' % dest
                )

            self.machine.add_state(dest)
            if stop_after_state_change and dest is not None:
                state = self.machine.get_state(dest)
                state.on_enter = [self.__wait_for_next_event]

            states = self.machine.states.keys()
            for state in sources:
                if state not in states:
                    self.machine.add_state(state)

            for trigger in triggers:
                try:
                    self.__add_transition(sources, dest, trigger)
                except TriggerParameterConfigurationError as e:
                    msg = "Configuration error for source states '%s' in trigger '%s': '%s'" % (
                        ', '.join(sources), trigger.get('name'), e.get_message())
                    self.__get_logger().error(msg)
                    raise ConfigurationError(msg)


    def __add_transition(self, sources: list, dest: str, config: dict):
        trigger = self.__default_trigger.copy()
        trigger.update(config)

        name = trigger.get('name')
        trigger.pop('name')

        if name in self.__illegal_trigger_names:
            raise ConfigurationError('trigger name %s is not allowed' % name)

        # prepare functions will be executed first
        prepare = trigger.get('prepare')
        if type(prepare) is not list:
            raise TriggerParameterConfigurationError('prepare is not a list')
        trigger.pop('prepare')

        ignore_errors = trigger.get('ignore_errors')
        trigger.pop('ignore_errors')
        if ignore_errors:
            # set ignore errors before any other function
            # so we set this as early as possible (in prepare)
            prepare = [self.__ignore_operation_failure] + prepare

        conditions = trigger.get('conditions')
        if type(conditions) is not list:
            raise TriggerParameterConfigurationError('conditions is not a list')
        trigger.pop('conditions')
        if not ignore_errors:
            # require the event to be successful
            conditions = [self.__is_event_successful] + conditions
        unless = trigger.get('unless')
        if type(unless) is not list:
            raise TriggerParameterConfigurationError('unless is not a list')
        trigger.pop('unless')

        before = trigger.get('before')
        if type(before) is not list:
            raise TriggerParameterConfigurationError('before is not a list')
        trigger.pop('before')
        # log the event before any other transition function
        before = [self.__log_before] + before

        after = trigger.get('after')
        if type(after) is not list:
            raise TriggerParameterConfigurationError('after is not a list')
        trigger.pop('after')
        after = after + [self.__log_after]
        # set stop condition if needed
        stop_after_trigger = trigger.get('stop_after_trigger')
        trigger.pop('stop_after_trigger')
        if stop_after_trigger:
            after += [self.__stop_after_trigger]

        if trigger != { }:
            raise TriggerParameterConfigurationError(
                'unknown options %s for trigger %s' % (", ".join(trigger.keys()), name))

        self.machine.add_transition(
            name,
            sources,
            dest,
            prepare = prepare,
            conditions = conditions,
            unless = unless,
            before = before,
            after = after
        )

    #
    # processing
    #
    def __call__(self):
        # fail early, if lifecycle conditions do not pass
        if self.model.event.is_lifecycle():
            if self.model.event.get_lifecycle_data().is_launching() and self.model.event.is_autoscaling() and not self.model.node.is_new():
                raise self.__get_formatter().get_error(RuntimeError, "Only new nodes can be launched.")

            if self.model.event.get_lifecycle_data().is_terminating() and self.model.node.is_new():
                raise self.__get_formatter().get_error(RuntimeError, "New nodes cannot terminate.")

        # fail early, if no triggers can be found for the current state
        triggers = self.machine.get_triggers(self.model.state)
        if len(triggers) < 1:
            raise RuntimeError('no trigger could be found for %s' % self.model.state)

        self.__get_logger().info('processing model %s', repr(self.model))

        self.__process(triggers)

        self.__get_logger().info('processed model %s', repr(self.model))


    def __process(self, triggers: list):
        try:
            while len(triggers) > 0:
                state = self.model.state
                self.__get_logger().debug('possible triggers for state %s: %s', state, triggers)
                for trigger in triggers:
                    # reset trigger condition
                    self.__raise_on_operation_failure = True

                    try:
                        self.__get_logger().info('pulling trigger %s', trigger)
                        self.machine.dispatch(trigger)

                    except StopProcessingAfterStateChange as e:
                        self.__get_logger().info(e.get_message())
                        return

                    except StopIterationAfterTrigger as e:
                        self.__get_logger().info(e.get_message())
                        return

                    except Exception as e:
                        transitions = self.machine.events.get(trigger).transitions.get(self.model.state)
                        self.__get_clients().get('sns').publish_error(
                            e,
                            transitions[0] if transitions is not None else trigger,
                            'eu-west-1'
                        )
                        if self.__raise_on_operation_failure:
                            raise

                        self.__get_logger().exception('Ignoring failure %s in trigger %s.', repr(e), trigger)
                        # in case the error occurred somewhere before the state change,
                        # we need to find the destination state and update the model,
                        # to be able to proceed with next triggers
                        # a destination state of None indicates an internal transition and the model
                        # will not be updated and thus the iteration is stopped
                        if transitions is not None and transitions[0].dest is not None:
                            msg = 'Trigger pulled before state change or error in conditions. Forcing state to %s'
                            self.__get_logger().info(msg, transitions[0].dest)
                            self.model.state = transitions[0].dest

                    self.__get_logger().info('trigger %s complete', trigger)

                    if self.model.state != state:
                        # the state change has been applied
                        # proceed with triggers for the updated state
                        break

                if self.model.state == state:
                    # state has not changed after pulling all triggers
                    # stop processing
                    break

                # load new triggers from updated state
                triggers = self.machine.get_triggers(self.model.state)

        except Exception as e:
            if self.__in_failure_handling:
                self.__get_logger().exception("An error occured during failure handling.", repr(e))
                self.__get_clients().get('sns').publish_error(
                    e,
                    'fail',
                    'eu-west-1'
                )
                raise

            msg = "An error occurred during transition. %s. Entering failure handling."
            self.__get_logger().exception(msg, repr(e))
            self.__in_failure_handling = True

            self.model.event.set_has_failure()
            self.model.state = 'failure'
            triggers = self.machine.get_triggers(self.model.state)
            if len(triggers) < 1:
                self.__get_logger().warning("No triggers for state failure found.")
            else:
                self.__process(triggers)

    #
    # private built-in trigger functions
    #

    def __log_transition(self, direction: str, event_data: EventData):
        self.__get_logger().info(
            '%s from %s to %s via %s%s',
            direction,
            event_data.transition.source,
            event_data.transition.dest,
            event_data.event.name,
            ' on node %s' % (self.model.node.get_id() if self.model.node is not None else " ")
        )
        self.__log_autoscaling_activity(event_data)


    def __log_before(self, event_data: EventData):
        self.__log_transition('Transitioning', event_data)
        self.model.report('Transitioning', event_data)


    def __log_after(self, event_data: EventData):
        self.__log_transition('Transitioned', event_data)
        self.model.report('Transitioned', event_data)


    def __is_event_successful(self, event_data: EventData) -> bool:
        self.__get_logger().debug("Check event status: %s", repr(event_data))
        status = self.model.event.is_successful()
        if self.model.event.is_command():
            if status:
                self.__get_logger().debug("Command was successful.")
            else:
                self.__get_logger().error("Command was not successful.")
                raise RuntimeError("Command was not successful.")

        return status


    def __stop_after_trigger(self, event_data: EventData):
        raise StopIterationAfterTrigger("Trigger forces to continue with next trigger. %s" % repr(event_data))


    def __wait_for_next_event(self, event_data: EventData):
        raise StopProcessingAfterStateChange("State requires to wait for the next event. %s" % repr(event_data))


    def __ignore_operation_failure(self, event_data: EventData):
        self.__get_logger().debug("%s requires to ignore exceptions.", repr(event_data))
        self.__raise_on_operation_failure = False


    def __log_autoscaling_activity(self, event_data: EventData):
        if self.model.event.is_lifecycle():
            _lifecycle_data = self.model.event.get_lifecycle_data()
            activity = self.model.clients.get('autoscaling').get_activity(
                _lifecycle_data.get_autoscaling_group_name(),
                _lifecycle_data.is_launching(),
                _lifecycle_data.get_instance_id()
            )
            self.__get_logger().info('%s %s autoscaling activity on event %s: %s', activity.get('StatusCode').upper(),
                                     'launching' if _lifecycle_data.is_launching else 'terminating',
                                     repr(event_data), activity)

            if activity == dict():
                self.__get_logger().warning('Could not find autoscaling activity for node %s', _lifecycle_data.get_instance_id())

    #
    # convenience methods
    #

    def __get_logger(self) -> Logger:
        return self.model.logger


    def __get_formatter(self) -> MessageFormatter:
        return self.model.formatter


    def __get_node_repository(self) -> NodeRepository:
        return self.model.get_node_repository()


    def __get_command_repository(self) -> CommandRepository:
        return self.model.get_command_repository()


    def __get_clients(self) -> Clients:
        return self.model.clients


    def __get_event(self) -> Event:
        return self.model.event


    def __get_node(self) -> Node:
        return self.model.node
