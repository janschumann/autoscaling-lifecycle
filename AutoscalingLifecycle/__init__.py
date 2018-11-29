import json
import time
from logging import Logger

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


class Event(object):
    """
    :param _event:
    :type _event: dict
    :param node:
    :type node: Node
    :param command:
    :type _command: Command
    """

    LAUNCHING = 'autoscaling:EC2_INSTANCE_LAUNCHING'
    TERMINATING = 'autoscaling:EC2_INSTANCE_TERMINATING'

    __AUTOSCALING = 'aws.autoscaling'
    __COMMAND = 'aws.ssm'

    _CONTINUE = 'CONTINUE'
    _ABANDON = 'ABANDON'

    EVENT = 'event'
    NODE = 'node'
    COMMAND = 'command'

    _event = { }
    _command = { }

    node = None

    has_failure = False


    @staticmethod
    def from_sns_message(message: dict):
        event = json.loads(message.get('Records')[0].get('Sns').get('Message'))

        if event.get('source') == 'aws.autoscaling':
            return AutoscalingEvent(event)

        elif event.get('source') == 'aws.ssm':
            return SsmEvent(event)

        else:
            raise RuntimeError('Unkonwn event %s', event.get('source'))


    def __init__(self, event: dict):
        self._event = event


    def to_str(self):
        msg = '%s in group "%s" on instance "%s"' % (
            self._event.get('detail-type'),
            self.get_autoscaling_group_name(),
            self.node.get_id()
        )
        return msg


    def get_raw_event(self) -> dict:
        return self._event


    def get_event(self) -> dict:
        return self.get_raw_event()


    def get_command_metadata(self) -> dict:
        return self.get_event()


    def get_detail(self) -> dict:
        return self._event.get('detail')


    def get_source(self) -> str:
        return self._event.get('source')


    def is_autoscaling(self) -> bool:
        return self.get_source() == self.__AUTOSCALING


    def has_command(self) -> bool:
        return self.get_source() == self.__COMMAND


    def set_command(self, command: dict):
        self._command = command


    def gez_command(self) -> dict:
        return self._command


    def is_successful(self) -> bool:
        return True


    def is_launching(self) -> bool:
        """
        :rtype: bool
        :return: Whether we react on a launch event
        """
        return self.get_lifecycle_transition() == self.LAUNCHING


    def is_terminating(self) -> bool:
        """
        :rtype: bool
        :return: Whether we react on a terminate event
        """
        return self.get_lifecycle_transition() == self.TERMINATING


    def get_lifecycle_result(self) -> str:
        if self.is_terminating() and not self.has_failure:
            return self._CONTINUE

        return self._ABANDON


    def get_lifecycle_action_token(self) -> str:
        raise NotImplementedError()


    def get_lifecycle_transition(self) -> str:
        raise NotImplementedError()


    def get_lifecycle_hook_name(self) -> str:
        raise NotImplementedError()


    def get_autoscaling_group_name(self) -> str:
        raise NotImplementedError()


    def get_instance_id(self) -> str:
        raise NotImplementedError()


    def get_metadata(self) -> dict:
        raise NotImplementedError()


    def set_has_failure(self):
        self.has_failure = True


class AutoscalingEvent(Event):

    def __init__(self, event: dict):
        super().__init__(event)

        metadata = self._event.get('detail').get('NotificationMetadata')
        if type(metadata) is not dict:
            self._event.get('detail').update({
                'NotificationMetadata': json.loads(metadata)
            })


    def get_lifecycle_action_token(self) -> str:
        return self._event.get('detail').get('LifecycleActionToken')


    def get_lifecycle_transition(self) -> str:
        return self._event.get('detail').get('LifecycleTransition')


    def get_lifecycle_hook_name(self) -> str:
        return self._event.get('detail').get('LifecycleHookName')


    def get_autoscaling_group_name(self) -> str:
        return self._event.get('detail').get('AutoScalingGroupName')


    def get_instance_id(self) -> str:
        return self._event.get('detail').get('EC2InstanceId')


    def get_metadata(self) -> dict:
        return self._event.get('detail').get('NotificationMetadata')


class SsmEvent(Event):

    def to_str(self):
        action = 'launching' if self.is_launching() else 'terminating'
        msg = '%s for "%s" while "%s" in group "%s" on instance "%s"' % (
            self._event.get('detail-type'),
            self._command.get('Comment'),
            action,
            self.get_autoscaling_group_name(),
            self.node.get_id()
        )
        return msg


    def get_command_id(self):
        return self._event.get('detail').get('command-id')


    def get_lifecycle_result(self) -> str:
        if not self.has_failure and (self.is_terminating() or self.is_successful()):
            return self._CONTINUE

        return self._ABANDON


    def get_event(self):
        e = super().get_event()
        e.update({ 'CommandMetadata': self.get_command_metadata() })
        return e


    def is_successful(self) -> bool:
        return self._event.get('detail').get('status') == 'Success'


    def get_command_metadata(self) -> dict:
        return self._command


    def get_lifecycle_action_token(self) -> str:
        return self._command.get('detail').get('LifecycleActionToken')


    def get_lifecycle_transition(self) -> str:
        return self._command.get('detail').get('LifecycleTransition')


    def get_lifecycle_hook_name(self) -> str:
        return self._command.get('detail').get('LifecycleHookName')


    def get_autoscaling_group_name(self) -> str:
        return self._command.get('detail').get('AutoScalingGroupName')


    def get_instance_id(self) -> str:
        return self._command.get('detail').get('EC2InstanceId')


    def get_metadata(self) -> dict:
        return self._command.get('detail').get('NotificationMetadata')


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
    """
    logger = None
    formatter = None
    clients = None
    repositories = None

    _event = None
    _state = None
    seen_states = []

    EVENT = 'event'
    NODE = 'node'
    COMMAND = 'command'


    def __init__(self, clients: Clients, repositories: Repositories, logging: Logging):
        self.logger = logging.get_logger()
        self.repositories = repositories
        self.clients = clients
        self.formatter = logging.get_formatter()


    def load_event(self, message) -> Event:
        self._event = Event.from_sns_message(message)
        if self._event.has_command():
            command_id = self._event.get_raw_event().get('detail').get('command-id')
            command = self.get_command_repository().get(command_id)
            if command == { }:
                raise RuntimeError('Could not load command %s.' % command_id)
            self.get_command_repository().delete(command_id)
            self._event.set_command(command)

        self._event.node = self.get_node_repository().get(self._event.get_instance_id())
        if self._event.node.is_new():
            self._wait_for_cloud_init()

        # set the initial state
        self._state = self._event.node.get_state()

        # reset seen states
        self.seen_states = []

        self.report()

        return self._event


    def _wait_for_cloud_init(self):
        self.logger.debug("Waiting for node to be registered and cloud init to finish ...")
        node = self._event.node
        self.clients.get('dynamodb').wait_for_scan_count_is(
            1,
            'Ident = :id and ItemStatus = :status',
            {
                ":id": node.get_id(),
                ":status": 'finished_cloud_init'
            }
        )
        # fetch the node again to pick up all data probably set by cloud init
        self._event.node = self.repositories.get('node').get(node.get_id())


    @property
    def event(self) -> Event:
        return self._event


    @property
    def state(self) -> str:
        return self._state


    @state.setter
    def state(self, value: str):
        # ignore state updates until an event has been loaded
        # @see self.load_event()
        if self._state is None:
            return

        self._state = value
        self.repositories.get('node').update(self.event.node, {
            'ItemStatus': self._state
        })
        self.seen_states.append(self._state)


    def get_transitions(self):
        raise NotImplementedError()


    def report(self, force_report_autoscaling_activity: bool = False):
        if self._event.has_command():
            self.logger.debug('Reporting activity: %s', self._event.to_str())
            self.clients.get('sns').publish_activity(
                'SUCCESS' if self._event.is_successful() else "ERROR",
                self._event.to_str(),
                'eu-west-1'
            )

        if force_report_autoscaling_activity or self._event.is_autoscaling():
            activity = self.clients.get('autoscaling').get_activity(
                self._event.get_autoscaling_group_name(),
                self._event.is_launching(),
                self._event.node.get_id()
            )
            self.logger.debug('Reporting activity: %s', activity)
            self.clients.get('sns').publish_autoscaling_activity(activity, 'eu-west-1')


    def _send_command(self, event_data: EventData, comment: str, commands: list, target_nodes = None, command_timeout = 60):
        _event = self.get_event(event_data)
        if target_nodes is not None:
            target_nodes = listify(target_nodes)
        else:
            target_nodes = [_event.node]

        target_node_ids = []
        for node in target_nodes:
            target_node_ids.append(node.get_id())

        metadata = _event.get_command_metadata()
        metadata.update({ 'RunningOn': ', '.join(target_node_ids) })
        metadata.update({ 'Comment': comment })
        metadata.update({ 'Commands': ', '.join(commands) })

        command_id = self.clients.get('ssm').send_command(target_node_ids, comment, commands, command_timeout)
        self.repositories.get('command').register(command_id, metadata)


    #
    # convenience methods
    #

    def get_event(self, event_data: EventData) -> Event:
        return event_data.kwargs.get(self.EVENT)


    def get_node(self, event_data: EventData) -> Node:
        return self.get_event(event_data).node


    def get_node_repository(self) -> NodeRepository:
        return self.repositories.get(self.NODE)


    def get_command_repository(self) -> CommandRepository:
        return self.repositories.get(self.COMMAND)


    #
    # built-in trigger functions
    #

    def do_complete_lifecycle_action(self, event_data: EventData):
        _event = self.get_event(event_data)
        _node = _event.node
        _token = _event.get_lifecycle_action_token()
        if _node.has_property('LifecycleActionToken'):
            _token = _node.get_property('LifecycleActionToken')
            self.get_node_repository().unset_property(_node, ['LifecycleActionToken'])

        self.logger.info('completing autoscaling action for node %s', _node.to_dict())
        self.clients.get('autoscaling').complete_lifecycle_action(
            _event.get_lifecycle_hook_name(),
            _event.get_autoscaling_group_name(),
            _token,
            _event.get_lifecycle_result(),
            _node.get_id()
        )

        if _event.is_launching():
            self.clients.get('autoscaling').wait_for_activity_to_complete(
                _event.get_autoscaling_group_name(),
                _event.is_launching(),
                _node.get_id()
            )

        self.report(True)


    def do_remove_from_db(self, event_data: EventData):
        _node = self.get_event(event_data).node
        self.logger.info('removing node %s from db', _node.to_dict())
        self.repositories.get('node').delete(_node)


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
    :type __in_failure_handling: bool
    :type __raise_on_operation_failure: bool
    """
    machine_cls = Machine
    machine = None
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
        self.machine = self.machine_cls(model, auto_transitions = False, send_event = True, queued = False)

        self.__get_logger().debug('initializing transitions')
        for config in self.machine.model.get_transitions():
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
            after += [self.__continue_with_next_state]

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

    def __call__(self, message: dict):
        event = self.__get_model().load_event(message)

        if event.is_launching() and event.is_autoscaling() and not event.node.is_new():
            raise self.__get_formatter().get_error(RuntimeError, "Only new nodes can be launched.")

        if event.is_terminating() and event.node.is_new():
            raise self.__get_formatter().get_error(RuntimeError, "New nodes cannot terminate.")

        msg = 'processing event %s with data: (event)%s, (node)%s'
        self.__get_logger().info(msg, repr(event), event.get_event(), event.node.to_dict())

        # fail early, if no triggers can be found for the current state
        triggers = self.machine.get_triggers(self.__get_model().state)
        if len(triggers) < 1:
            raise RuntimeError('no trigger could be found for %s' % self.__get_model().state)

        self.__process(triggers, event)

        self.__get_logger().info('processing event %s complete', repr(event))


    def __process(self, triggers: list, event: Event):
        try:
            while len(triggers) > 0:
                state = self.__get_model().state
                self.__get_logger().debug('possible triggers for state %s: %s', state, triggers)
                for trigger in triggers:
                    # reset trigger condition
                    self.__raise_on_operation_failure = True

                    try:
                        self.__get_logger().info('pulling trigger %s on node %s', trigger, event.node.to_dict())
                        # the ide shows an argument error event should be of type dict, which is
                        # not correct due to incorrect doc comments. **kwargs is always of type tuple not dict
                        self.machine.dispatch(trigger, event = event)

                    except StopProcessingAfterStateChange as e:
                        self.__get_logger().info(e.get_message())
                        return

                    except StopIterationAfterTrigger as e:
                        self.__get_logger().info(e.get_message())
                        break

                    except Exception as e:
                        self.__get_clients().get('sns').publish_error(
                            e,
                            'launching' if event.is_launching() else 'terminating',
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
                        transitions = self.machine.events.get(trigger).transitions.get(self.__get_model().state)
                        if transitions is not None and transitions[0].dest is not None:
                            msg = 'Trigger pulled before state change or error in conditions. Forcing state to %s'
                            self.__get_logger().info(msg, transitions[0].dest)
                            self.__get_model().state = transitions[0].dest

                    self.__get_logger().info('trigger %s complete', trigger)

                    if self.__get_model().state != state:
                        # the state change has been applied
                        # proceed with triggers for the updated state
                        break

                if self.__get_model().state == state:
                    # state has not changed after pulling all triggers
                    # stop processing
                    break

                # load new triggers from updated state
                triggers = self.machine.get_triggers(self.__get_model().state)

        except Exception as e:
            if self.__in_failure_handling:
                self.__get_logger().exception("An error occured during failure handling.", repr(e))
                self.__get_clients().get('sns').publish_error(
                    e,
                    'failure handling while ' + 'launching' if event.is_launching() else 'terminating',
                    'eu-west-1'
                )
                raise

            msg = "An error occurred during transition. %s. Entering failure handling."
            self.__get_logger().exception(msg, repr(e))
            self.__in_failure_handling = True
            event.set_has_failure()

            self.machine.model.state = 'failure'
            triggers = self.machine.get_triggers(self.__get_model().state)
            if len(triggers) < 1:
                self.__get_logger().warning("No triggers for state failure found.")
            else:
                self.__process(triggers, event)


    #
    # private built-in trigger functions
    #

    def __log_transition(self, direction: str, event_data: EventData):
        self.__get_logger().info(
            '%s node %s from %s to %s via %s',
            direction,
            self.__get_node(event_data).get_id(),
            event_data.transition.source,
            event_data.transition.dest,
            event_data.event.name
        )
        self.__log_autoscaling_activity(event_data)


    def __log_before(self, event_data: EventData):
        self.__log_transition('Transitioning', event_data)


    def __log_after(self, event_data: EventData):
        self.__log_transition('Transitioned', event_data)


    def __is_event_successful(self, event_data: EventData) -> bool:
        status = self.__get_event(event_data).is_successful()
        if self.__get_event(event_data).has_command():
            if status:
                self.__get_logger().debug("Command was successful.")
            else:
                self.__get_logger().error("Command was not successful.")
                raise RuntimeError("Command was not successful.")

        return status


    def __continue_with_next_state(self, event_data: EventData):
        raise StopIterationAfterTrigger("Trigger forces to continue with next trigger. %s" % repr(event_data))


    def __wait_for_next_event(self, event_data: EventData):
        raise StopProcessingAfterStateChange("State requires to wait for the next event. %s" % repr(event_data))


    def __ignore_operation_failure(self, event_data: EventData):
        self.__get_logger().debug("%s requires to ignore exceptions.", repr(event_data))
        self.__raise_on_operation_failure = False


    def __log_autoscaling_activity(self, event_data: EventData):
        _event = self.__get_event(event_data)
        activity = self.machine.model.clients.get('autoscaling').get_activity(
            _event.get_autoscaling_group_name(), _event.is_launching(), _event.get_instance_id()
        )
        if activity == { }:
            self.__get_logger().warning('Could not find autoscaling activity for node %s', _event.get_instance_id())
            return

        self.__get_logger().info('%s %s autoscaling activity on event %s: %s', activity.get('StatusCode').upper(),
                                 'launching' if _event.is_launching else 'terminating',
                                 repr(event_data), activity)


    #
    # convenience methods
    #

    def __get_model(self) -> Model:
        return self.machine.model


    def __get_logger(self) -> Logger:
        return self.__get_model().logger


    def __get_formatter(self) -> MessageFormatter:
        return self.__get_model().formatter


    def __get_node_repository(self) -> NodeRepository:
        return self.__get_model().get_node_repository()


    def __get_command_repository(self) -> CommandRepository:
        return self.__get_model().get_command_repository()


    def __get_clients(self) -> Clients:
        return self.__get_model().clients


    def __get_event(self, event_data: EventData) -> Event:
        return self.__get_model().get_event(event_data)


    def __get_node(self, event_data: EventData) -> Node:
        return self.__get_model().get_node(event_data)
