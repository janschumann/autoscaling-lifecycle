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


    @staticmethod
    def from_sns_message(message: dict, node_repository: NodeRepository, command_repository: CommandRepository):
        event = json.loads(message.get('Records')[0].get('Sns').get('Message'))

        if event.get('source') == 'aws.autoscaling':
            return AutoscalingEvent(event, node_repository, command_repository)

        elif event.get('source') == 'aws.ssm':
            return SsmEvent(event, node_repository, command_repository)

        else:
            raise RuntimeError('Unkonwn event %s', event.get('source'))


    def __init__(self, event: dict, node_repository: NodeRepository, command_repository: CommandRepository):
        self._event = event
        if self.has_command():
            self._command = command_repository.get(self._event.get('detail').get('command-id'))
            command_repository.delete(self._event.get('detail').get('command-id'))
        self.node = node_repository.get(self.get_instance_id())


    def to_str(self):
        msg = '%s in group "%s" on instance "%s"' % (
            self._event.get('detail-type'),
            self.get_autoscaling_group_name(),
            self.node.get_id()
        )
        return msg


    def get_event(self) -> dict:
        return self._event


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
        if self.is_terminating():
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


class AutoscalingEvent(Event):

    def __init__(self, event: dict, node_repository: NodeRepository, command_repository: CommandRepository):
        super().__init__(event, node_repository, command_repository)

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
        if self.is_terminating() or self.is_successful():
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

    __cloud_init_delay = 45

    EVENT = 'event'
    NODE = 'node'
    COMMAND = 'command'


    def __init__(self, clients: Clients, repositories: Repositories, logging: Logging):
        self.logger = logging.get_logger()
        self.repositories = repositories
        self.clients = clients
        self.formatter = logging.get_formatter()


    def load_event(self, message) -> Event:
        self._event = Event.from_sns_message(message, self.get_node_repository(), self.get_command_repository())
        # set the initial state
        self._state = self._event.node.get_state()
        # reset seen states
        self.seen_states = []

        self.report()

        return self.event


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


    def report(self):
        if self._event.is_autoscaling():
            activity = self.clients.get('autoscaling').get_activity(
                self._event.get_autoscaling_group_name(),
                self._event.is_launching(),
                self._event.node.get_id()
            )
            self.logger.debug('Reporting activity: %s', activity)
            self.clients.get('sns').publish_autoscaling_activity(activity, 'eu-west-1')

        if self._event.has_command():
            self.logger.debug('Reporting activity: %s', self._event.to_str())
            self.clients.get('sns').publish_activity(
                'SUCCESS' if self._event.is_successful() else "ERROR",
                self._event.to_str(),
                'eu-west-1'
            )


    def _send_command(self, event_data: EventData, comment: str, commands: list, target_node: Node = None):
        _event = self.get_event(event_data)
        if target_node is None:
            target_node = _event.node

        metadata = _event.get_command_metadata()
        metadata.update({ 'RunningOn': target_node.get_id() })
        metadata.update({ 'Comment': comment })
        metadata.update({ 'Commands': ','.join(commands) })

        command_id = self.clients.get('ssm').send_command(target_node.get_id(), comment, commands)
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

    def do_wait_for_cloud_init(self, event_data: EventData):
        self.logger.info('Wait %s for cloud init to become ready', str(self.__cloud_init_delay) + "s")
        time.sleep(self.__cloud_init_delay)


    def do_register(self, event_data: EventData):
        _event = self.get_event(event_data)
        _event.node.set_type(_event.get_metadata().get('type'))
        self.repositories.get('node').put(_event.node)


    def do_complete_lifecycle_action(self, event_data: EventData):
        _event = self.get_event(event_data)
        _node = _event.node
        self.logger.info('completing autoscaling action for node %s', _node.to_dict())
        try:
            self.clients.get('autoscaling').complete_lifecycle_action(
                _event.get_lifecycle_hook_name(),
                _event.get_autoscaling_group_name(),
                _event.get_lifecycle_action_token(),
                _event.get_lifecycle_result(),
                _node.get_id()
            )
        except Exception as e:
            self.logger.exception('failed to completete autoscaling action for node %s: %s', _node.to_dict(), repr(e))

        self.clients.get('autoscaling').wait_for_activity_to_complete(
            _event.get_autoscaling_group_name(),
            _event.is_launching(),
            _node.get_id()
        )

        self.report()


    def do_remove_from_db(self, event_data: EventData):
        _node = self.get_event(event_data).node
        self.logger.info('removing node %s from db', _node.to_dict())
        self.repositories.get('node').delete(_node)


class ConfigurationError(RuntimeError):
    def get_message(self):
        return self.args[0]


class TriggerParameterConfigurationError(ConfigurationError):
    pass


class LifecycleHandler(object):
    """
    :type machine: Machine
    :type __exception_count: int
    :type stop: bool
    :type _raise_on_operation_failure: bool
    """
    machine_cls = Machine
    machine = None
    __in_failure_handling = False
    __state_change_count = 0

    _stop = None
    _raise_on_operation_failure = None


    #
    # initialization
    #

    def __init__(self, model: Model):
        self.machine = self.machine_cls(model, auto_transitions = False, send_event = True, queued = False)

        self.__get_logger().debug('initializing transitions')
        for transition in self.machine.model.get_transitions():
            sources = listify(transition.get('source'))
            dest = transition.get('dest')

            self.machine.add_state(sources)
            self.machine.add_state(dest)

            for trigger in transition.get('triggers'):
                try:
                    self.__add_transition(sources, dest, trigger)
                except TriggerParameterConfigurationError as e:
                    msg = "Configuration error for source states '%s' in trigger '%s': '%s'" % (
                        ', '.join(sources), trigger.get('name'), e.get_message())
                    self.__get_logger().error(msg)
                    raise ConfigurationError(msg)


    def __add_transition(self, sources, dest, trigger: dict):
        # prepare functions will be executed first
        prepare = trigger.get('prepare', [])
        if type(prepare) is not list:
            raise TriggerParameterConfigurationError('prepare is not a list')

        if trigger.get('ignore_errors', False):
            # set ignore errors before any other function
            # so we set this as early as possible (in prepare)
            prepare = [self.__ignore_operation_failure] + prepare

        if trigger.get('last', False):
            # we need to stop after this operation
            # so we set this as early as possible (in prepare)
            prepare = prepare + [self.__wait_for_next_event]

        # conditions are checked before transition functions
        conditions = trigger.get('conditions', [])
        if type(conditions) is not list:
            raise TriggerParameterConfigurationError('conditions is not a list')
        if not trigger.get('ignore_errors', False):
            # require the event to be successful
            conditions = [self.__is_event_successful] + conditions
        unless = trigger.get('unless', [])
        if type(unless) is not list:
            raise TriggerParameterConfigurationError('unless is not a list')

        before = trigger.get('before', [])
        if type(before) is not list:
            raise TriggerParameterConfigurationError('before is not a list')
        # log the event before any other transition function
        before = [self.__log_before] + before

        after = trigger.get('after', [])
        if type(after) is not list:
            raise TriggerParameterConfigurationError('after is not a list')
        # set stop condition if needed
        if trigger.get('stop_after_state_change', False):
            after += [self.__wait_for_next_event]
        # logging is last
        after += after + [self.__log_after]

        self.machine.add_transition(
            trigger.get('name', 'default'),
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

        msg = 'processing event %s with data: (event)%s, (node)%s'
        self.__get_logger().info(msg, repr(event), event.get_event(), event.node.to_dict())

        # fail early, if no triggers can be found for the state
        triggers = self.machine.get_triggers(self.__get_model().state)
        if len(triggers) < 1:
            raise RuntimeError('no trigger could be found for %s', self.__get_model().state)

        if event.is_terminating():
            # reverse triggers for terminating events
            triggers = triggers[::-1]

        self.__process(triggers, event)

        self.__get_logger().info('processing event %s complete', repr(event))


    def __process(self, triggers: list, event: Event):
        try:
            while len(triggers) > 0:
                state = self.__get_model().state
                self.__get_logger().debug('possible triggers for state %s: %s', state, triggers)
                for trigger in triggers:
                    # reset trigger condition
                    self._stop = False
                    self._raise_on_operation_failure = True

                    self.__get_logger().info('pulling trigger %s on node %s', trigger, event.node.to_dict())
                    try:
                        # the ide shows an argument error event should be of type dict, which is
                        # not correct due to incorrect doc comments. **kwargs is always of type tuple not dict
                        self.machine.dispatch(trigger, event = event)
                    except Exception as e:
                        self.__get_clients().get('sns').publish_error(
                            e,
                            'launching' if event.is_launching() else 'terminating',
                            'eu-west-1'
                        )
                        if self._raise_on_operation_failure:
                            raise

                        self.__get_logger().exception('Ignoring failure %s in trigger %s.', repr(e), trigger)
                        # in case the error occurred somewhere before the state change,
                        # we need to find the destination state and update the model,
                        # to be able to proceed with next triggers
                        # a destination state of None indicates an internal transition and the model
                        # will not be updated
                        transitions = self.machine.events.get(trigger).transitions.get(self.__get_model().state)
                        if transitions is not None and transitions[0].dest is not None:
                            msg = 'Trigger pulled before state change or error in conditions. Forcing state to %s'
                            self.__get_logger().info(msg, transitions[0].dest)
                            self.__get_model().state = transitions[0].dest

                    self.__get_logger().info('trigger %s complete', trigger)

                    if self._stop:
                        self.__get_logger().debug('trigger %s caused the loop to stop', trigger)
                        return

                    if self.__get_model().state != state:
                        # the state change has been applied
                        # proceed with next triggers
                        break

                if self.__get_model().state == state:
                    # state has not changed after pulling all possible triggers
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

            self.machine.model.state = 'failure'
            triggers = self.machine.get_triggers(self.__get_model().state)
            if len(triggers) < 1:
                self.__get_logger().warning("No state failure found.")
            else:
                self.__process(triggers, event)
            return


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

        return status


    def __wait_for_next_event(self, event_data: EventData):
        self.__get_logger().debug("%s requires to wait for the next event.", repr(event_data))
        self._stop = True


    def __ignore_operation_failure(self, event_data: EventData):
        self.__get_logger().debug("%s requires to ignore exceptions.", repr(event_data))
        self._raise_on_operation_failure = False


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
