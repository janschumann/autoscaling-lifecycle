from transitions import EventData
from transitions import Machine

from . import Event
from . import Node
from . import AutoscalingEvent
from . import SsmEvent
from .logging import Logging
from .clients import Clients
from .repository import Repositories


class StateHandler(object):
    """
    :param machine: A state machine instance
    :type machine: Machine
    :param __operations: A dict of operations
    :type __operations: dict
    :param _wait_for_next_event: If set to true, the execution will be suspended until the next event occurs
    :type _wait_for_next_event: bool
    :param _raise_on_operation_failuret: If set to true, transition will not halt on errors
    :type _raise_on_operation_failure: bool
    :param _node: The node that triggered the event
    :type _node: Node
    :param _event: The event
    :type _event: Event
    :param state: The current state
    :type state: str
    """
    machine = None
    __operations = { }
    _wait_for_next_event = False
    _raise_on_operation_failure = True
    _node = None
    _event = None
    state = 'new'


    def __init__(self, clients: Clients, repositories: Repositories, logging_factory: Logging):
        self.logger = logging_factory.get_logger()
        self.repositories = repositories
        self.clients = clients
        self.formatter = logging_factory.get_formatter()


    def prepare_machine(self, message: dict, send_event: bool = True):
        if message.get('source') == 'aws.autoscaling':
            self._event = AutoscalingEvent(message)

        elif message.get('source') == 'aws.ssm':
            repo = self.repositories.get('command')
            self._event = SsmEvent(message, repo.get(message.get('detail').get('command-id')))
            repo.delete(message.get('detail').get('command-id'))

        else:
            raise self.formatter.get_error(TypeError, 'Unknown event source ' + message.get('source'))

        self.logger.info('processing %s event: %s', repr(self._event), self._event.get_event())
        self.log_autoscaling_activity()

        self.logger.debug('loading node %s', self._event.get_instance_id())
        self._node = self.repositories.get('node').get(self._event.get_instance_id())
        self.logger.debug('node is %s', self._node.to_dict())
        if not self._event.is_successful():
            self.logger.error("Event not successful. Setting state to failure.")
            self.repositories.get('node').update(self._node, {
                'ItemStatus': 'failure'
            })
        self.logger.debug('initializing the machine')
        self.__initialize_machine(send_event)
        self.logger.debug('machine initialized with %s', self.__operations)


    def __call__(self, raise_on_failure = False):
        if self._event is None or self._node is None:
            self.logger.error('Machine is not initialized. Please call prepare_machine() before.')
            return

        self.logger.info('find trigger for %s', self._node.to_dict())
        try:
            for __op, __options in self.__operations.items():
                for __source in __options.get('sources'):
                    if __source == self._node.get_state():
                        self.logger.info('state %s matched. pulling trigger %s', __source, __op)
                        func = getattr(self, __op)
                        try:
                            func()
                        except Exception as e:
                            if self._raise_on_operation_failure:
                                raise
                            msg = "Ignoring failure %s in trigger %s failed. Proceed to next trigger."
                            self.logger.exception(msg, repr(e), __op)

                        self.logger.info('trigger %s complete', __op)
                        if self._wait_for_next_event:
                            return

        except Exception as e:
            self.logger.exception("An error occured during transition. %s. Setting state to failure.", repr(e))
            self.repositories.get('node').update(self._node, {
                'ItemStatus': 'failure'
            })
            # @todo is this necessary?
            #self.machine.initial = self._node.get_state()
            #self.state = self.machine.initial
            if raise_on_failure:
                raise

            self(True)

        self.logger.warning('termination criterion not met. no last operation found. Please make sure to add a transition calling wait_for_next_event() at the end of the transition.')


    def __initialize_machine(self, send_event: bool = True):
        self.machine = Machine(self, send_event = send_event, initial = self._node.get_state())
        self.state = self.machine.initial

        __transitions = []
        if self._event.is_launching():
            if self._event.get_source() is 'aws.autoscaling' and not self._node.is_new():
                raise self.formatter.get_error(RuntimeError, "Only new nodes can be launched.")

            __transitions = self._get_transitions().get('launching', [])

        elif self._event.is_terminating():
            if self._node.is_new():
                raise self.formatter.get_error(RuntimeError, "New nodes cannot terminate.")

            __transitions = self._get_transitions().get('terminating', [])

        for __transition in __transitions:
            self.__add_transition(
                sources = __transition.get('source'),
                dest = __transition.get('dest'),
                operations = __transition.get('operations')
            )


    def __add_transition(self, operations: list, sources, dest: str):
        if type(sources) is not list:
            sources = [sources]

        for __op in operations:
            self.__operations.update({
                __op.get('name'): {
                    'sources': sources
                }
            })

        self.machine.add_state(sources)
        self.machine.add_state(dest)

        for __op in operations:
            # first log the event, than do the action
            __before = [self.__log_before] + __op.get('before', [])

            # first update the node, than do the action and log the event
            __after = [self.__update_state] + __op.get('after', []) + [self.__log_after]

            __conditions = __op.get('conditions', [])
            # add successful event condition if needed
            if __op.get('require_successful_event', True):
                __conditions = [self.__is_event_successful] + __conditions

            self.machine.add_transition(
                __op.get('name'),
                sources,
                dest,
                conditions = __conditions,
                unless = __op.get('unless', []),
                before = __before,
                after = __after,
                prepare = __op.get('prepare', [])
            )


    def _get_transitions(self):
        raise NotImplementedError()


    def __log_transition(self, direction: str, event_data: EventData):
        self.logger.info(
            '%s node %s from %s to %s via %s',
            direction,
            self._node.get_id(),
            event_data.transition.source,
            event_data.transition.dest,
            event_data.event.name
        )
        self.log_autoscaling_activity(event_data)


    def __log_before(self, event_data: EventData):
        self.__log_transition('Transitioning', event_data)


    def __log_after(self, event_data: EventData):
        self.__log_transition('Transitioned', event_data)


    def __update_state(self, event_data: EventData):
        if event_data.transition.dest is not None:
            self.repositories.get('node').update(self._node, {
                'ItemStatus': event_data.transition.dest
            })
            #self.machine.initial = self._node.get_state()
            #self.state = self.machine.initial


    def _call_ssm_command(self, instance_id: str, comment: str, commands: list):
        """
        Initiate a ssm run command of type AWS-RunShellScript.
        We will also store the command metadata to the state.

        :type instance_id: str
        :param instance_id: The instance id to call the script on

        :type comment: str
        :param comment: The comment to display

        :type commands: list
        :param commands: A list of commands to execute
        """

        metadata = self._event.get_command_metadata()
        metadata.update({ 'RunningOn': instance_id })
        metadata.update({ 'Comment': comment })
        metadata.update({ 'Commands': ','.join(commands) })

        try:
            command_id = self.clients.get('ssm').send_command(instance_id, comment, commands)
            self.repositories.get('command').register(command_id, metadata)
        except Exception as e:
            self.clients.get('ssm').send_command(
                instance_id,
                'ABANDON NODE due to an error. See logs for details.',
                ['exit 1']
            )
            raise self.formatter.get_error(
                RuntimeError,
                'Could not send command %s. Node will be abandoned. Error was: %s',
                comment,
                repr(e)
            )


    def __is_event_successful(self, event_data: EventData) -> bool:
        return self._event.is_successful()


    def wait_for_next_event(self, event_data: EventData):
        self.logger.debug("%s requires to wait for the next event.", repr(event_data))
        self._wait_for_next_event = True


    def ignore_operation_failure(self, event_data: EventData):
        self.logger.debug("%s requires to ignore exceptions.", repr(event_data))
        self._raise_on_operation_failure = False


    def log_autoscaling_activity(self, event_data: EventData = None):
        activity = self.clients.get('autoscaling').get_activity(
            self._event.get_autoscaling_group_name(), self._event.is_launching(), self._event.get_instance_id()
        )
        self.logger.info('autoscaling activity: %s on op %s', activity, event_data)


    def do_complete(self, event_data: EventData):
        self.logger.info('completing autoscaling action for node %s', self._node.to_dict())
        self.clients.get('autoscaling').complete_lifecycle_action(
            self._event.get_lifecycle_hook_name(),
            self._event.get_autoscaling_group_name(),
            self._event.get_lifecycle_action_token(),
            self._event.get_lifecycle_result(),
            self._node.get_id()
        )

        self.clients.get('autoscaling').wait_for_activity_to_complete(
            self._event.get_autoscaling_group_name(),
            self._event.is_launching(),
            self._node.get_id()
        )


    def do_remove(self, event_data: EventData):
        self.logger.info('removing node %s from db', self._node.to_dict())
        self.repositories.get('node').delete(self._node)
