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
    :param __states: A dict of states
    :type __states: dict
    :param __operations: A dict of operations
    :type __operations: dict
    :param _wait_for_next_event: If set to true, the execution will be suspended until the next event occurs
    :type _wait_for_next_event: bool
    :param _node: The node that triggered the event
    :type _node: Node
    :param _event: The event
    :type _event: Event
    :param state: The current state
    :type state: str
    """
    __states = { }
    __operations = { }
    _wait_for_next_event = False
    _node = None
    _event = None
    state = 'new'


    def __init__(self, clients: Clients, repositories: Repositories, logging_factory: Logging):
        self.logger = logging_factory.get_logger()
        self.repositories = repositories
        self.clients = clients
        self.formatter = logging_factory.get_formatter()


    def prepare_machine(self, message: dict):
        if message.get('source') == 'aws.autoscaling':
            self._event = AutoscalingEvent(message)

        elif message.get('source') == 'aws.ssm':
            repo = self.repositories.get('command')
            command = repo.get(message.get('detail').get('command-id'))
            self._event = SsmEvent(message, command)
            repo.delete(command.get('id'))

        else:
            raise self.formatter.get_error(TypeError, 'Unknown event source ' + message.get('source'))

        self.logger.debug('loading node %s', self._event.get_instance_id())
        self._node = self.repositories.get('node').get(self._event.get_instance_id())
        self.logger.debug('node is %s', self._node.to_dict())

        self.logger.debug('initializing the machine')
        self.__initialize_machine()
        self.logger.debug('machine initialized with %s', self.__operations)


    def __call__(self):
        if self._event is None or self._node is None:
            self.logger.error('Machine is not initialized')

        self.logger.info('processing event: %s', self._event.get_event())
        self.logger.info('execute transitions for %s', self._node.to_dict())
        for __op, __options in self.__operations.items():
            for __source in __options.get('sources'):
                if __source == self._node.get_state():
                    self.logger.debug('node state %s matched %s. proceeding.', self._node.get_state(), __source)
                    self.logger.debug('pulling trigger %s', __op)
                    func = getattr(self, __op)
                    func()
                    if self._wait_for_next_event:
                        self.logger.debug('%s requires to wait for the next event.', __op)
                        return


    def __initialize_machine(self):
        self.machine = Machine(self, send_event = True, initial = self._node.get_state())
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
            __after = [self.__update_node] + __op.get('after', []) + [self.__log_after]

            self.machine.add_transition(
                __op.get('name'),
                sources,
                dest,
                conditions = __op.get('conditions', []),
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


    def __log_before(self, event_data: EventData):
        self.__log_transition('Transitioning', event_data)


    def __log_after(self, event_data: EventData):
        self.__log_transition('Transitioned', event_data)


    def __update_node(self, event_data: EventData):
        self.repositories.get('node').update(self._node, {
            'ItemStatus': event_data.transition.dest
        })


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

        self._wait_for_next_event = True

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
