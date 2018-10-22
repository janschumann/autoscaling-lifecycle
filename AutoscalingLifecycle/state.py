from transitions import EventData
from transitions import Machine
from transitions import State

from AutoscalingLifecycle.event import Event
from AutoscalingLifecycle.helper.logger import LifecycleLogger
from AutoscalingLifecycle.entity.node import Node


class StateHandler(object):
    __states = { }
    __operations = { }
    _proceed = True


    def __init__(self, event: Event, clients: dict, repositories: dict, logger: LifecycleLogger):
        self.machine = Machine(self, send_event = True, initial = 'new')
        self._event = event
        self.logger = logger
        self.repositories = repositories
        self.clients = clients


    def _get_transitions(self):
        raise NotImplementedError()


    def __call__(self):
        __node = self.repositories.get('node').get(self._event.get_instance_id())

        self.__initialize_state(__node)

        self.logger.debug('looking for current state for node %s', __node)
        for __state in self.__operations.keys():
            self.logger.debug(
                'trying %s. state is %s, proceed is %s'
                , __state, __node.get_state(), self._proceed
            )
            if __state == __node.get_state() and self._proceed:
                self.logger.debug('state %s matched. proceeding ...', __state)
                for __trigger in self.__operations.get(__state):
                    if not self._proceed:
                        self.logger.debug('operation has been canceled by previous operation')
                        break

                    self.logger.debug('pulling trigger %s', __trigger.get('name'))
                    func = getattr(self, __trigger.get('name'))
                    func(__node)


    def __initialize_state(self, node: Node):
        self.machine.initial = node.get_state()

        __transitions = []
        if self._event.is_launching():
            if self._event.get_source() is 'aws.autoscaling' and not node.is_new():
                raise RuntimeError("Only new nodes can be launched.")

            __transitions = self._get_transitions().get('launching', [])

        elif self._event.is_terminating():
            if node.is_new():
                raise RuntimeError("New nodes cannot terminate.")

            __transitions = self._get_transitions().get('terminating', [])

        for __transition in __transitions:
            self.__add_transition(
                source = __transition.get('source'),
                dest = __transition.get('dest'),
                operations = __transition.get('operations')
            )


    def __add_transition(self, operations: list, source: str, dest: str):
        if source not in self.__states.keys():
            self.__states.update({ source: State(source) })
            self.machine.add_state(self.__states.get(source))

        if dest not in self.__states.keys():
            self.__states.update({ dest: State(dest) })
            self.machine.add_state(self.__states.get(dest))

        self.__operations.update({ source: operations })
        for __op in operations:
            # first log the event, than do the action
            __before = [self.__log_before] + __op.get('before', [])
            # first do the action, than update node state and log the event
            __after = __op.get('after', []) + [
                self.__update_node,
                self.__log_after
            ]

            self.machine.add_transition(
                __op.get('name'),
                self.__states.get(source),
                self.__states.get(dest),
                conditions = __op.get('conditions', []),
                unless = __op.get('unless', []),
                before = __before,
                after = __after,
                prepare = __op.get('prepare', [])
            )


    def __log_transition(self, direction: str, event_data: EventData):
        self.logger.info(
            '%s node %s from %s to %s via %s',
            direction,
            event_data.args[0].get_id(),
            event_data.transition.source,
            event_data.transition.dest,
            event_data.event.name
        )


    def __log_before(self, event_data: EventData):
        self.__log_transition('Transitioning', event_data)


    def __log_after(self, event_data: EventData):
        self.__log_transition('Transitioned', event_data)


    def __update_node(self, event_data: EventData):
        __node = event_data.args[0]
        self.repositories.get('node').update(__node, {
            'ItemStatus': event_data.transition.dest
        })
