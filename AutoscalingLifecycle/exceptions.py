
class BaseError(RuntimeError):
    def get_message(self):
        return self.args[0]


class CommandNotFoundError(BaseError):
    pass


class ConfigurationError(BaseError):
    pass


class TriggerParameterConfigurationError(BaseError):
    pass


class StopIterationAfterTrigger(BaseError):
    """ Signal the end trigger from LifecycleHandler.__process(). """
    pass


class StopProcessingAfterStateChange(BaseError):
    """ Signal the end from LifecycleHandler.__process(). """
    pass


class EventNotSupportedError(BaseError):
    pass
