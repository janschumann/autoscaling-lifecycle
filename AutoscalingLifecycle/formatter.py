import json
import datetime


class MessageFormatter(object):

    def __init__(self, name: str = ''):
        self.name = name


    def format(self, message: str, args) -> str:
        return ('%s: ' + message) % self.format_args(self.name, args)


    def format_args(self, name, args) -> tuple:
        if not args or len(args) == 0:
            return tuple([name])

        if type(args) is not tuple and type(args) is not list:
            args = [args]

        if type(args) is tuple:
            args = list(args)

        args = [name] + args

        formatted_args = []
        for arg in args:
            if type(arg) is not str:
                try:
                    arg = self.to_str(arg)

                except Exception:
                    arg = repr(arg)

            formatted_args.append(arg)

        return tuple(formatted_args)


    def to_str(self, data):
        return json.dumps(
            data,
            sort_keys = True,
            indent = 4,
            ensure_ascii = False,
            default = self.__json_convert
        )


    def __json_convert(self, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()


    def get_error(self, error_type, message: str, *args):
        """
        Returns a error type that can directly be used with raise()

        :type error_type: class
        :param error_type: The error type

        :type message: str
        :param message: The message with placeholders

        :type args: str
        :param args: A list of placeholder values

        :rtype Exception
        :return: The error object
        """
        return error_type(self.format(message, args))
