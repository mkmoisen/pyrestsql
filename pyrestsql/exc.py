class RestError(Exception):
    code = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        message = args[0] if args else ''
        self.messages = [message] if isinstance(message, (str, bytes)) else message


class UserError(RestError):
    code = 400


class ServerError(RestError):
    code = 500


class BadInput(UserError):
    code = 400


class EntityNotFound(UserError):
    code = 404


class AuthenticationError(UserError):
    code = 401


class AuthorizationError(UserError):
    code = 403


class MethodNotAllowed(UserError):
    code = 405