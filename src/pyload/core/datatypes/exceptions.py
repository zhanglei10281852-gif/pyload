

class FileDoesNotExists(Exception):
    __slots__ = ["fid"]

    def __init__(self, fid=None):
        self.fid = fid


class PackageDoesNotExists(Exception):
    __slots__ = ["pid"]

    def __init__(self, pid=None):
        self.pid = pid


class ServiceDoesNotExists(Exception):
    __slots__ = ["plugin", "func"]

    def __init__(self, plugin=None, func=None):
        self.plugin = plugin
        self.func = func


class ServiceException(Exception):
    __slots__ = ["msg"]

    def __init__(self, msg=None):
        self.msg = msg


class CaptchaTaskException(Exception):
    """Base class for captcha task protocol violations."""

    __slots__ = ["msg"]

    #: suggested HTTP status code for REST clients
    status_code = 409

    def __init__(self, msg=None):
        self.msg = msg or self.__class__.__name__


class CaptchaTaskNotFound(CaptchaTaskException):
    """The task id is unknown or the task was already purged."""

    status_code = 404


class CaptchaTaskExpired(CaptchaTaskException):
    """The task overall deadline elapsed without an accepted result."""

    status_code = 410


class CaptchaTaskOwnership(CaptchaTaskException):
    """The credential does not match the current task generation/owner."""

    status_code = 409


class CaptchaTaskSolved(CaptchaTaskException):
    """The task was already solved by another party, it cannot be overwritten."""

    status_code = 409


class CaptchaResultInvalid(CaptchaTaskException):
    """The submitted result cannot be parsed for the task result type."""

    status_code = 400
