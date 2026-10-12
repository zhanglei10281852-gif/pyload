

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


class RemovalError(Exception):
    """
    Raised when a file or package deletion could not be completed safely.

    The failure is definite but retryable: no record was removed, no deletion
    event was dispatched and the affected entries were left in a stopped,
    manageable state.
    """

    __slots__ = ["msg", "file_ids", "package_ids"]

    def __init__(self, msg=None, file_ids=None, package_ids=None):
        self.msg = msg
        self.file_ids = list(file_ids or [])
        self.package_ids = list(package_ids or [])
        super().__init__(msg or "Deletion failed, please retry")
