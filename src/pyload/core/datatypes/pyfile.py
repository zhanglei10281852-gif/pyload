import time
from threading import RLock

from ..managers.event_manager import UpdateEvent
from ..utils import format, purge
from ..utils.struct.lock import lock

#: maximum time to wait for a download/decryption transport to honor a
#: cancellation request before giving up and reporting a retryable failure
ABORT_TIMEOUT = 30

status_map = {
    "finished": 0,
    "offline": 1,
    "online": 2,
    "queued": 3,
    "skipped": 4,
    "waiting": 5,
    "temp. offline": 6,
    "starting": 7,
    "failed": 8,
    "aborted": 9,
    "decrypting": 10,
    "custom": 11,
    "downloading": 12,
    "processing": 13,
    "unknown": 14,
}


def _set_size(self, value):
    self._size = int(value)


def _set_name(self, value):
    self._name = purge.name(value, sep="")


class PyFile:
    """
    Represents a file object at runtime.
    """

    def __init__(
        self, manager, id, url, name, size, status, error, pluginname, package, order
    ):
        self.m = self.manager = manager
        self.m.cache[int(id)] = self

        self.id = int(id)
        self.url = url
        self._name = None
        self.name = name

        self._size = None
        self.size = size

        self.status = status
        self.pluginname = pluginname
        self.packageid = package  #: should not be used, use package() instead
        self.error = error
        self.order = order
        # database information ends here

        self.lock = RLock()

        self.plugin = None
        # self.download = None

        self.wait_until = 0  #: time.monotonic() + time to wait

        # status attributes
        self.active = False  #: obsolete?
        self.abort = False
        self.reconnected = False

        #: True once deletion of this record has been claimed; worker threads
        #: must then stop persisting state, sending events or firing callbacks
        self.removed = False

        self.statusname = None

        self.progress = 0
        self.maxprogress = 100

    # will convert all sizes to ints
    size = property(lambda self: self._size, _set_size)

    # ensure safe name assignment
    name = property(lambda self: self._name, _set_name)

    def __repr__(self):
        return f"PyFile {self.id}: {self.name}@{self.pluginname}"

    @lock
    def init_plugin(self):
        """
        inits plugin instance.
        """
        if not self.plugin:
            self.pluginmodule = self.m.pyload.plugin_manager.get_plugin(self.pluginname)
            self.pluginclass = getattr(
                self.pluginmodule,
                self.m.pyload.plugin_manager.get_plugin_name(self.pluginname),
            )
            self.plugin = self.pluginclass(self)

    @lock
    def has_plugin(self):
        """
        Thread safe way to determine this file has initialized plugin attribute.

        :return:
        """
        return hasattr(self, "plugin") and self.plugin

    def package(self):
        """
        return package instance.
        """
        return self.m.get_package(self.packageid)

    def set_status(self, status):
        self.status = status_map[status]
        self.sync()  # TODO: needed as long no better job approving exists

    def set_custom_status(self, msg, status="processing"):
        self.statusname = msg
        self.set_status(status)

    def get_status_name(self):
        if self.status not in (13, 14) or not self.statusname:
            return self.m.status_msg[self.status]
        else:
            return self.statusname

    def has_status(self, status):
        return status_map[status] == self.status

    @lock
    def claim_removal(self):
        """
        claim this pyfile for deletion.

        Marks the record as being removed and requests cancellation of any
        running transfer. Idempotent: only the first claimant gets ``True``,
        so repeated deletions and worker epilogues cannot resurrect the record
        or trigger duplicate callbacks.

        :return: True on the first claim, False if it was already claimed
        """
        if self.removed:
            return False

        self.removed = True
        self.abort = True
        self._request_abort()
        return True

    @lock
    def unclaim_removal(self):
        """
        revoke a removal claim after a deletion failed, keeping the record
        visible and manageable.
        """
        self.removed = False

    def _request_abort(self):
        """
        signal cancellation to the plugin/transport without blocking.
        """
        self.abort = True
        plugin = self.plugin
        req = getattr(plugin, "req", None) if plugin is not None else None
        if req is not None:
            try:
                req.abort_downloads()
            except Exception:
                pass

    def sync(self):
        """
        sync PyFile instance with database.
        """
        if self.removed:
            return

        self.m.update_link(self)

    @lock
    def release(self):
        """
        sync and remove from cache.
        """
        if self.removed:
            # record is gone or being deleted: the worker epilogue must not
            # write anything back, only drop its runtime resources
            if self.has_plugin():
                try:
                    self.plugin.clean()
                except Exception:
                    pass
                del self.plugin

            self.m.release_link(self.id)
            return

        # file has valid package
        if self.packageid > 0:
            self.sync()

        if self.has_plugin():
            self.plugin.clean()
            del self.plugin

        self.m.release_link(self.id)

    def delete(self):
        """
        delete pyfile from database.
        """
        if self.removed:
            # deletion is already driven by the FileManager (or completed);
            # a late worker epilogue must not run it a second time
            return

        self.m.delete_link(self.id)

    def to_dict(self):
        """
        return dict with all information for interface.
        """
        return self.to_db_dict()

    def to_db_dict(self):
        """
        return data as dict for databse.

        format:

        {
            id: {'url': url, 'name': name ... }
        }
        """
        return {
            self.id: {
                "id": self.id,
                "url": self.url,
                "name": self.name,
                "plugin": self.pluginname,
                "size": self.get_size(),
                "format_size": self.format_size(),
                "status": self.status,
                "statusmsg": self.get_status_name(),
                "package": self.packageid,
                "error": self.error,
                "order": self.order,
            }
        }

    def abort_download(self, timeout=None):
        """
        abort pyfile if possible.

        Requests cancellation and waits until the file is no longer assigned
        to (or processed by) any worker thread, so its request resources are
        being released by the worker's own epilogue.

        :param timeout: maximum seconds to wait; ``None`` waits indefinitely
        :return: True once the transfer has converged, False if the timeout
            elapsed before the transport honored the cancellation
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        thread_manager = self.m.pyload.thread_manager

        while self.id in thread_manager.processing_ids() or thread_manager.is_pending(
            self.id
        ):
            self._request_abort()
            if deadline is not None and time.monotonic() >= deadline:
                return False

            time.sleep(0.1)

        self.abort = False
        plugin = getattr(self, "plugin", None)
        req = getattr(plugin, "req", None) if plugin is not None else None
        if req is not None:
            try:
                req.abort_downloads()
            except Exception:
                pass

        self.release()
        return True

    def finish_if_done(self):
        """
        set status to finish and release file if every thread is finished with it.
        """
        if self.removed:
            self.release()
            return False

        if self.id in self.m.pyload.thread_manager.processing_ids():
            return False

        self.set_status("finished")
        self.release()
        self.m.check_all_links_finished()
        return True

    def check_if_processed(self):
        if self.removed:
            return

        self.m.check_all_links_processed(self.id)

    def format_wait(self):
        """
        formats and return wait time in human readable format.
        """
        seconds = int(self.wait_until - time.monotonic())
        return format.time(seconds, literally=False)

    def format_size(self):
        """
        formats size to readable format.
        """
        return format.size(self.get_size())

    def format_eta(self):
        """
        formats eta to readable format.
        """
        seconds = self.get_eta()
        return format.time(seconds, literally=False)

    def get_speed(self):
        """
        calculates speed.
        """
        try:
            return self.plugin.req.speed
        except Exception:
            return 0

    def get_eta(self):
        """
        gets established time of arrival.
        """
        try:
            return int(self.get_bytes_left() // self.get_speed())
        except ZeroDivisionError:
            return 0

    def get_bytes_left(self):
        """
        gets bytes left.
        """
        try:
            return max(self.get_size() - self.plugin.req.arrived, 0)
        except Exception:
            return 0

    def get_percent(self):
        """
        get % of download.
        """
        if self.status == 12:
            try:
                return self.plugin.req.percent
            except Exception:
                return 0
        else:
            return self.progress

    def get_size(self):
        """
        get size of download.
        """
        try:
            if self.plugin.req.size:
                return self.plugin.req.size
            else:
                return self.size
        except Exception:
            return self.size

    def notify_change(self):
        if self.removed:
            return

        e = UpdateEvent(
            "file", self.id, "collector" if not self.package().queue else "queue"
        )
        self.m.pyload.event_manager.add_event(e)

    def set_progress(self, value):
        if value != self.progress:
            self.progress = value
            self.notify_change()

    def set_name(self, value):
        if value != self.name:
            self.name = value
            self.notify_change()
