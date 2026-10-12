"""Tests for the closed deletion lifecycle of files and packages."""

import threading
import time

import pytest

from pyload.core.datatypes.exceptions import RemovalError
from pyload.core.datatypes.pyfile import PyFile
from pyload.core.datatypes.pypackage import PyPackage
from pyload.core.managers import file_manager as file_manager_module
from pyload.core.managers.file_manager import FileManager
from pyload.core.network.exceptions import Retry
from pyload.core.threads.download_thread import DownloadThread


class FakeDB:
    def __init__(self):
        self.links = {}
        self.packages = {}
        self.update_calls = []
        self.fail_delete_link = False
        self.fail_delete_package = False
        self.commits = 0
        self.link_deletes = 0
        self.package_deletes = 0

    def update_link(self, f):
        self.update_calls.append(f.id)

    def delete_link(self, f):
        if self.fail_delete_link:
            raise RuntimeError("disk on fire")

        self.link_deletes += 1
        pid = f.packageid
        order = self.links[f.id].order
        del self.links[f.id]
        for link in self.links.values():
            if link.packageid == pid and link.order > order:
                link.order -= 1

    def delete_package(self, p):
        if self.fail_delete_package:
            raise RuntimeError("disk on fire")

        self.package_deletes += 1
        for fid in [k for k, v in self.links.items() if v.packageid == p.id]:
            del self.links[fid]
        del self.packages[p.id]

    def get_file(self, id):
        return self.links.get(id)

    def get_link_data(self, id):
        if id not in self.links:
            return None
        f = self.links[id]
        return {id: {"id": f.id, "package": f.packageid, "order": f.order}}

    def get_package(self, id):
        return self.packages.get(id)

    def get_package_data(self, id):
        return {
            fid: {"id": fid}
            for fid, link in self.links.items()
            if link.packageid == id
        }

    def sync_save(self):
        self.commits += 1

    def commit(self):
        self.commits += 1


class FakeEventManager:
    def __init__(self):
        self.events = []

    def add_event(self, event):
        self.events.append(event.to_list())


class FakeAddonManager:
    def __init__(self):
        self.calls = []

    def dispatch_event(self, event, *args):
        self.calls.append((event,) + args)

    def download_preparing(self, pyfile):
        self.calls.append(("download_preparing", pyfile.id))

    def download_finished(self, pyfile):
        self.calls.append(("download_finished", pyfile.id))

    def download_failed(self, pyfile):
        self.calls.append(("download_failed", pyfile.id))


class FakeLog:
    def info(self, *a, **k):
        pass

    warning = error = debug = info


class FakeThreadManager:
    def __init__(self):
        self.active_ids = set()
        self.pending_ids = set()
        self.lock = threading.Lock()

    def processing_ids(self):
        return list(self.active_ids)

    def is_pending(self, fid):
        with self.lock:
            return fid in self.pending_ids

    def register_pending(self, fid):
        with self.lock:
            self.pending_ids.add(fid)

    def discard_pending(self, fid):
        with self.lock:
            self.pending_ids.discard(fid)


class FakeReq:
    def __init__(self):
        self.aborted = 0

    def abort_downloads(self):
        self.aborted += 1


class FakePlugin:
    def __init__(self):
        self.req = FakeReq()
        self.cleaned = 0
        self.preprocessing_calls = 0

    def clean(self):
        self.cleaned += 1


def make_manager(db=None):
    core = type("Core", (), {})()
    core._ = lambda value: value
    core.db = db or FakeDB()
    core.log = FakeLog()
    core.event_manager = FakeEventManager()
    core.addon_manager = FakeAddonManager()
    core.thread_manager = FakeThreadManager()

    manager = FileManager(core)
    return manager, core


def make_file(manager, fid=1, pid=1, status=3, order=0, plugin=True):
    f = PyFile(
        manager,
        fid,
        f"http://example.com/{fid}",
        f"file{fid}",
        0,
        status,
        "",
        "Http",
        pid,
        order,
    )
    if plugin:
        f.plugin = FakePlugin()
    manager.pyload.db.links[fid] = f
    return f


def make_package(manager, pid=1, queue=1, order=0):
    package = PyPackage(manager, pid, "pack", "folder", "", "", queue, order)
    manager.pyload.db.packages[pid] = package
    return package


def wait_until(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition was not met in time")
        time.sleep(0.01)


def test_delete_idle_file_publishes_once_and_reorders_siblings():
    manager, core = make_manager()
    make_package(manager)
    f1 = make_file(manager, fid=1, order=0)
    f2 = make_file(manager, fid=2, order=1, plugin=False)

    assert manager.delete_link(1) is True

    assert 1 not in manager.cache
    assert 1 not in core.db.links
    assert core.db.commits >= 1
    assert f2.order == 0  # sibling closed the gap

    remove_events = [e for e in core.event_manager.events if e[0] == "remove"]
    assert remove_events == [["remove", "queue", "file", 1]]
    assert ("file_deleted", 1, 1) in core.addon_manager.calls
    assert core.db.packages  # package still there

    # repeat deletion is a silent, event-free no-op
    before = len(core.event_manager.events)
    assert manager.delete_link(1) is False
    assert len(core.event_manager.events) == before


def test_delete_last_file_cleans_empty_package():
    manager, core = make_manager()
    make_package(manager)
    make_file(manager, fid=1, order=0)

    assert manager.delete_link(1) is True

    assert 1 not in core.db.packages
    assert 1 not in manager.package_cache
    assert ["remove", "queue", "file", 1] in core.event_manager.events
    assert ["remove", "queue", "pack", 1] in core.event_manager.events
    assert ("package_deleted", 1) in core.addon_manager.calls


def test_delete_running_file_waits_for_worker_convergence():
    manager, core = make_manager()
    make_package(manager)
    f = make_file(manager, fid=1, order=0, status=12)
    core.thread_manager.active_ids.add(1)

    def worker():
        wait_until(lambda: f.abort)
        # worker epilogue, as run by the download/decrypter threads
        f.release()
        core.thread_manager.active_ids.discard(1)
        # late epilogue writes must not resurrect the removed record
        f.sync()
        f.notify_change()
        f.delete()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert manager.delete_link(1) is True
    finally:
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert 1 not in core.db.links
    assert 1 not in manager.cache
    assert core.db.update_calls.count(1) == 0  # nothing written back
    assert ["remove", "queue", "file", 1] in core.event_manager.events


def test_delete_pending_assignment_waits_for_pickup():
    manager, core = make_manager()
    make_package(manager)
    f = make_file(manager, fid=1, order=0)
    core.thread_manager.pending_ids.add(1)

    def worker():
        wait_until(lambda: f.abort)
        core.thread_manager.discard_pending(1)

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert manager.delete_link(1) is True
    finally:
        thread.join(timeout=2)

    assert 1 not in core.db.links


def test_unresponsive_transfer_returns_retryable_failure():
    manager, core = make_manager()
    make_package(manager)
    f = make_file(manager, fid=1, order=0, status=12)
    core.thread_manager.active_ids.add(1)  # never converges

    old_timeout = file_manager_module.ABORT_TIMEOUT
    file_manager_module.ABORT_TIMEOUT = 0.2
    try:
        with pytest.raises(RemovalError) as exc:
            manager.delete_link(1)
    finally:
        file_manager_module.ABORT_TIMEOUT = old_timeout

    assert exc.value.file_ids == [1]

    # entry stays visible in a stopped, retryable state
    assert 1 in manager.cache
    assert 1 in core.db.links
    assert f.status == 9  # aborted
    assert f.removed is False
    assert core.db.link_deletes == 0
    assert not any(e[0] == "remove" for e in core.event_manager.events)
    assert not any(c[0] == "file_deleted" for c in core.addon_manager.calls)


def test_database_failure_restores_consistency():
    manager, core = make_manager()
    make_package(manager)
    f = make_file(manager, fid=1, order=0)
    core.db.fail_delete_link = True

    with pytest.raises(RemovalError):
        manager.delete_link(1)

    # record and cache stay consistent, no success event was sent
    assert 1 in core.db.links
    assert 1 in manager.cache
    assert f.removed is False
    assert f.status == 9
    assert not any(e[0] == "remove" for e in core.event_manager.events)
    assert not any(c[0] == "file_deleted" for c in core.addon_manager.calls)


def test_delete_package_converges_all_children():
    manager, core = make_manager()
    p = make_package(manager, pid=1, order=0)
    make_package(manager, pid=2, order=1)  # sibling package
    f1 = make_file(manager, fid=1, pid=1, order=0, plugin=False)
    f2 = make_file(manager, fid=2, pid=1, order=1)
    core.thread_manager.active_ids.add(2)

    def worker():
        wait_until(lambda: f2.abort)
        f2.release()
        core.thread_manager.active_ids.discard(2)

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert manager.delete_package(1) is True
    finally:
        thread.join(timeout=2)

    assert 1 not in core.db.packages
    assert core.db.link_deletes == 0  # package transaction owns the links
    assert core.db.package_deletes == 1
    assert 1 not in manager.package_cache
    assert manager.package_cache[2].order == 0  # sibling closed the gap
    assert ["remove", "queue", "pack", 1] in core.event_manager.events
    assert ["update", "queue", "pack", 2] in core.event_manager.events
    assert core.addon_manager.calls == [("package_deleted", 1)]


def test_delete_package_fails_atomically_when_one_child_stuck():
    manager, core = make_manager()
    make_package(manager, pid=1)
    f1 = make_file(manager, fid=1, pid=1, order=0, plugin=False)
    f2 = make_file(manager, fid=2, pid=1, order=1, status=12)
    core.thread_manager.active_ids.add(2)

    old_timeout = file_manager_module.ABORT_TIMEOUT
    file_manager_module.ABORT_TIMEOUT = 0.2
    try:
        with pytest.raises(RemovalError) as exc:
            manager.delete_package(1)
    finally:
        file_manager_module.ABORT_TIMEOUT = old_timeout

    assert exc.value.package_ids == [1]

    # nothing was partially released, deleted or reordered
    assert core.db.package_deletes == 0
    assert 1 in core.db.packages
    assert 1 in manager.package_cache
    assert 1 in manager.cache and 2 in manager.cache
    assert f1.status == 3 and f1.removed is False
    assert f2.status == 9 and f2.removed is False
    assert not any(e[0] == "remove" for e in core.event_manager.events)
    assert not any(
        c[0] in ("file_deleted", "package_deleted") for c in core.addon_manager.calls
    )


def test_repeat_package_deletion_is_silent():
    manager, core = make_manager()

    assert manager.delete_package(99) is False
    assert core.event_manager.events == []
    assert core.addon_manager.calls == []


# ---------------------------------------------------------------------------
# worker thread level guarantees
# ---------------------------------------------------------------------------


class FakeThreadPyFile:
    def __init__(self, removed=False, abort=False, plugin=None):
        self.id = 7
        self.name = "fake"
        self.pluginname = "Fake"
        self.removed = removed
        self.abort = abort
        self.status = 3
        self.error = ""
        self.plugin = plugin
        self.released = 0

    def has_plugin(self):
        return self.plugin is not None

    def release(self):
        self.released += 1

    def set_status(self, status):
        self.status = status

    def check_if_processed(self):
        pass

    def finish_if_done(self):
        pass


class ThreadPlugin:
    def __init__(self, behavior):
        self.req = None
        self.behavior = behavior
        self.preprocessing_calls = 0
        self.cleaned = 0

    def check_for_same_files(self, starting=False):
        pass

    def preprocessing(self, thread):
        self.preprocessing_calls += 1
        if self.behavior == "retry":
            self.behavior = "succeed"
            raise Retry("nope")

    def clean(self):
        self.cleaned += 1


class FakePyLoad:
    def __init__(self):
        class _Log:
            def info(self, *a, **k):
                pass

            warning = error = debug = info

        self.log = _Log()
        self.debug = 0

        class Files:
            def save(self_inner):
                pass

            def check_package_finished(self_inner, pyfile):
                pass

        self.files = Files()

        class Addons:
            def __init__(self_inner):
                self_inner.calls = []

            def download_preparing(self_inner, pyfile):
                self_inner.calls.append("preparing")

            def download_finished(self_inner, pyfile):
                self_inner.calls.append("finished")

            def download_failed(self_inner, pyfile):
                self_inner.calls.append("failed")

        self.addon_manager = Addons()


class FakeDownloadManager:
    def __init__(self, pyload):
        self.pyload = pyload
        self._ = lambda value: value
        self.threads = []
        self.reconnecting = threading.Event()
        self.pending = set()

    def register_pending(self, fid):
        self.pending.add(fid)

    def discard_pending(self, fid):
        self.pending.discard(fid)


def _drain(thread, timeout=2.0):
    thread.stop()
    thread.join(timeout=timeout)
    assert not thread.is_alive()


def test_worker_skips_job_claimed_while_queued():
    pyload = FakePyLoad()
    tm = FakeDownloadManager(pyload)
    thread = DownloadThread(tm)
    tm.threads.append(thread)
    plugin = ThreadPlugin("succeed")
    job = FakeThreadPyFile(removed=True, abort=True, plugin=plugin)

    thread.put(job)
    wait_until(lambda: job.released > 0)
    assert plugin.preprocessing_calls == 0
    _drain(thread)
    assert 7 not in tm.pending


def test_late_retry_does_not_resurrect_cancelled_job():
    pyload = FakePyLoad()
    tm = FakeDownloadManager(pyload)
    thread = DownloadThread(tm)
    tm.threads.append(thread)
    plugin = ThreadPlugin("retry")
    job = FakeThreadPyFile(removed=False, abort=True, plugin=plugin)

    thread.put(job)
    wait_until(lambda: job.released > 0)
    time.sleep(0.1)
    assert thread.queue.qsize() == 0  # not re-queued
    assert job.status == "aborted"
    _drain(thread)


def test_normal_retry_flow_still_compatible():
    pyload = FakePyLoad()
    tm = FakeDownloadManager(pyload)
    thread = DownloadThread(tm)
    tm.threads.append(thread)
    plugin = ThreadPlugin("retry")  # first call retries, second succeeds
    job = FakeThreadPyFile(removed=False, abort=False, plugin=plugin)

    thread.put(job)
    wait_until(lambda: "finished" in pyload.addon_manager.calls)
    assert plugin.preprocessing_calls == 2
    _drain(thread)
