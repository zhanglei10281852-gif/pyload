"""
Tests for the checkOnlineStatus job machinery:

* stable, per-process non-reusable result ids
* replayable batch polling with independent consumer cursors
* per-job activity timers and expiration
* distinguishable terminal states (plugin error / container error / expired)
* legacy single-consumer pollResults behavior
"""
import time
import types

import pytest

from pyload.core.datatypes.data import OnlineCheck, OnlineCheckStatus, OnlineStatus
from pyload.core.managers import thread_manager as thread_manager_module
from pyload.core.managers.thread_manager import (
    RESULT_JOB_TTL,
    RESULT_JOB_TOMBSTONE_TTL,
    ThreadManager,
)
from pyload.core.threads.info_thread import InfoThread


# ---------------------------------------------------------------------------
# fakes / fixtures
# ---------------------------------------------------------------------------

class FakeConfig:
    def get(self, section, option):
        if (section, option) == ("download", "max_downloads"):
            return 0
        return False


class FakeLog:
    def debug(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass


class FakeCore:
    def __init__(self):
        self._ = lambda value: value
        self.config = FakeConfig()
        self.log = FakeLog()
        self.debug = 0


class FakeInfoThread:
    """Replaces InfoThread so create_result_thread starts no real thread."""

    instances = []

    def __init__(self, manager, data, pid=-1, rid=-1, add=False):
        self.manager = manager
        self.data = data
        self.pid = pid
        self.rid = rid
        self.add = add
        self.instances.append(self)


class FakePluginManager:
    def __init__(self, container_plugins=(), bad_plugin=False):
        self.container_plugins = {name: {} for name in container_plugins}
        self.bad_plugin = bad_plugin

    def get_plugin(self, plugin_name, original=False):
        if self.bad_plugin:
            raise RuntimeError("plugin load failed")
        return object()


class BadInfoPlugin:
    #: get_info raises as if a hoster plugin failed fetching online status
    def get_info(self, urls):
        raise RuntimeError("info fetch failed")


class GoodInfoPlugin:
    def __init__(self, result):
        self._result = result

    def get_info(self, urls):
        yield self._result


@pytest.fixture
def manager(monkeypatch):
    FakeInfoThread.instances = []
    monkeypatch.setattr(thread_manager_module, "InfoThread", FakeInfoThread)
    core = FakeCore()
    core.plugin_manager = FakePluginManager()
    return ThreadManager(core)


def make_status(url, status=2):
    return OnlineStatus(
        name=url.rsplit("/", 1)[-1],
        plugin="X",
        packagename="f",
        status=status,
        size=1,
    )


def append_urls(thread_manager, rid, urls, status=2):
    thread_manager.append_result_batch(
        rid, {url: make_status(url, status) for url in urls}
    )


def backdate(job, seconds, attribute="last_activity"):
    setattr(job, attribute, time.monotonic() - seconds)


def make_real_info_thread(thread_manager, data, rid):
    """Builds an InfoThread bound to the real manager without starting it."""
    info_thread = InfoThread.__new__(InfoThread)
    info_thread.m = info_thread.manager = thread_manager
    info_thread.pyload = thread_manager.pyload
    info_thread._ = thread_manager.pyload._
    info_thread.data = data
    info_thread.pid = -1
    info_thread.rid = rid
    info_thread.add = False
    info_thread.cache = []
    return info_thread


# ---------------------------------------------------------------------------
# identity
# ---------------------------------------------------------------------------

def test_result_id_is_stable_for_the_job_and_unique(manager):
    rid = manager.create_result_thread([("u", "X")])
    job = manager.result_jobs[rid]

    assert rid >= 0
    assert job.rid == rid

    rid_other = manager.create_result_thread([("u", "X")])
    assert rid_other != rid


def test_add_mode_creates_no_pollable_job(manager):
    rid = manager.create_result_thread([("u", "X")], add=True)

    assert rid not in manager.result_jobs
    assert FakeInfoThread.instances[-1].add is True


def test_result_ids_are_not_reused_across_process_restart(manager):
    rid = manager.create_result_thread([("u", "X")])

    restarted = ThreadManager(FakeCore())

    # the old id is explicitly unknown instead of silently returning data
    snapshot = restarted.poll_result(rid)
    assert snapshot.rid == -1
    assert snapshot.data == {}
    assert snapshot.status is None

    snapshot = restarted.poll_result(rid, 0)
    assert snapshot.rid == -1
    assert snapshot.status == OnlineCheckStatus.UNKNOWN.value
    assert snapshot.progress == 0
    assert snapshot.next_progress == 0

    # a fresh job on the restarted manager can never reuse the old identity
    new_rid = restarted._alloc_result_id()
    assert new_rid != rid


def test_online_check_legacy_wire_format_is_unchanged():
    status = make_status("u", 3)
    assert dict(OnlineCheck(rid=3, data={"u": status})) == {"rid": 3, "data": {"u": status}}

    check = OnlineCheck(
        rid=-1, data={}, progress=0, next_progress=1,
        status="running", errors=None,
    )
    serialized = dict(check)
    assert serialized["progress"] == 0
    assert serialized["next_progress"] == 1
    assert serialized["status"] == "running"
    assert "errors" not in serialized


# ---------------------------------------------------------------------------
# replayable polling
# ---------------------------------------------------------------------------

def test_same_progress_replays_same_data_and_lost_reply_is_recovered(manager):
    rid = manager.create_result_thread([])
    append_urls(manager, rid, ["u1"])

    first = manager.poll_result(rid, 0)
    repeated = manager.poll_result(rid, 0)

    assert first.rid == rid
    assert first.status == "running"
    assert first.next_progress == 1
    assert set(first.data) == {"u1"}
    assert repeated.data == first.data
    assert repeated.next_progress == first.next_progress

    # a new batch arrives meanwhile: re-requesting the old cursor still
    # returns everything, so a lost response can never drop a batch
    append_urls(manager, rid, ["u2"])
    recovered = manager.poll_result(rid, 0)
    assert set(recovered.data) == {"u1", "u2"}
    assert recovered.next_progress == 2

    assert set(manager.poll_result(rid, 1).data) == {"u2"}

    waiting = manager.poll_result(rid, 2)
    assert waiting.rid == rid
    assert waiting.status == "running"
    assert waiting.data == {}
    assert waiting.next_progress == 2


def test_multiple_consumers_advance_without_stealing_results(manager):
    rid = manager.create_result_thread([])
    append_urls(manager, rid, ["u1", "u2"])

    page_a = manager.poll_result(rid, 0)
    page_b = manager.poll_result(rid, 0)

    assert set(page_a.data) == {"u1", "u2"}
    assert set(page_b.data) == {"u1", "u2"}

    manager.finalize_result_job(rid)

    # consumer A is done; consumer B may still read the final batch
    done_a = manager.poll_result(rid, 2)
    assert done_a.rid == -1
    assert done_a.status == "complete"

    final_b = manager.poll_result(rid, 0)
    assert final_b.rid == -1
    assert set(final_b.data) == {"u1", "u2"}
    assert final_b.status == "complete"

    assert manager.poll_result(rid, 2).status == "complete"


def test_last_batch_and_completion_are_visible_together_and_in_order(manager):
    rid = manager.create_result_thread([])
    append_urls(manager, rid, ["u1"])

    assert manager.poll_result(rid, 0).rid == rid

    manager.finalize_result_job(rid)

    final = manager.poll_result(rid, 0)
    assert final.rid == -1
    assert final.status == "complete"
    assert list(final.data) == ["u1"]
    assert final.next_progress == 1

    # the final reply is replayable at the same cursor
    replayed = manager.poll_result(rid, 0)
    assert replayed.data == final.data
    assert replayed.status == "complete"

    assert manager.poll_result(rid, 1).data == {}


def test_legacy_path_keeps_single_consumer_semantics(manager):
    rid = manager.create_result_thread([])

    assert manager.poll_result(rid).rid == rid
    assert manager.poll_result(rid).data == {}

    append_urls(manager, rid, ["u1"])
    first = manager.poll_result(rid)
    assert first.rid == rid
    assert first.status is None
    assert set(first.data) == {"u1"}

    assert manager.poll_result(rid).data == {}

    append_urls(manager, rid, ["u2"])
    manager.finalize_result_job(rid)
    last = manager.poll_result(rid)
    assert last.rid == -1
    assert last.status is None
    assert set(last.data) == {"u2"}

    assert manager.poll_result(rid).rid == -1


def test_legacy_and_cursor_consumers_do_not_steal_from_each_other(manager):
    rid = manager.create_result_thread([])
    append_urls(manager, rid, ["u1"])

    assert set(manager.poll_result(rid).data) == {"u1"}
    # cursor consumer still gets the full batch log despite legacy advance
    assert set(manager.poll_result(rid, 0).data) == {"u1"}

    append_urls(manager, rid, ["u2"])
    manager.finalize_result_job(rid)

    assert set(manager.poll_result(rid).data) == {"u2"}
    assert manager.poll_result(rid).rid == -1

    cursor = manager.poll_result(rid, 0)
    assert set(cursor.data) == {"u1", "u2"}
    assert cursor.status == "complete"


def test_unknown_rid_is_an_explicit_terminal_state(manager):
    legacy = manager.poll_result(999_999_999_999)
    assert legacy.rid == -1
    assert legacy.data == {}
    assert legacy.status is None

    cursor = manager.poll_result(999_999_999_999, 4)
    assert cursor.rid == -1
    assert cursor.status == "unknown"
    assert cursor.progress == 4
    assert cursor.next_progress == 4


# ---------------------------------------------------------------------------
# terminal states
# ---------------------------------------------------------------------------

def test_plugin_error_is_a_distinguishable_terminal_state(manager):
    rid = manager.create_result_thread([])
    # default unknown results are still delivered, exactly like before
    append_urls(manager, rid, ["u1"], status=3)
    manager.mark_result_failure(rid, OnlineCheckStatus.PLUGIN_ERROR)
    manager.finalize_result_job(rid)

    snapshot = manager.poll_result(rid, 0)
    assert snapshot.rid == -1
    assert snapshot.status == "plugin_error"
    assert snapshot.errors == ["plugin_error"]
    assert snapshot.data["u1"].status == 3


def test_container_error_is_a_distinguishable_terminal_state(manager):
    rid = manager.create_result_thread([])
    manager.mark_result_failure(rid, OnlineCheckStatus.CONTAINER_ERROR)
    manager.finalize_result_job(rid)

    snapshot = manager.poll_result(rid, 0)
    assert snapshot.rid == -1
    assert snapshot.status == "container_error"
    assert snapshot.errors == ["container_error"]


def test_both_failure_kinds_are_reported_in_canonical_order(manager):
    rid = manager.create_result_thread([])
    manager.mark_result_failure(rid, OnlineCheckStatus.PLUGIN_ERROR)
    manager.mark_result_failure(rid, OnlineCheckStatus.CONTAINER_ERROR)
    manager.finalize_result_job(rid)

    snapshot = manager.poll_result(rid, 0)
    assert snapshot.status == "container_error"
    assert snapshot.errors == ["container_error", "plugin_error"]


# ---------------------------------------------------------------------------
# per-job lifetime / expiration
# ---------------------------------------------------------------------------

def test_expired_job_is_observable_then_becomes_unknown(manager):
    rid = manager.create_result_thread([])
    append_urls(manager, rid, ["u1"])
    job = manager.result_jobs[rid]
    backdate(job, RESULT_JOB_TTL + 1)

    manager.purge_result_jobs()
    assert job.terminal == OnlineCheckStatus.EXPIRED

    # unread data is still delivered together with the expired terminal state
    expired = manager.poll_result(rid, 0)
    assert expired.rid == -1
    assert expired.status == "expired"
    assert set(expired.data) == {"u1"}

    activity_before = job.last_activity
    assert manager.poll_result(rid, 0).status == "expired"
    assert job.last_activity == activity_before  # expired tombstone is frozen

    assert manager.poll_result(rid).rid == -1  # legacy sees a terminal as well

    backdate(job, RESULT_JOB_TOMBSTONE_TTL + 1, attribute="terminal_at")
    manager.purge_result_jobs()
    assert rid not in manager.result_jobs

    assert manager.poll_result(rid, 0).status == "unknown"


def test_idle_cleanup_leaves_producing_and_read_jobs_untouched(manager):
    idle_rid = manager.create_result_thread([])
    active_rid = manager.create_result_thread([])
    append_urls(manager, active_rid, ["u1"])

    backdate(manager.result_jobs[idle_rid], RESULT_JOB_TTL + 1)
    manager.purge_result_jobs()

    assert manager.result_jobs[idle_rid].terminal == OnlineCheckStatus.EXPIRED
    assert manager.result_jobs[active_rid].terminal is None
    assert manager.poll_result(active_rid, 0).status == "running"

    # a still-active producer keeps an otherwise idle job alive
    producer_rid = manager.create_result_thread([])
    backdate(manager.result_jobs[producer_rid], RESULT_JOB_TTL - 10)
    manager.touch_result_job(producer_rid)
    manager.purge_result_jobs()
    assert manager.result_jobs[producer_rid].terminal is None

    # a finished job that is still read is not garbage collected
    finished_rid = manager.create_result_thread([])
    append_urls(manager, finished_rid, ["u"])
    manager.finalize_result_job(finished_rid)
    job = manager.result_jobs[finished_rid]
    job.terminal_at = time.monotonic() - RESULT_JOB_TTL
    job.last_activity = time.monotonic()  # polled just now
    manager.purge_result_jobs()
    assert finished_rid in manager.result_jobs
    assert manager.poll_result(finished_rid, 0).status == "complete"


def test_appends_and_finalization_after_expiry_are_ignored(manager):
    rid = manager.create_result_thread([])
    job = manager.result_jobs[rid]
    backdate(job, RESULT_JOB_TTL + 1)
    manager.purge_result_jobs()

    append_urls(manager, rid, ["u1"])
    manager.finalize_result_job(rid)

    assert job.batches == []
    assert job.terminal == OnlineCheckStatus.EXPIRED


def test_purge_removes_finished_job_after_idle_tombstone_interval(manager):
    rid = manager.create_result_thread([])
    append_urls(manager, rid, ["u"])
    manager.finalize_result_job(rid)

    job = manager.result_jobs[rid]
    backdate(job, RESULT_JOB_TTL + RESULT_JOB_TOMBSTONE_TTL + 1)
    manager.purge_result_jobs()

    assert rid not in manager.result_jobs


def test_info_cache_deadline_is_independent_from_result_jobs(manager):
    # purging jobs never touches the shared hoster info cache
    manager.info_cache["url"] = ("name", 1, 2, "url")
    manager._refresh_info_cache_deadline()
    manager.purge_result_jobs()
    assert manager.info_cache == {"url": ("name", 1, 2, "url")}

    # an expired cache is purged even when no result jobs exist
    manager.info_cache_deadline = time.monotonic() - 1
    manager.run()
    assert manager.info_cache == {}


# ---------------------------------------------------------------------------
# InfoThread orchestration against the real manager
# ---------------------------------------------------------------------------

def test_info_thread_delivers_default_results_and_completes(manager):
    rid = manager.create_result_thread([])
    info_thread = make_real_info_thread(manager, [("http://host/f.zip", "DefaultPlugin")], rid)

    info_thread.run()

    snapshot = manager.poll_result(rid, 0)
    assert snapshot.rid == -1
    assert snapshot.status == "complete"
    assert snapshot.errors is None
    assert list(snapshot.data) == ["http://host/f.zip"]
    assert snapshot.data["http://host/f.zip"].status == 3


def test_info_thread_marks_plugin_error_but_keeps_default_unknown_results(manager):
    rid = manager.create_result_thread([])

    # plugin object with a raising get_info
    manager.pyload.plugin_manager.get_plugin = lambda name, original=False: BadInfoPlugin()
    info_thread = make_real_info_thread(manager, [("http://host/f.zip", "BrokenHoster")], rid)

    info_thread.run()

    snapshot = manager.poll_result(rid, 0)
    assert snapshot.rid == -1
    assert snapshot.status == "plugin_error"
    assert snapshot.errors == ["plugin_error"]
    assert snapshot.data["http://host/f.zip"].status == 3


def test_info_thread_successful_plugin_get_info(manager):
    rid = manager.create_result_thread([])
    plugin = GoodInfoPlugin([("f.zip", 100, 2, "http://host/f.zip")])
    manager.pyload.plugin_manager.get_plugin = lambda name, original=False: plugin
    info_thread = make_real_info_thread(manager, [("http://host/f.zip", "GoodHoster")], rid)

    info_thread.run()

    snapshot = manager.poll_result(rid, 0)
    assert snapshot.status == "complete"
    assert snapshot.data["http://host/f.zip"].status == 2
    assert snapshot.data["http://host/f.zip"].size == 100


def test_info_thread_marks_container_decrypt_failure(manager):
    manager.pyload.plugin_manager = FakePluginManager(container_plugins=("DLC",))
    rid = manager.create_result_thread([])
    info_thread = make_real_info_thread(manager, [("http://host/container.dlc", "DLC")], rid)

    def fail_decrypt(plugin, url):
        raise ValueError("cannot decrypt")

    info_thread.decrypt_container = fail_decrypt
    info_thread.run()

    snapshot = manager.poll_result(rid, 0)
    assert snapshot.rid == -1
    assert snapshot.status == "container_error"
    assert snapshot.data == {}


def test_info_thread_finalizes_even_when_an_unexpected_error_escapes(manager):
    rid = manager.create_result_thread([])
    manager.pyload.plugin_manager = FakePluginManager(bad_plugin=True)
    info_thread = make_real_info_thread(manager, [("http://host/f.zip", "X")], rid)

    info_thread.run()  # get_plugin raises outside fetch_for_plugin's guard

    snapshot = manager.poll_result(rid, 0)
    assert snapshot.rid == -1
    assert snapshot.status == "plugin_error"
