import os
import secrets
import subprocess
import time
from datetime import timedelta
from threading import Event, RLock

from ..datatypes.data import OnlineCheckStatus
from ..datatypes.pyfile import PyFile
from ..threads.decrypter_thread import DecrypterThread
from ..threads.download_thread import DownloadThread
from ..threads.info_thread import InfoThread
from ..utils import fs
from ..utils.struct.lock import lock
from ..utils.web.check import get_public_address

# import pycurl


#: idle interval (seconds) after which an online-check job expires
RESULT_JOB_TTL = timedelta(minutes=5).total_seconds()
#: interval (seconds) an expired/finished job id stays resolvable before it becomes unknown
RESULT_JOB_TOMBSTONE_TTL = timedelta(minutes=5).total_seconds()
#: idle interval (seconds) after which the shared url info cache is purged
INFO_CACHE_TTL = timedelta(minutes=5).total_seconds()


class ResultJob:
    """
    State of a single `checkOnlineStatus` invocation.

    The result id is allocated once when the job is created and is never
    reused within the same process. Result batches are appended by the
    producing InfoThread and stay available for replayable polling until
    the job expires independently of every other job.
    """

    __slots__ = (
        "rid", "batches", "legacy_cursor", "failures", "terminal",
        "last_activity", "terminal_at",
    )

    def __init__(self, rid):
        self.rid = rid
        self.batches = []  #: append-only batches, each a {url: OnlineStatus} dict
        self.legacy_cursor = 0  #: batches already delivered on the legacy poll path
        self.failures = []  #: OnlineCheckStatus values recorded by the producer
        self.terminal = None  #: None while running, final OnlineCheckStatus otherwise
        self.last_activity = time.monotonic()
        self.terminal_at = 0.0

    def touch(self):
        self.last_activity = time.monotonic()

    @property
    def expires_at(self):
        return self.last_activity + RESULT_JOB_TTL


class ResultSnapshot:
    """
    Immutable view returned by a result poll; converted to OnlineCheck by the API.
    """

    __slots__ = ("rid", "data", "progress", "next_progress", "status", "errors")

    def __init__(self, rid, data, progress=-1, next_progress=-1, status=None, errors=None):
        self.rid = rid
        self.data = data
        self.progress = progress
        self.next_progress = next_progress
        self.status = status
        self.errors = errors


class ThreadManager:
    """
    manages the download threads, assign jobs, reconnect etc.
    """

    def __init__(self, core):
        """
        Constructor.
        """
        self.pyload = core
        self._ = core._

        self.threads = []  #: thread list
        self.local_threads = []  #: addon+decrypter threads

        self.pause = True

        self.reconnecting = Event()
        self.reconnecting.clear()
        self.downloaded = 0  #: number of files downloaded since last cleanup

        self.lock = RLock()

        # some operations require to fetch url info from hoster, so we caching them so it wont be done twice
        # contains a timestamp and will be purged after timeout
        self.info_cache = {}
        # idle deadline for the shared info cache, independent from result jobs
        self.info_cache_deadline = 0.0

        # online check jobs keyed by their unique, non-reusable result id
        self.result_jobs = {}

        # result ids are namespaced by a random per-process epoch, so ids
        # issued before a restart can never collide with a new job or leak data
        self._result_epoch = secrets.randbits(32)
        self._result_seq = 0

        # pycurl.global_init(pycurl.GLOBAL_DEFAULT)

        for i in range(self.pyload.config.get("download", "max_downloads")):
            self.create_download_thread()

    def create_download_thread(self):
        """
        create a download thread.
        """
        thread = DownloadThread(self)
        self.threads.append(thread)

    def _refresh_info_cache_deadline(self):
        self.info_cache_deadline = time.monotonic() + INFO_CACHE_TTL

    @lock
    def refresh_info_cache(self):
        """
        keeps the shared url info cache alive while info threads are fetching.
        """
        self._refresh_info_cache_deadline()

    def create_info_thread(self, data, pid):
        """
        start a thread which fetches online status and other infos
        data = [ .. () .. ]
        """
        self._refresh_info_cache_deadline()

        InfoThread(self, data, pid)

    @lock
    def create_result_thread(self, data, add=False):
        """
        creates a thread to fetch online status, returns a stable, unique
        (and per process non-reusable) result id.
        """
        self._refresh_info_cache_deadline()

        rid = self._alloc_result_id()

        # add=True writes results straight to the database, nothing is polled
        if not add:
            self.result_jobs[rid] = ResultJob(rid)

        InfoThread(self, data, rid=rid, add=add)

        return rid

    def _alloc_result_id(self):
        # caller must hold the lock
        rid = (self._result_epoch << 32) | (self._result_seq & 0xFFFFFFFF)
        self._result_seq += 1
        return rid

    @lock
    def touch_result_job(self, rid):
        """
        marks producer activity on a result job, so a slow fetch keeps the job
        alive even while no result batch is being delivered.
        """
        job = self.result_jobs.get(rid)
        if job is not None and job.terminal is None:
            job.touch()

    @lock
    def append_result_batch(self, rid, batch):
        """
        appends an immutable result batch to the job. Batches are retained
        until the job expires; polling never removes them.
        """
        job = self.result_jobs.get(rid)
        if job is not None and job.terminal is None and batch:
            job.batches.append(dict(batch))
            job.touch()

    @lock
    def mark_result_failure(self, rid, failure):
        """
        records a distinguishable producer failure for the final job state
        (e.g. OnlineCheckStatus.PLUGIN_ERROR / CONTAINER_ERROR).
        """
        job = self.result_jobs.get(rid)
        if job is not None and job.terminal is None and failure not in job.failures:
            job.failures.append(failure)
            job.touch()

    @lock
    def finalize_result_job(self, rid):
        """
        marks the end of production; the final state is derived from the
        recorded failures. Batches appended afterwards are not visible, and
        the final batch therefore always precedes the terminal state.
        """
        job = self.result_jobs.get(rid)
        if job is not None and job.terminal is None:
            order = (
                OnlineCheckStatus.CONTAINER_ERROR,
                OnlineCheckStatus.PLUGIN_ERROR,
            )
            job.failures = [failure for failure in order if failure in job.failures]
            job.touch()
            job.terminal = job.failures[0] if job.failures else OnlineCheckStatus.COMPLETE
            job.terminal_at = time.monotonic()

    @lock
    def poll_result(self, rid, progress=-1):
        """
        returns a ResultSnapshot for a result job.

        progress < 0 (default): legacy single-consumer path returning the
        batches accumulated since the previous legacy call, with rid=-1 once
        the job reached a terminal state.

        progress >= 0: replayable path returning all batches starting at the
        given cursor without removing them. Repeating the same progress
        replays the same data, so a lost reply is recovered simply by polling
        again; multiple consumers advance independently using own cursors.
        """
        now = time.monotonic()
        job = self.result_jobs.get(rid)

        if job is None:
            # the id never existed, was garbage collected or was issued by a
            # previous process run: report an explicit terminal state instead
            # of an ambiguous "no new data" reply
            if progress < 0:
                return ResultSnapshot(rid=-1, data={})
            return ResultSnapshot(
                rid=-1, data={}, progress=progress, next_progress=progress,
                status=OnlineCheckStatus.UNKNOWN.value,
            )

        # lazily terminalize a job whose producer has been idle past the TTL
        if job.terminal is None and now > job.expires_at:
            job.terminal = OnlineCheckStatus.EXPIRED
            job.terminal_at = now

        total = len(job.batches)
        legacy = progress < 0
        start = job.legacy_cursor if legacy else min(max(progress, 0), total)

        data = {}
        for batch in job.batches[start:total]:
            data.update(batch)

        if legacy:
            job.legacy_cursor = total

        # reading keeps the job alive (replays, late consumers); an expired
        # tombstone stays frozen until it is garbage collected
        if job.terminal != OnlineCheckStatus.EXPIRED:
            job.touch()

        terminal = job.terminal is not None
        if legacy:
            return ResultSnapshot(rid=-1 if terminal else rid, data=data)

        return ResultSnapshot(
            rid=-1 if terminal else rid,
            data=data,
            progress=progress,
            next_progress=total,
            status=(job.terminal.value if terminal else OnlineCheckStatus.RUNNING.value),
            errors=[failure.value for failure in job.failures] or None,
        )

    @lock
    def purge_result_jobs(self):
        """
        expires idle jobs on their own timers and removes expired/finished
        jobs only after a tombstone interval; actively producing or read
        jobs are never touched.
        """
        now = time.monotonic()

        for rid in tuple(self.result_jobs):
            job = self.result_jobs[rid]

            if job.terminal is None:
                if now > job.expires_at:
                    # the producer vanished (e.g. crashed) without finalizing
                    job.terminal = OnlineCheckStatus.EXPIRED
                    job.terminal_at = now

            elif job.terminal == OnlineCheckStatus.EXPIRED:
                if now > job.terminal_at + RESULT_JOB_TOMBSTONE_TTL:
                    del self.result_jobs[rid]

            elif now > job.expires_at + RESULT_JOB_TOMBSTONE_TTL:
                # finished jobs remain replayable while they are read, then
                # are garbage collected one tombstone interval after going idle
                del self.result_jobs[rid]

    def get_active_files(self):
        active = [
            x.active for x in self.threads if x.active and isinstance(x.active, PyFile)
        ]

        for t in self.local_threads:
            active.extend(t.get_active_files())

        return active

    def processing_ids(self):
        """
        get an id list of all pyfiles processed.
        """
        return [x.id for x in self.get_active_files()]

    def run(self):
        """
        run all task which have to be done (this is for repetivive call by core)
        """
        try:
            self.try_reconnect()
        except Exception as exc:
            self.pyload.log.error(
                self._("Reconnect Failed: {}").format(exc),
                exc_info=self.pyload.debug > 1,
                stack_info=self.pyload.debug > 2,
            )
            self.reconnecting.clear()

        try:
            self.assign_job()
        except Exception:
            self.pyload.log.warning(
                "Assign job error",
                exc_info=self.pyload.debug > 1,
                stack_info=self.pyload.debug > 2,
            )

            time.sleep(0.5)
            # it may be failed non-critical so we try it again
            self.assign_job()

        if self.info_cache and self.info_cache_deadline < time.monotonic():
            self.info_cache.clear()
            self.pyload.log.debug("Cleared info cache")

        if self.result_jobs:
            self.purge_result_jobs()

    # ----------------------------------------------------------------------
    def try_reconnect(self):
        """
        checks if reconnect needed.
        """
        if not (
            self.pyload.config.get("reconnect", "enabled")
            and self.pyload.api.is_time_reconnect()
        ):
            return False

        active = [
            x.active.plugin.want_reconnect and x.active.plugin.waiting
            for x in self.threads
            if x.active
        ]

        if not (0 < active.count(True) == len(active)):
            return False

        reconnect_script = self.pyload.config.get("reconnect", "script")
        if not os.path.isfile(reconnect_script):
            self.pyload.config.set("reconnect", "enabled", False)
            self.pyload.log.warning(self._("Reconnect script not found!"))
            return

        self.reconnecting.set()

        # Do reconnect
        self.pyload.log.info(self._("Starting reconnect"))

        while [x.active.plugin.waiting for x in self.threads if x.active].count(
            True
        ) != 0:
            time.sleep(0.25)

        old_ip = get_public_address()

        self.pyload.addon_manager.before_reconnect(old_ip)

        self.pyload.log.debug(f"Old IP: {old_ip}")

        try:
            subprocess.run(reconnect_script)
        except Exception:
            self.pyload.log.warning(self._("Failed executing reconnect script!"))
            self.pyload.config.set("reconnect", "enabled", False)
            self.reconnecting.clear()
            return

        time.sleep(1)
        new_ip = get_public_address()
        self.pyload.addon_manager.after_reconnect(new_ip, old_ip)

        self.pyload.log.info(self._("Reconnected, new IP: {}").format(new_ip))

        self.reconnecting.clear()

    def check_thread_count(self, extra=0):
        """
        checks if there are need for increasing or reducing thread count.
        """
        target_count = self.pyload.config.get("download", "max_downloads") + extra
        if len(self.threads) == target_count:
            # thread count matches, do nothing
            return
        elif len(self.threads) < target_count:
            # we need to increase thread count, add one thread
            self.create_download_thread()
        else:
            # we need to decrease thread count, remove one thread
            idle_threads = [x for x in self.threads if not x.active]
            if idle_threads:
                idle_threads[0].stop()

    # def clean_pycurl(self):
    # """
    # make a global curl cleanup (currently ununused)
    # """
    # if self.processing_ids():
    # return False
    # pycurl.global_cleanup()
    # pycurl.global_init(pycurl.GLOBAL_DEFAULT)
    # self.downloaded = 0
    # self.pyload.log.debug("Cleaned up pycurl")
    # return True

    # ----------------------------------------------------------------------
    def assign_job(self):
        """
        assign a job to a thread if possible.
        """
        if self.pause or not self.pyload.api.is_time_download():
            return

        # if self.downloaded > 20:
        #    if not self.clean_pycurl(): return

        inuse_plugin_threads = [
            x
            for x in self.threads
            if x.active and x.active.has_plugin()
        ]

        # (plugin_name, dl_limit, active_count)
        inuse_plugins = set(
            [
                (
                    x.active.pluginname,
                    self.get_limit(x),
                    len([y for y in self.threads if y.active and y.active.pluginname == x.active.pluginname]),
                )
                for x in inuse_plugin_threads
            ]
        )

        # plugins that are over their download limit (e.g., 3 active download for a plugin with limit 3)
        over_limit_plugins = [
            x[0] for x in inuse_plugins
            if x[2] >= x[1] > 0
        ]
        # plugins that are waiting for a very long time (e.g., waiting for download slot)
        long_waiting_plugins = [
            x.active.pluginname
            for x in inuse_plugin_threads
            if getattr(x.active.plugin, "long_waiting", False)
        ]
        # non-parallel plugins are those that do not support multiple simultaneous downloads
        non_parallel_plugins = [
            x.active.pluginname
            for x in inuse_plugin_threads
            if not getattr(x.active.plugin, "multi_dl", True)
        ]

        # long waiting plugins are not counted towards the download limit, so we add them as extra
        self.check_thread_count(extra=len(long_waiting_plugins))
        free_threads = [x for x in self.threads if not x.active]

        occupied_plugins = sorted(over_limit_plugins + long_waiting_plugins + non_parallel_plugins)
        occupied_plugins = tuple(set(occupied_plugins))  # remove duplicates
        job = self.pyload.files.get_job(occupied_plugins)
        if job:
            try:
                job.init_plugin()
            except Exception as exc:
                self.pyload.log.critical(
                    exc, exc_info=True, stack_info=self.pyload.debug > 2
                )
                job.set_status("failed")
                job.error = str(exc)
                job.release()
                return

            if job.plugin.__type__ == "downloader":
                space_left = (
                    fs.free_space(self.pyload.config.get("general", "storage_folder"))
                    >> 20
                )
                if space_left < self.pyload.config.get("general", "min_free_space"):
                    self.pyload.log.warning(self._("Not enough space left on device"))
                    self.pause = True

                if free_threads and not self.pause:
                    thread = free_threads[0]
                    # self.downloaded += 1

                    job.set_status("starting")
                    thread.put(job)
                else:
                    # put job back
                    if occupied_plugins not in self.pyload.files.job_cache:
                        self.pyload.files.job_cache[occupied_plugins] = []
                    self.pyload.files.job_cache[occupied_plugins].append(job.id)

                    # check for decrypt jobs
                    job = self.pyload.files.get_decrypt_job()
                    if job:
                        job.init_plugin()
                        thread = DecrypterThread(self, job)

            else:
                thread = DecrypterThread(self, job)

    def get_limit(self, thread):
        account_limit = 0
        account = thread.active.plugin.account
        if account:
            try:
                account_limit = max(
                    int(
                        account.get_account_data(account.user)["options"].get("limit_dl", ["0"])[0]
                    ),
                    0
                )
            except Exception as exc:
                self.pyload.log.error(
                    "Error occurred while fetching account limits: {}".format(exc),
                    exc_info=self.pyload.debug > 1,
                    stack_info=self.pyload.debug > 2
                )

        plugin_limit = (
            max(thread.active.plugin.limit_dl, 0)
            if hasattr(thread.active.plugin, "limit_dl")
            else 0
        )
        if account_limit > 0 and plugin_limit > 0:
            limit = min(account_limit, plugin_limit)
        else:
            limit = account_limit or plugin_limit

        return limit

    # def cleanup(self):
    # """
    # do global cleanup, should be called when finished with pycurl.
    # """
    # pycurl.global_cleanup()
