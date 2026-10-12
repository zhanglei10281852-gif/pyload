import secrets
import time
from threading import RLock

from ..datatypes.exceptions import (
    CaptchaResultInvalid,
    CaptchaTaskExpired,
    CaptchaTaskNotFound,
    CaptchaTaskOwnership,
    CaptchaTaskSolved,
)
from ..utils.struct.lock import lock

#: Seconds an exclusive acquirer owns a task without renewing the lease
#: before it is released and offered again to the next solver.
LEASE_SECONDS = 30

#: Seconds a terminal task ("solved"/"expired") is kept around after the
#: consumer left it, so status queries and idempotent duplicate submissions
#: keep working. Once purged, every old credential is invalid.
RETAIN_SECONDS = 60

#: Winner marker for results produced by internal producers (addon handlers,
#: chat bots); they never hold an exclusive credential.
INTERNAL_WINNER = "\x00internal"


class CaptchaManager:
    def __init__(self, core):
        # reentrant: locked API methods call other locked helpers
        self.lock = RLock()
        self.pyload = core
        self._ = core._
        self.tasks = []  #: Task store, for outgoing tasks only

        self.ids = 0  #: Only for internal purpose

    @lock
    def new_task(self, captcha_format, params, result_type):
        task = CaptchaTask(self.ids, captcha_format, params, result_type)
        self.ids += 1
        return task

    @lock
    def remove_task(self, task):
        """
        Hard-delete a task. Every credential bound to it dies immediately.
        """
        if task in self.tasks:
            self.tasks.remove(task)

    @lock
    def finish_task(self, task, aborted=False):
        """
        Called by the consuming download thread once it stops waiting.

        Aborted or failed tasks are removed at once; solved or expired tasks
        are retained briefly so clients can still observe the terminal state
        (and resubmit the winning answer idempotently) until cleanup purges
        them and invalidates the old credentials.
        """
        if task not in self.tasks:
            return

        now = time.monotonic()
        self._purge(now)

        if task not in self.tasks:
            return

        if aborted or task.error:
            self.tasks.remove(task)
            return

        self._refresh_task(task, now)

        if task.status == "solved" or task.result is not None:
            task.mark_solved()
            task.retain_until = max(task.retain_until, now + RETAIN_SECONDS)

        elif task.timed_out():
            task.mark_expired()
            task.retain_until = now + RETAIN_SECONDS

        else:
            self.tasks.remove(task)

    @lock
    def get_task(self, exclusive=False):
        """
        Return an available task.

        :param exclusive: atomically claim the task for the caller only;
            a fresh task generation with a short-lived credential and lease
            is created, every other exclusive request skips the task until
            the lease is renewed or lapses.
        :return: the claimed/acquired `CaptchaTask`, `None` otherwise.
        """
        now = time.monotonic()
        self._purge(now)

        for task in self.tasks:
            self._refresh_task(task, now)

            if task.status not in ("waiting", "shared-user"):
                continue  #: leased, solved, expired or otherwise unavailable

            if exclusive:
                task.claim(secrets.token_urlsafe(24), LEASE_SECONDS, now)

            return task

        return None

    @lock
    def get_task_by_id(self, tid):
        now = time.monotonic()
        self._purge(now)

        for task in self.tasks:
            if task.id == str(tid):  #: Task ids are strings
                self._refresh_task(task, now)
                return task

        return None

    @lock
    def renew_lease(self, tid, token):
        """
        Heartbeat for an exclusive holder; extends the lease while the
        credential still matches the current task generation.

        :return: the current task status
        """
        task = self.get_task_by_id(tid)
        if task is None:
            raise CaptchaTaskNotFound(self._("Captcha task does not exist"))

        if task.status == "solved":
            return task.status

        if task.status == "expired" or task.timed_out():
            task.mark_expired()
            task.retain_until = time.monotonic() + RETAIN_SECONDS
            raise CaptchaTaskExpired(self._("Captcha task has expired"))

        if task.status != "user" or not token or token != task.owner:
            raise CaptchaTaskOwnership(
                self._("Captcha task is not owned by this credential")
            )

        task.lease_until = time.monotonic() + LEASE_SECONDS
        return task.status

    @lock
    def submit_result(self, tid, result, token=None):
        """
        Accept a captcha result, validating task generation, ownership and
        deadline atomically. Only the first legal result wins; the exact
        same submission from the winner is accepted idempotently, every
        other late submission is explicitly rejected.

        :return: the accepted `CaptchaTask`
        """
        now = time.monotonic()
        self._purge(now)

        task = None
        for candidate in self.tasks:
            if candidate.id == str(tid):
                task = candidate
                break

        if task is None:
            raise CaptchaTaskNotFound(self._("Captcha task does not exist"))

        self._refresh_task(task, now)

        if task.status == "solved":
            if token == task.winner and task.equal_result(result):
                return task  #: same legal duplicate submission -> same outcome

            raise CaptchaTaskSolved(
                self._("Captcha task was already solved and cannot be overwritten")
            )

        if task.status == "expired" or task.timed_out():
            task.mark_expired()
            task.retain_until = now + RETAIN_SECONDS
            raise CaptchaTaskExpired(self._("Captcha task has expired"))

        if task.status == "user":
            if not token or token != task.owner:
                raise CaptchaTaskOwnership(
                    self._("Captcha task is not owned by this credential")
                )

        elif token is not None:
            #: a credential for a lease that was lost meanwhile
            raise CaptchaTaskOwnership(
                self._("Captcha task lease was lost")
            )

        normalized = task.normalize_result(result, task.captcha_result_type)
        if normalized is None:
            raise CaptchaResultInvalid(
                self._("Invalid captcha result for this task type")
            )

        if not task.store_result(normalized, winner=token):
            #: an internal handler (addon/chat bot) won the race concurrently
            raise CaptchaTaskSolved(
                self._("Captcha task was already solved and cannot be overwritten")
            )

        task.retain_until = now + RETAIN_SECONDS
        return task

    def handle_captcha(self, task, timeout):
        cli = self.pyload.is_client_connected()

        task.set_waiting(timeout)

        # if cli:  #: Client connected -> should solve the captcha
        #     task.set_waiting(50)  #: Wait minimum 50 sec for response

        for plugin in self.pyload.addon_manager.active_plugins():
            try:
                plugin.captcha_task(task)
            except Exception:
                self.pyload.log.warning(
                    self.pyload._("Unable to create captcha task"),
                    exc_info=self.pyload.debug > 1,
                    stack_info=self.pyload.debug > 2
                )

        if task.handler or cli:  #: The captcha was handled
            self.tasks.append(task)
            return True

        task.error = self._("No Client connected for captcha decrypting")

        return False

    def _refresh_task(self, task, now):
        """
        Advance the task lifecycle based on the current clock: an exclusive
        holder that disappeared or stopped renewing releases the task, which
        reopens as long as the overall deadline has not been reached; past
        the deadline the task expires and its credentials die.
        """
        if task.status == "user" and now > task.lease_until:
            task.reopen()

        if (
            task.status in ("waiting", "shared-user", "user")
            and task.result is None
            and not task.error
            and task.wait_until
            and now > task.wait_until
        ):
            task.mark_expired()
            task.retain_until = now + RETAIN_SECONDS

    def _purge(self, now):
        """Remove terminal tasks whose retention window elapsed."""
        for task in [t for t in self.tasks
                     if t.status in ("solved", "expired") and now > t.retain_until]:
            self.tasks.remove(task)


class CaptchaTask:
    def __init__(self, id, captcha_format, captcha_params=None, result_type="textual"):
        self.id = str(id)
        self.captcha_params = captcha_params or {}
        self.captcha_format = captcha_format
        self.captcha_result_type = result_type
        self.handler = []  #: the addon plugins that will take care of the solution
        self.result = None
        self.wait_until = 0
        self.error = None  #: error message

        self.status = "init"
        self.data = {}  #: handler can store data here

        #: exclusive ownership state
        self.generation = 0  #: bumped on every exclusive (re)acquisition
        self.owner = None  #: credential of the current exclusive holder
        self.lease_until = 0  #: monotonic deadline of the current lease
        self.winner = None  #: credential that produced the accepted result
        self.retain_until = 0  #: monotonic deadline for terminal retention

        self._lock = RLock()

    def get_captcha(self):
        return self.captcha_params, self.captcha_format, self.captcha_result_type

    @staticmethod
    def normalize_result(result, result_type):
        """
        Coerce a raw submission into the task result type; return None when
        the submission is not a legal answer.
        """
        if result_type == "positional":
            try:
                parts = str(result).split(",")
                return int(parts[0]), int(parts[1])

            except Exception:
                return None

        if result is None:
            return None

        if isinstance(result, str) and not result.strip():
            return None

        return result

    def equal_result(self, result):
        try:
            return self.normalize_result(
                result, self.captcha_result_type
            ) == self.result

        except Exception:
            return False

    def store_result(self, result, winner):
        """
        Atomic compare-and-set of the result. Only the first legal answer
        is stored, no later submission can remove or rewrite it.
        """
        with self._lock:
            if self.result is not None or self.error or self.timed_out():
                return False

            self.result = result
            self.winner = winner
            self.status = "solved"
            self.retain_until = time.monotonic() + RETAIN_SECONDS
            return True

    def set_result(self, result):
        """
        Entry point for internal solvers (addon handlers, chat bots).
        First legal answer wins; failed/None answers do not terminate the
        task and late answers can never overwrite an accepted one.
        """
        normalized = self.normalize_result(result, self.captcha_result_type)
        if normalized is None:
            return False

        return self.store_result(normalized, INTERNAL_WINNER)

    def get_result(self):
        return self.result

    def get_status(self):
        return self.status

    def set_waiting(self, sec):
        """
        let the captcha wait secs for the solution.
        """
        self.wait_until = max(time.monotonic() + sec, self.wait_until)
        if self.status != "user":
            self.status = "waiting"

    def is_waiting(self):
        if self.result or self.error or time.monotonic() > self.wait_until:
            return False

        return True

    def is_textual(self):
        """
        returns if text is written on the captcha.
        """
        return self.captcha_result_type == "textual"

    def is_positional(self):
        """
        returns if user have to click a specific region on the captcha.
        """
        return self.captcha_result_type == "positional"

    def is_interactive(self):
        """
        returns if user have to solve the captcha in an interactive iframe.
        """
        return self.captcha_result_type == "interactive"

    def is_invisible(self):
        """
        returns if invisible (browser only, no user interaction) captcha.
        """
        return self.captcha_result_type == "invisible"

    def claim(self, token, lease_seconds, now=None):
        """
        Atomically bind a new task generation to the exclusive holder:
        a fresh credential, a short lease and the "user" status.
        """
        now = now if now is not None else time.monotonic()
        with self._lock:
            self.generation += 1
            self.owner = token
            self.lease_until = now + lease_seconds
            self.status = "user"

    def reopen(self):
        """
        Release an expired exclusive lease; the task is open again for the
        next solver, but the overall deadline stays untouched.
        """
        with self._lock:
            self.owner = None
            self.lease_until = 0
            if self.status == "user":
                self.status = "waiting"

    def lease_valid(self):
        return bool(self.owner) and time.monotonic() <= self.lease_until

    def mark_solved(self):
        with self._lock:
            self.status = "solved"

    def mark_expired(self):
        """
        Overall deadline reached: invalidate every outstanding credential.
        """
        with self._lock:
            self.owner = None
            self.lease_until = 0
            self.status = "expired"
            self.retain_until = time.monotonic() + RETAIN_SECONDS

    def set_waiting_for_user(self, exclusive):
        if exclusive:
            self.status = "user"
        else:
            self.status = "shared-user"

    def timed_out(self):
        return time.monotonic() > self.wait_until

    def invalid(self):
        """
        indicates the captcha was not correct.
        """
        [x.captcha_invalid(self) for x in self.handler]

    def correct(self):
        [x.captcha_correct(self) for x in self.handler]

    def __str__(self):
        return f"<CaptchaTask '{self.id}'>"
