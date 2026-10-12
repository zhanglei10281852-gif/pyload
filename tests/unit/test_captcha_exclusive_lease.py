"""Tests for generation-bound exclusive captcha task acquisition and submission."""

import time
from threading import Barrier, Thread

import pytest

from pyload.core.api import Api
from pyload.core.datatypes.exceptions import (
    CaptchaResultInvalid,
    CaptchaTaskExpired,
    CaptchaTaskNotFound,
    CaptchaTaskOwnership,
    CaptchaTaskSolved,
)
from pyload.core.managers.captcha_manager import CaptchaManager


def make_manager():
    core = type("Core", (), {})()
    core._ = lambda value: value
    return CaptchaManager(core)


def make_task(manager, result_type="textual", timeout=120, status="waiting"):
    task = manager.new_task("jpg", {"src": "data:", "file": "x.jpg"}, result_type)
    task.set_waiting(timeout)
    task.status = status
    manager.tasks.append(task)
    return task


def expire_lease(task):
    task.lease_until = time.monotonic() - 1


def expire_total(task):
    task.wait_until = time.monotonic() - 1


def test_exclusive_claim_is_atomic_and_unique():
    manager = make_manager()
    task = make_task(manager)

    first = manager.get_task(exclusive=True)
    second = manager.get_task(exclusive=True)

    assert first is task
    assert second is None
    assert task.status == "user"
    assert task.owner
    assert task.generation == 1
    assert task.lease_valid()

    #: a shared (legacy) client cannot observe an exclusively leased task
    assert manager.get_task(exclusive=False) is None


def test_other_exclusive_client_gets_next_task_not_the_same_one():
    manager = make_manager()
    task_a = make_task(manager)
    task_b = make_task(manager)

    first = manager.get_task(exclusive=True)
    second = manager.get_task(exclusive=True)

    assert first is task_a
    assert second is task_b
    assert first.owner != second.owner
    assert first.generation == second.generation == 1  #: generations are per task


def test_dead_holder_releases_task_before_total_deadline():
    manager = make_manager()
    task = make_task(manager)

    claimed = manager.get_task(exclusive=True)
    old_token = claimed.owner
    expire_lease(claimed)  #: client vanished, stopped heartbeating

    reopened = manager.get_task_by_id(claimed.id)
    assert reopened.status == "waiting"
    assert reopened.owner is None
    assert reopened.is_waiting()  #: overall deadline still alive

    #: a new solver gets the same task, but a fresh generation/credential
    next_claim = manager.get_task(exclusive=True)
    assert next_claim is task
    assert next_claim.generation == 2
    assert next_claim.owner != old_token


def test_lapsed_lease_does_not_reopen_past_total_deadline():
    manager = make_manager()
    task = make_task(manager)
    claimed = manager.get_task(exclusive=True)
    old_token = claimed.owner
    expire_lease(claimed)
    expire_total(claimed)

    assert manager.get_task(exclusive=True) is None

    refreshed = manager.get_task_by_id(claimed.id)
    assert refreshed.status == "expired"
    assert refreshed.owner is None

    with pytest.raises(CaptchaTaskExpired):
        manager.submit_result(claimed.id, "late", old_token)


def test_renew_lease_extends_and_validates_credential():
    manager = make_manager()
    task = make_task(manager)
    claimed = manager.get_task(exclusive=True)
    token = claimed.owner

    assert manager.renew_lease(claimed.id, token) == "user"
    assert claimed.lease_valid()

    with pytest.raises(CaptchaTaskOwnership):
        manager.renew_lease(claimed.id, "forged-token")

    expire_lease(claimed)
    with pytest.raises(CaptchaTaskOwnership):
        manager.renew_lease(claimed.id, token)


def test_owner_submission_accepted_and_duplicates_consistent():
    manager = make_manager()
    task = make_task(manager)
    claimed = manager.get_task(exclusive=True)
    token = claimed.owner

    accepted = manager.submit_result(claimed.id, "ABCD", token)
    assert accepted is task
    assert task.status == "solved"
    assert task.result == "ABCD"
    assert task.winner == token
    assert not task.is_waiting()  #: download thread observes the terminal state

    #: the exact same legal duplicate submission yields the same outcome
    again = manager.submit_result(claimed.id, "ABCD", token)
    assert again is task
    assert task.result == "ABCD"

    #: same holder, different answer -> rejected, cannot rewrite
    with pytest.raises(CaptchaTaskSolved):
        manager.submit_result(claimed.id, "WXYZ", token)

    #: any other party -> rejected
    with pytest.raises(CaptchaTaskSolved):
        manager.submit_result(claimed.id, "ABCD", "somebody-else")

    with pytest.raises(CaptchaTaskSolved):
        manager.submit_result(claimed.id, "ABCD", None)


def test_stale_generation_credential_cannot_submit():
    manager = make_manager()
    task = make_task(manager)

    first_claim = manager.get_task(exclusive=True)
    first_token = first_claim.owner
    expire_lease(first_claim)

    #: task reopens and is claimed by a second generation owner
    second_claim = manager.get_task(exclusive=True)
    second_token = second_claim.owner
    assert second_claim.generation == 2

    #: old credential loses ownership while task is still open
    with pytest.raises(CaptchaTaskOwnership):
        manager.submit_result(task.id, "old-answer", first_token)

    #: no credential at all also fails on an exclusively leased task
    with pytest.raises(CaptchaTaskOwnership):
        manager.submit_result(task.id, "answer", None)

    #: forged credential rejected
    with pytest.raises(CaptchaTaskOwnership):
        manager.submit_result(task.id, "answer", "bogus")

    #: current generation owner succeeds
    manager.submit_result(task.id, "new-answer", second_token)
    assert task.result == "new-answer"

    #: late old answer can never overwrite the accepted result
    with pytest.raises(CaptchaTaskSolved):
        manager.submit_result(task.id, "old-answer", first_token)


def test_expired_waiting_task_rejects_submission():
    manager = make_manager()
    task = make_task(manager)
    expire_total(task)

    refreshed = manager.get_task_by_id(task.id)
    assert refreshed.status == "expired"

    with pytest.raises(CaptchaTaskExpired):
        manager.submit_result(task.id, "too-late", None)


def test_total_timeout_invalidates_a_still_valid_lease():
    manager = make_manager()
    task = make_task(manager)
    claimed = manager.get_task(exclusive=True)
    token = claimed.owner
    assert claimed.lease_valid()  #: lease itself is fine...

    expire_total(task)  #: ...but the overall captcha deadline elapsed

    assert manager.get_task_by_id(task.id).status == "expired"
    assert claimed.owner is None  #: credential killed

    with pytest.raises(CaptchaTaskExpired):
        manager.submit_result(task.id, "answer", token)

    with pytest.raises(CaptchaTaskExpired):
        manager.renew_lease(task.id, token)

    assert manager.get_task(exclusive=True) is None  #: never reopened


def test_positional_results_normalized_and_idempotent():
    manager = make_manager()
    task = make_task(manager, result_type="positional")
    claimed = manager.get_task(exclusive=True)
    token = claimed.owner

    with pytest.raises(CaptchaResultInvalid):
        manager.submit_result(task.id, "not-a-coordinate", token)
    assert task.result is None  #: illegal submission changed nothing

    manager.submit_result(task.id, "12 , 34", token)
    assert task.result == (12, 34)

    #: semantically identical duplicate, different formatting -> consistent
    manager.submit_result(task.id, "12,34", token)
    assert task.result == (12, 34)

    with pytest.raises(CaptchaTaskSolved):
        manager.submit_result(task.id, "56,78", token)


def test_shared_submission_remains_legacy_compatible():
    manager = make_manager()
    task = make_task(manager)

    manager.submit_result(task.id, "shared-answer", None)
    assert task.status == "solved"
    assert task.result == "shared-answer"
    assert task.winner is None

    #: repeated shared double-submit with same answer is consistent
    manager.submit_result(task.id, "shared-answer", None)

    with pytest.raises(CaptchaTaskSolved):
        manager.submit_result(task.id, "other", None)


def test_exclusive_claim_takes_shared_user_task_but_not_vice_versa():
    manager = make_manager()
    task = make_task(manager, status="shared-user")

    claimed = manager.get_task(exclusive=True)
    assert claimed is task
    assert task.status == "user"

    assert manager.get_task(exclusive=False) is None


def test_handler_answer_competes_with_exclusive_human():
    manager = make_manager()
    task = make_task(manager)
    claimed = manager.get_task(exclusive=True)
    human_token = claimed.owner

    #: background handler (Captcha9Kw/ExpertDecoders/...) answers first
    assert task.set_result("handler-answer") is True
    assert task.result == "handler-answer"

    #: human loses the race and cannot overwrite the terminal state
    with pytest.raises(CaptchaTaskSolved):
        manager.submit_result(task.id, "human-answer", human_token)
    assert task.result == "handler-answer"


def test_handler_cannot_overwrite_human_winner():
    manager = make_manager()
    task = make_task(manager)
    claimed = manager.get_task(exclusive=True)
    token = claimed.owner

    manager.submit_result(task.id, "human-answer", token)

    assert task.set_result("handler-answer") is False
    assert task.result == "human-answer"


def test_failed_handler_none_result_keeps_task_open():
    manager = make_manager()
    task = make_task(manager)

    #: Captcha9Kw reports None after its own polling fails
    assert task.set_result(None) is False
    assert task.result is None
    assert task.is_waiting()

    #: the task can still be claimed and solved afterwards
    claimed = manager.get_task(exclusive=True)
    manager.submit_result(task.id, "late-but-valid", claimed.owner)
    assert task.result == "late-but-valid"


def test_first_legal_result_wins_under_concurrency():
    manager = make_manager()
    task = make_task(manager)

    winners = []
    barrier = Barrier(8)

    def handler_answer(answer):
        barrier.wait()
        if task.set_result(answer):
            winners.append(answer)

    threads = [
        Thread(target=handler_answer, args=(f"answer-{i}",))
        for i in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
        assert not t.is_alive()

    assert len(winners) == 1
    assert task.result == winners[0]
    assert task.status == "solved"


def test_submission_cannot_touch_another_task():
    manager = make_manager()
    task_a = make_task(manager)
    task_b = make_task(manager)

    claim_a = manager.get_task(exclusive=True)
    claim_b = manager.get_task(exclusive=True)
    assert claim_a is task_a and claim_b is task_b

    manager.submit_result(task_a.id, "for-a", claim_a.owner)

    assert task_a.result == "for-a"
    assert task_b.result is None
    assert task_b.status == "user"


def test_unknown_task_submission_is_rejected():
    manager = make_manager()

    with pytest.raises(CaptchaTaskNotFound):
        manager.submit_result(404, "answer", None)

    with pytest.raises(CaptchaTaskNotFound):
        manager.renew_lease(404, "token")


def test_finish_solved_task_is_retained_then_purged():
    manager = make_manager()
    task = make_task(manager)
    claimed = manager.get_task(exclusive=True)
    manager.submit_result(task.id, "answer", claimed.owner)

    manager.finish_task(task)
    assert manager.get_task_by_id(task.id) is task  #: observable as solved
    assert manager.get_task_by_id(task.id).status == "solved"
    #: winner credential still works idempotently during retention
    manager.submit_result(task.id, "answer", claimed.owner)

    #: cleanup arrives -> old credentials die
    task.retain_until = 0
    assert manager.get_task_by_id(task.id) is None
    with pytest.raises(CaptchaTaskNotFound):
        manager.submit_result(task.id, "answer", claimed.owner)


def test_finish_expired_task_is_observable_then_purged():
    manager = make_manager()
    task = make_task(manager)
    expire_total(task)

    manager.finish_task(task)
    assert manager.get_task_by_id(task.id).status == "expired"

    task.retain_until = 0
    assert manager.get_task_by_id(task.id) is None


def test_cancelled_download_invalidates_credentials_immediately():
    manager = make_manager()
    task = make_task(manager)
    claimed = manager.get_task(exclusive=True)
    token = claimed.owner

    manager.finish_task(task, aborted=True)

    assert manager.get_task_by_id(task.id) is None
    with pytest.raises(CaptchaTaskNotFound):
        manager.submit_result(task.id, "answer", token)


def make_api():
    core = type("Core", (), {})()
    core._ = lambda value: value
    core.captcha_manager = make_manager()
    core.last_client_connected = 0
    return Api(core), core


def test_api_exclusive_flow_end_to_end():
    api, core = make_api()
    task = make_task(core.captcha_manager)

    info = api.get_captcha_task(exclusive=True)
    assert info.tid == int(task.id)
    assert info.token
    assert info.result_type == "textual"

    #: second exclusive caller gets nothing
    assert api.get_captcha_task(exclusive=True).tid == -1

    assert api.renew_captcha_task(info.tid, info.token) == "user"
    assert api.get_captcha_task_status(info.tid) == "user"

    assert api.set_captcha_result(info.tid, "answer", info.token) == "solved"
    assert api.get_captcha_task_status(info.tid) == "solved"

    #: renewing after solution reports the single terminal state
    assert api.renew_captcha_task(info.tid, info.token) == "solved"


def test_api_shared_flow_and_status_states():
    api, core = make_api()
    task = make_task(core.captcha_manager)

    assert api.is_captcha_waiting()

    shared = api.get_captcha_task(exclusive=False)
    assert shared.tid == int(task.id)
    assert shared.token is None
    assert api.get_captcha_task_status(task.id) == "shared-user"

    assert api.set_captcha_result(task.id, "web-answer") == "solved"
    assert api.get_captcha_task_status(task.id) == "solved"


def test_api_status_expired_and_nonexistent():
    api, core = make_api()
    task = make_task(core.captcha_manager)

    assert api.get_captcha_task_status(task.id) == "waiting"

    expire_total(task)
    assert api.get_captcha_task_status(task.id) == "expired"

    assert api.get_captcha_task_status(999) == ""  #: nonexistent


def test_api_waiting_state_after_reopen():
    api, core = make_api()
    task = make_task(core.captcha_manager)

    info = api.get_captcha_task(exclusive=True)
    expire_lease(core.captcha_manager.get_task_by_id(info.tid))

    assert api.get_captcha_task_status(info.tid) == "waiting"
    assert api.is_captcha_waiting()
