"""End-to-end HTTP tests for the generation-bound exclusive captcha protocol."""

import time

API_KEY_HEADER = "X-API-KEY"


def _queue_captcha(pyload_core, timeout=120):
    manager = pyload_core.captcha_manager
    task = manager.new_task("jpg", {"src": "data:", "file": "x.jpg"}, "textual")
    task.set_waiting(timeout)
    manager.tasks.append(task)
    return task


class TestExclusiveCaptchaProtocol:
    def test_exclusive_claim_renew_submit_and_rejections(self, pyload_core, client, api_key):
        headers = {API_KEY_HEADER: api_key}
        task = _queue_captcha(pyload_core)
        tid = int(task.id)

        #: first exclusive caller atomically gets the task + credential
        response = client.get(
            f"/api/get_captcha_task?exclusive=true", headers=headers
        )
        assert response.status_code == 200
        claim = response.json
        assert claim["tid"] == tid
        token = claim["token"]
        assert token

        #: another exclusive caller cannot get the same task
        response = client.get(
            "/api/get_captcha_task?exclusive=true", headers=headers
        )
        assert response.status_code == 200
        assert response.json["tid"] == -1

        #: heartbeat works with the issued credential
        response = client.get(
            f"/api/renew_captcha_task?tid={tid}&token={token}", headers=headers
        )
        assert response.status_code == 200
        assert response.json == "user"

        #: submission without / with wrong credential is explicitly rejected
        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=ABCD", headers=headers
        )
        assert response.status_code == 409

        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=ABCD&token=forged",
            headers=headers,
        )
        assert response.status_code == 409

        #: owner submission accepted
        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=ABCD&token={token}",
            headers=headers,
        )
        assert response.status_code == 200
        assert response.json == "solved"
        assert pyload_core.captcha_manager.get_task_by_id(tid).result == "ABCD"

        #: same legal duplicate submission -> consistent result
        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=ABCD&token={token}",
            headers=headers,
        )
        assert response.status_code == 200
        assert response.json == "solved"

        #: a losing party (even with a different answer) cannot overwrite it
        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=WXYZ&token={token}",
            headers=headers,
        )
        assert response.status_code == 409

        response = client.get(
            f"/api/get_captcha_task_status?tid={tid}", headers=headers
        )
        assert response.json == "solved"

        #: renewal after solution reports the single terminal state
        response = client.get(
            f"/api/renew_captcha_task?tid={tid}&token={token}", headers=headers
        )
        assert response.json == "solved"

    def test_dead_holder_reopens_then_stale_credential_rejected(self, pyload_core, client, api_key):
        headers = {API_KEY_HEADER: api_key}
        task = _queue_captcha(pyload_core)
        tid = int(task.id)

        claim = client.get(
            "/api/get_captcha_task?exclusive=true", headers=headers
        ).json
        old_token = claim["token"]
        assert task.generation == 1

        #: holder disappears: force the lease to lapse
        task.lease_until = time.monotonic() - 1

        status = client.get(
            f"/api/get_captcha_task_status?tid={tid}", headers=headers
        ).json
        assert status == "waiting"

        #: task is offered again and a new generation/credential is issued
        claim_again = client.get(
            "/api/get_captcha_task?exclusive=true", headers=headers
        ).json
        assert claim_again["tid"] == tid
        assert claim_again["token"] != old_token
        assert task.generation == 2

        #: the old credential can no longer submit
        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=late&token={old_token}",
            headers=headers,
        )
        assert response.status_code == 409

        client.post(
            f"/api/set_captcha_result?tid={tid}"
            f"&result=winner&token={claim_again['token']}",
            headers=headers,
        )

        #: stale answer from the first generation cannot overwrite the winner
        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=late&token={old_token}",
            headers=headers,
        )
        assert response.status_code == 409
        assert task.result == "winner"

    def test_expired_and_unknown_tasks_rejected(self, pyload_core, client, api_key):
        headers = {API_KEY_HEADER: api_key}
        task = _queue_captcha(pyload_core)
        tid = int(task.id)
        task.wait_until = time.monotonic() - 1

        response = client.get(
            f"/api/get_captcha_task_status?tid={tid}", headers=headers
        )
        assert response.json == "expired"

        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=too-late", headers=headers
        )
        assert response.status_code == 410

        #: unknown task ids are 404 on submit and "" on status
        response = client.post(
            "/api/set_captcha_result?tid=987654&result=x", headers=headers
        )
        assert response.status_code == 404

        response = client.get(
            "/api/get_captcha_task_status?tid=987654", headers=headers
        )
        assert response.status_code == 200
        assert response.json == ""

    def test_shared_submission_remains_supported(self, pyload_core, client, api_key):
        headers = {API_KEY_HEADER: api_key}
        task = _queue_captcha(pyload_core)
        tid = int(task.id)

        claim = client.get("/api/get_captcha_task", headers=headers).json
        assert claim["tid"] == tid
        assert claim["token"] is None

        response = client.post(
            f"/api/set_captcha_result?tid={tid}&result=web", headers=headers
        )
        assert response.status_code == 200
        assert response.json == "solved"

        #: solved tasks are no longer offered to shared clients
        response = client.get("/api/is_captcha_waiting", headers=headers)
        assert response.status_code == 200
        assert response.json is False
