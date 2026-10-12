import time
from io import BytesIO

import pytest


API_KEY_HEADER = "X-API-KEY"


def test_oversized_multipart_api_upload_returns_413(app, client, api_key, monkeypatch):
    monkeypatch.setitem(app.config, "MAX_CONTENT_LENGTH", 1024)

    response = client.post(
        "/api/check_online_status_container",
        data={
            "urls": "[]",
            "container": "test.bin",
            "data": (BytesIO(b"x" * 2048), "test.bin"),
        },
        headers={API_KEY_HEADER: api_key},
    )

    assert response.status_code == 413
    assert response.json == {"error": "Request body too large"}


class TestOnlineStatusPolling:
    URL = "https://example.org/file.zip"
    UNKNOWN_RID = 12345

    def _start_check(self, client, api_key):
        response = client.post(
            "/api/check_online_status",
            json={"urls": [self.URL]},
            headers={API_KEY_HEADER: api_key},
        )
        assert response.status_code == 200
        return response.json

    def _poll(self, client, api_key, rid, **params):
        params["rid"] = rid
        response = client.get(
            "/api/poll_results",
            query_string=params,
            headers={API_KEY_HEADER: api_key},
        )
        assert response.status_code == 200
        return response.json

    def _poll_until_terminal(self, client, api_key, rid, **params):
        result = None
        for _ in range(30):
            result = self._poll(client, api_key, rid, **params)
            if result["rid"] == -1:
                return result
            time.sleep(0.1)
        raise AssertionError(f"online check {rid} never reached a terminal state: {result}")

    def test_initial_check_has_stable_identity_and_legacy_shape(self, api_key, client):
        check = self._start_check(client, api_key)

        assert check["rid"] >= 0
        # legacy reply shape is unchanged
        assert set(check) == {"rid", "data"}
        assert set(check["data"]) == {self.URL}
        assert check["data"][self.URL]["status"] == 3

    def test_legacy_polling_completes_with_rid_minus_one(self, api_key, client):
        rid = self._start_check(client, api_key)["rid"]

        result = self._poll_until_terminal(client, api_key, rid)

        assert set(result) == {"rid", "data"}
        assert result["rid"] == -1
        assert result["data"][self.URL]["status"] == 3

        # an unknown id is an explicit terminal reply, not "no new data"
        unknown = self._poll(client, api_key, self.UNKNOWN_RID)
        assert unknown == {"rid": -1, "data": {}}

    def test_cursor_polling_replays_batches_and_completes_in_order(self, api_key, client):
        rid = self._start_check(client, api_key)["rid"]

        result = self._poll_until_terminal(client, api_key, rid, progress=0)

        assert result["rid"] == -1
        assert result["status"] == "complete"
        assert result["progress"] == 0
        assert result["next_progress"] >= 1
        assert set(result["data"]) == {self.URL}

        # same cursor replays the exact same batches (lost reply recovery)
        replayed = self._poll(client, api_key, rid, progress=0)
        assert replayed["data"] == result["data"]
        assert replayed["status"] == "complete"
        assert replayed["next_progress"] == result["next_progress"]

        # the next cursor only confirms the terminal state
        after = self._poll(client, api_key, rid, progress=result["next_progress"])
        assert after["rid"] == -1
        assert after["status"] == "complete"
        assert after["data"] == {}

        # two independent consumers both see the complete batch log
        other = self._poll(client, api_key, rid, progress=0)
        assert set(other["data"]) == {self.URL}

        unknown = self._poll(client, api_key, self.UNKNOWN_RID, progress=0)
        assert unknown["rid"] == -1
        assert unknown["status"] == "unknown"
        assert unknown["data"] == {}


class TestAuthorization:
    def test_error_when_missing_auth(self, client):
        response = client.get("/api/status_server")

        assert response.status_code == 401

        response = client.post("/api/restart")

        assert response.status_code == 401

    @pytest.mark.parametrize(
        "invalid_api_key",
        [
            None, "", "x", "pl_123456",
        ]
    )
    def test_error_when_invalid_auth(self, client, invalid_api_key):
        response = client.get("/api/status_server", headers={API_KEY_HEADER: invalid_api_key})

        assert response.status_code == 401

        response = client.post("/api/restart", headers={API_KEY_HEADER: invalid_api_key})

        assert response.status_code == 401

    def test_error_when_auth_expired(self, pyload_core, client):
        expired_api_key = pyload_core.api.generate_apikey("pyload", "pyload", expires=int(time.time() * 1000))["data"]["key"]

        response = client.get("/api/status_server", headers={API_KEY_HEADER: expired_api_key})

        assert response.status_code == 401

        response = client.post("/api/restart", headers={API_KEY_HEADER: expired_api_key})

        assert response.status_code == 401

    def test_success_with_valid_auth_without_expiry(self, api_key, client):
        response = client.get("/api/status_server", headers={API_KEY_HEADER: api_key})

        assert response.status_code == 200
        assert response.json["active"] == 0

        response = client.post("/api/restart", headers={API_KEY_HEADER: api_key})

        assert response.status_code == 200

    def test_success_with_valid_auth_with_expiry(self, pyload_core, client):
        expiry_date = int(time.time() * 1000) + 1000
        valid_api_key = pyload_core.api.generate_apikey("pyload", "pyload", expires=expiry_date)["data"]["key"]

        response = client.get("/api/status_server", headers={API_KEY_HEADER: valid_api_key})

        assert response.status_code == 200
        assert response.json["active"] == 0

        response = client.post("/api/restart", headers={API_KEY_HEADER: valid_api_key})

        assert response.status_code == 200


class TestPackages:
    @pytest.mark.parametrize(
        ("destination", "destination_id"),
        [
            ("collector", 0),
            ("queue", 1),
        ]
    )
    def test_add_and_remove_package(self, api_key, client, destination, destination_id):
        # Assert no packages in destination at the start
        response = client.get(f"api/get_{destination}", headers={API_KEY_HEADER: api_key})

        assert response.status_code == 200
        assert len(response.json) == 0

        # Add a dummy package to the destination
        response = client.post(
            "/api/add_package",
            json={
                "name": "test_package",
                "links": [
                    "https://localhost/non-existent-file.zip"
                ],
                "dest": destination_id
            },
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200
        package_id = response.text

        # Assert destination contains dummy package
        response = client.get(f"api/get_{destination}", headers={API_KEY_HEADER: api_key})

        assert response.status_code == 200
        assert len(response.json) == 1
        assert response.json[0]["name"] == "test_package"
        assert response.json[0]["pid"] == int(package_id)

        # Delete package
        response = client.post(
            "/api/delete_packages",
            json={
                "package_ids": [package_id]
            },
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200

        # Assert destination is empty again
        response = client.get(f"api/get_{destination}", headers={API_KEY_HEADER: api_key})

        assert response.status_code == 200
        assert len(response.json) == 0

    def test_move_package(self, api_key, client):
        # Add a dummy package to the collector
        response = client.post(
            "/api/add_package",
            json={
                "name": "test_package",
                "links": [
                    "https://localhost/non-existent-file.zip"
                ],
                "dest": 0
            },
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200
        package_id = response.text

        # Move package to queue
        response = client.post(
            "/api/move_package",
            query_string={"destination": 1, "package_id": package_id},
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200

        # Assert package is now in queue and no longer in collector
        response = client.get("api/get_queue", headers={API_KEY_HEADER: api_key})

        assert response.status_code == 200
        assert len(response.json) == 1
        assert response.json[0]["name"] == "test_package"
        assert response.json[0]["pid"] == int(package_id)

        response = client.get("api/get_collector", headers={API_KEY_HEADER: api_key})

        assert response.status_code == 200
        assert len(response.json) == 0

        # Delete package again
        response = client.post(
            "/api/delete_packages",
            json={
                "package_ids": [package_id]
            },
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200

    def test_package_folder_is_sanitized(self, api_key, client):
        # Add a dummy package to the collector
        response = client.post(
            "/api/add_package",
            json={
                "name": "https://TEST strange package:name\\",
                "links": [
                    "https://localhost/non-existent-file.zip"
                ],
                "dest": 0
            },
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200
        package_id = response.text

        # Get package data for dummy package
        response = client.get(
            "api/get_package_data",
            query_string={"package_id": package_id},
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200
        assert response.json["folder"] == "TEST_strange_packagename"

        # Update package folder
        response = client.post(
            "/api/set_package_data",
            json={
                "package_id": package_id,
                "data": {
                    "_folder": "/users/root/"
                }
            },
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200

        # Get package data for updated dummy package
        response = client.get(
            "api/get_package_data",
            query_string={"package_id": package_id},
            headers={API_KEY_HEADER: api_key}
        )

        assert response.status_code == 200
        assert response.json["folder"] == "users_root"
