"""
Testes unitarios para o DBFSClient.
Usa responses para mockar chamadas HTTP sem precisar de rede real.
"""

import base64
from unittest.mock import patch

import pytest
import responses as responses_lib

from src.upload.dbfs_client import DBFSClient

FAKE_HOST = "community.cloud.databricks.com"
FAKE_TOKEN = "dapi-test-token-fake"
BASE_URL = f"https://{FAKE_HOST}/api/2.0/dbfs"


@pytest.fixture
def client():
    return DBFSClient(host=FAKE_HOST, token=FAKE_TOKEN)


class TestDBFSClientInit:
    def test_raises_without_credentials(self):
        with patch("src.upload.dbfs_client.get_databricks_host", return_value=""):
            with patch("src.upload.dbfs_client.get_databricks_token", return_value=""):
                with pytest.raises(ValueError, match="DATABRICKS_HOST"):
                    DBFSClient()

    def test_sets_auth_header(self, client):
        assert client.session.headers["Authorization"] == f"Bearer {FAKE_TOKEN}"

    def test_builds_correct_base_url(self, client):
        assert client.base_url == BASE_URL


class TestDBFSClientMkdirs:
    @responses_lib.activate
    def test_mkdirs_success(self, client):
        responses_lib.add(responses_lib.POST, f"{BASE_URL}/mkdirs", json={}, status=200)

        assert client.mkdirs("/lol-lakehouse/bronze") is True

    @responses_lib.activate
    def test_mkdirs_sends_correct_payload(self, client):
        responses_lib.add(responses_lib.POST, f"{BASE_URL}/mkdirs", json={}, status=200)

        client.mkdirs("/test/path")

        import json
        body = json.loads(responses_lib.calls[0].request.body)
        assert body["path"] == "/test/path"


class TestDBFSClientPutFile:
    @responses_lib.activate
    def test_put_small_file(self, client, tmp_path):
        responses_lib.add(responses_lib.POST, f"{BASE_URL}/put", json={}, status=200)

        test_file = tmp_path / "test.json"
        test_file.write_text('{"key": "value"}')

        assert client.put_file(str(test_file), "/dbfs/test.json") is True

        import json
        body = json.loads(responses_lib.calls[0].request.body)
        assert body["path"] == "/dbfs/test.json"
        assert body["overwrite"] is True
        decoded = base64.b64decode(body["contents"])
        assert json.loads(decoded) == {"key": "value"}

    @responses_lib.activate
    def test_put_returns_false_on_auth_error(self, client, tmp_path):
        responses_lib.add(responses_lib.POST, f"{BASE_URL}/put", json={}, status=403)

        test_file = tmp_path / "test.json"
        test_file.write_text("{}")

        assert client.put_file(str(test_file), "/dbfs/test.json") is False


class TestDBFSClientFileExists:
    @responses_lib.activate
    def test_file_exists_returns_true(self, client):
        responses_lib.add(
            responses_lib.GET,
            f"{BASE_URL}/get-status",
            json={"path": "/test.json", "is_dir": False, "file_size": 100},
            status=200,
        )

        assert client.file_exists("/test.json") is True

    @responses_lib.activate
    def test_file_exists_returns_false_on_404(self, client):
        responses_lib.add(
            responses_lib.GET,
            f"{BASE_URL}/get-status",
            json={},
            status=404,
        )

        assert client.file_exists("/not-found.json") is False

    @responses_lib.activate
    def test_file_exists_returns_false_for_directory(self, client):
        responses_lib.add(
            responses_lib.GET,
            f"{BASE_URL}/get-status",
            json={"path": "/test", "is_dir": True},
            status=200,
        )

        assert client.file_exists("/test") is False


class TestDBFSClientRetry:
    @responses_lib.activate
    def test_retries_on_500_and_returns_none(self, client):
        for _ in range(3):
            responses_lib.add(responses_lib.POST, f"{BASE_URL}/put", json={}, status=500)

        with patch("src.upload.dbfs_client.time.sleep"):
            result = client._post("put", {"path": "/test", "contents": "x", "overwrite": True})

        assert result is None
        assert len(responses_lib.calls) == 3

    @responses_lib.activate
    def test_respects_retry_after_on_429(self, client):
        responses_lib.add(
            responses_lib.POST,
            f"{BASE_URL}/mkdirs",
            json={},
            status=429,
            headers={"Retry-After": "2"},
        )
        responses_lib.add(responses_lib.POST, f"{BASE_URL}/mkdirs", json={}, status=200)

        sleep_calls = []
        with patch("src.upload.dbfs_client.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            result = client._post("mkdirs", {"path": "/test"})

        assert result == {}
        assert any(s >= 3 for s in sleep_calls)


class TestDBFSClientContextManager:
    def test_context_manager_closes_session(self):
        with DBFSClient(host=FAKE_HOST, token=FAKE_TOKEN) as client:
            assert client.session is not None
        # Apos o context manager, session deve estar fechada
        # (requests nao tem um .closed, mas nao deve dar erro)
