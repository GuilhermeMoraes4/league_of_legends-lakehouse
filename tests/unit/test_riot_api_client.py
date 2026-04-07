"""
Testes unitarios para o RiotAPIClient.
Usa responses para mockar chamadas HTTP sem precisar de rede real.
"""

from unittest.mock import patch

import pytest
import responses as responses_lib

from src.extract.riot_api_client import RiotAPIClient

FAKE_KEY = "RGAPI-test-key-fake"
BASE_URL = "https://americas.api.riotgames.com"


@pytest.fixture
def client():
    with patch("src.extract.riot_api_client.get_api_key", return_value=FAKE_KEY):
        return RiotAPIClient(api_key=FAKE_KEY)


class TestRiotAPIClientInit:
    def test_raises_without_api_key(self):
        with patch("src.extract.riot_api_client.get_api_key", return_value=None):
            with pytest.raises(ValueError, match="RIOT_DEVELOPER_API"):
                RiotAPIClient(api_key=None)

    def test_sets_auth_header(self, client):
        assert client.session.headers["X-Riot-Token"] == FAKE_KEY


class TestRiotAPIClientGet:
    @responses_lib.activate
    def test_returns_json_on_200(self, client):
        url = f"{BASE_URL}/test"
        responses_lib.add(responses_lib.GET, url, json={"puuid": "abc123"}, status=200)

        result = client.get(url)

        assert result == {"puuid": "abc123"}

    @responses_lib.activate
    def test_returns_none_on_404(self, client):
        url = f"{BASE_URL}/test"
        responses_lib.add(responses_lib.GET, url, json={}, status=404)

        result = client.get(url)

        assert result is None

    @responses_lib.activate
    def test_returns_none_on_403(self, client):
        url = f"{BASE_URL}/test"
        responses_lib.add(responses_lib.GET, url, json={}, status=403)

        result = client.get(url)

        assert result is None

    @responses_lib.activate
    def test_retries_on_500_and_returns_none_after_exhaustion(self, client):
        url = f"{BASE_URL}/test"
        # MAX_RETRIES = 3, adiciona 3 respostas 500
        for _ in range(3):
            responses_lib.add(responses_lib.GET, url, json={}, status=500)

        with patch("src.extract.riot_api_client.time.sleep"):
            result = client.get(url)

        assert result is None
        assert len(responses_lib.calls) == 3

    @responses_lib.activate
    def test_respects_retry_after_on_429(self, client):
        url = f"{BASE_URL}/test"
        responses_lib.add(
            responses_lib.GET, url,
            json={}, status=429,
            headers={"Retry-After": "2"},
        )
        responses_lib.add(responses_lib.GET, url, json={"ok": True}, status=200)

        sleep_calls = []
        with patch("src.extract.riot_api_client.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            result = client.get(url)

        assert result == {"ok": True}
        # Deve ter dormido pelo menos Retry-After + 1 = 3s
        assert any(s >= 3 for s in sleep_calls)
