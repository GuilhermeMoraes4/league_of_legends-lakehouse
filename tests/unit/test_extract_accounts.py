"""
Testes unitarios para extract_accounts.
Verifica URL encoding e persistencia no bronze layer.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

FAKE_KEY = "RGAPI-test-key-fake"


@pytest.fixture
def mock_client():
    return MagicMock()


class TestURLEncoding:
    """Garante que nomes com espacos e caracteres especiais nao quebram a URL."""

    def test_game_name_with_space_is_encoded(self, mock_client, tmp_path):
        mock_client.get.return_value = {
            "puuid": "puuid-jean-mago",
            "gameName": "Jean Mago",
            "tagLine": "BR1",
        }

        players = [{"team": "LOUD", "role": "mid", "game_name": "Jean Mago", "tag_line": "BR1"}]

        with (
            patch("src.extract.extract_accounts.CBLOL_PLAYERS", players),
            patch("src.extract.extract_accounts.BASE_OUTPUT_DIR", str(tmp_path)),
        ):
            from src.extract.extract_accounts import extract_accounts
            extract_accounts(mock_client, "2026-03-26")

        # Verifica que a URL usada contem %20 (espaco encodado)
        called_url = mock_client.get.call_args[0][0]
        assert "Jean%20Mago" in called_url or "Jean+Mago" in called_url
        assert " " not in called_url

    def test_successful_extraction_returns_puuid(self, mock_client, tmp_path):
        mock_client.get.return_value = {
            "puuid": "abc-123-puuid",
            "gameName": "Guigo",
            "tagLine": "BR1",
        }

        players = [{"team": "FURIA", "role": "top", "game_name": "Guigo", "tag_line": "BR1"}]

        with (
            patch("src.extract.extract_accounts.CBLOL_PLAYERS", players),
            patch("src.extract.extract_accounts.BASE_OUTPUT_DIR", str(tmp_path)),
        ):
            from src.extract.extract_accounts import extract_accounts
            results = extract_accounts(mock_client, "2026-03-26")

        assert len(results) == 1
        assert results[0]["puuid"] == "abc-123-puuid"
        assert results[0]["team"] == "FURIA"

    def test_failed_player_excluded_from_results(self, mock_client, tmp_path):
        mock_client.get.return_value = None  # API retornou erro

        players = [{"team": "FURIA", "role": "top", "game_name": "Guigo", "tag_line": "BR1"}]

        with (
            patch("src.extract.extract_accounts.CBLOL_PLAYERS", players),
            patch("src.extract.extract_accounts.BASE_OUTPUT_DIR", str(tmp_path)),
        ):
            from src.extract.extract_accounts import extract_accounts
            results = extract_accounts(mock_client, "2026-03-26")

        assert results == []

    def test_output_file_created_in_bronze_layer(self, mock_client, tmp_path):
        mock_client.get.return_value = {
            "puuid": "abc-123",
            "gameName": "Guigo",
            "tagLine": "BR1",
        }

        players = [{"team": "FURIA", "role": "top", "game_name": "Guigo", "tag_line": "BR1"}]

        with (
            patch("src.extract.extract_accounts.CBLOL_PLAYERS", players),
            patch("src.extract.extract_accounts.BASE_OUTPUT_DIR", str(tmp_path)),
        ):
            from src.extract.extract_accounts import extract_accounts
            extract_accounts(mock_client, "2026-03-26")

        expected_path = tmp_path / "accounts" / "dt=2026-03-26" / "accounts.json"
        assert expected_path.exists()

        data = json.loads(expected_path.read_text())
        assert len(data) == 1
        assert data[0]["puuid"] == "abc-123"
