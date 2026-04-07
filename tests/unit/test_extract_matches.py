"""
Testes unitarios para extract_matches.
Verifica deduplicacao de match IDs, idempotencia e load_match_ids.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_client():
    return MagicMock()


class TestExtractMatchIds:
    def test_deduplicates_match_ids_across_players(self, mock_client, tmp_path):
        mock_client.get.return_value = ["BR1_100", "BR1_200", "BR1_300"]

        accounts = [
            {"team": "FURIA", "game_name": "Guigo", "puuid": "puuid-1"},
            {"team": "FURIA", "game_name": "Tatu", "puuid": "puuid-2"},
        ]

        with patch("src.extract.extract_matches.BASE_OUTPUT_DIR", str(tmp_path)):
            from src.extract.extract_matches import extract_match_ids
            result = extract_match_ids(mock_client, accounts, "2026-03-26")

        assert len(result) == 3
        assert sorted(result) == ["BR1_100", "BR1_200", "BR1_300"]

    def test_empty_response_excluded(self, mock_client, tmp_path):
        mock_client.get.side_effect = [["BR1_100"], None]

        accounts = [
            {"team": "FURIA", "game_name": "Guigo", "puuid": "puuid-1"},
            {"team": "FURIA", "game_name": "Tatu", "puuid": "puuid-2"},
        ]

        with patch("src.extract.extract_matches.BASE_OUTPUT_DIR", str(tmp_path)):
            from src.extract.extract_matches import extract_match_ids
            result = extract_match_ids(mock_client, accounts, "2026-03-26")

        assert result == ["BR1_100"]

    def test_saves_match_ids_json(self, mock_client, tmp_path):
        mock_client.get.return_value = ["BR1_100"]

        accounts = [{"team": "FURIA", "game_name": "Guigo", "puuid": "puuid-1"}]

        with patch("src.extract.extract_matches.BASE_OUTPUT_DIR", str(tmp_path)):
            from src.extract.extract_matches import extract_match_ids
            extract_match_ids(mock_client, accounts, "2026-03-26")

        ids_path = tmp_path / "match_ids" / "dt=2026-03-26" / "match_ids.json"
        assert ids_path.exists()
        assert json.loads(ids_path.read_text()) == ["BR1_100"]


class TestExtractMatchDetails:
    def test_skips_existing_match_idempotency(self, mock_client, tmp_path):
        matches_dir = tmp_path / "matches" / "dt=2026-03-26"
        matches_dir.mkdir(parents=True)
        (matches_dir / "BR1_100.json").write_text('{"existing": true}')

        with patch("src.extract.extract_matches.BASE_OUTPUT_DIR", str(tmp_path)):
            from src.extract.extract_matches import extract_match_details
            extracted = extract_match_details(
                mock_client, ["BR1_100"], "2026-03-26"
            )

        assert extracted == 0
        mock_client.get.assert_not_called()

    def test_saves_new_match(self, mock_client, tmp_path):
        mock_client.get.return_value = {
            "metadata": {"matchId": "BR1_100"},
            "info": {"gameDuration": 1800},
        }

        with patch("src.extract.extract_matches.BASE_OUTPUT_DIR", str(tmp_path)):
            from src.extract.extract_matches import extract_match_details
            extracted = extract_match_details(
                mock_client, ["BR1_100"], "2026-03-26"
            )

        assert extracted == 1
        match_path = tmp_path / "matches" / "dt=2026-03-26" / "BR1_100.json"
        assert match_path.exists()

        data = json.loads(match_path.read_text())
        assert data["metadata"]["matchId"] == "BR1_100"
        assert "_extraction_metadata" in data

    def test_failed_match_not_saved(self, mock_client, tmp_path):
        mock_client.get.return_value = None

        with patch("src.extract.extract_matches.BASE_OUTPUT_DIR", str(tmp_path)):
            from src.extract.extract_matches import extract_match_details
            extracted = extract_match_details(
                mock_client, ["BR1_100"], "2026-03-26"
            )

        assert extracted == 0
        match_path = tmp_path / "matches" / "dt=2026-03-26" / "BR1_100.json"
        assert not match_path.exists()


class TestLoadMatchIds:
    def test_loads_existing_file(self, tmp_path):
        ids_dir = tmp_path / "match_ids" / "dt=2026-03-26"
        ids_dir.mkdir(parents=True)
        (ids_dir / "match_ids.json").write_text('["BR1_100", "BR1_200"]')

        with patch("src.extract.extract_matches.BASE_OUTPUT_DIR", str(tmp_path)):
            from src.extract.extract_matches import load_match_ids
            result = load_match_ids("2026-03-26")

        assert result == ["BR1_100", "BR1_200"]

    def test_returns_empty_when_file_missing(self, tmp_path):
        with patch("src.extract.extract_matches.BASE_OUTPUT_DIR", str(tmp_path)):
            from src.extract.extract_matches import load_match_ids
            result = load_match_ids("2026-03-26")

        assert result == []
