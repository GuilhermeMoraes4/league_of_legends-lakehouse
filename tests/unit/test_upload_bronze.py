"""
Testes unitarios para upload_bronze.
Usa MagicMock para o DBFSClient e tmp_path para o filesystem.
"""


from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_dbfs_client():
    client = MagicMock()
    client.mkdirs.return_value = True
    client.put_file.return_value = True
    client.file_exists.return_value = False
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)
    return client


class TestUploadBronze:
    def test_skips_when_databricks_not_configured(self, tmp_path):
        with (
            patch("src.upload.upload_bronze.get_databricks_host", return_value=""),
            patch("src.upload.upload_bronze.get_databricks_token", return_value=""),
            patch("src.upload.upload_bronze.BASE_OUTPUT_DIR", str(tmp_path)),
        ):
            from src.upload.upload_bronze import upload_bronze
            result = upload_bronze("2026-04-04")

        assert result == 0

    def test_uploads_accounts_file(self, tmp_path, mock_dbfs_client):
        accounts_dir = tmp_path / "accounts" / "dt=2026-04-04"
        accounts_dir.mkdir(parents=True)
        (accounts_dir / "accounts.json").write_text('[{"puuid": "abc"}]')

        with (
            patch("src.upload.upload_bronze.get_databricks_host", return_value="host"),
            patch("src.upload.upload_bronze.get_databricks_token", return_value="token"),
            patch("src.upload.upload_bronze.BASE_OUTPUT_DIR", str(tmp_path)),
            patch("src.upload.upload_bronze.DBFSClient", return_value=mock_dbfs_client),
        ):
            from src.upload.upload_bronze import upload_bronze
            result = upload_bronze("2026-04-04")

        assert result == 1
        mock_dbfs_client.put_file.assert_called_once()

    def test_skips_existing_dbfs_files(self, tmp_path, mock_dbfs_client):
        accounts_dir = tmp_path / "accounts" / "dt=2026-04-04"
        accounts_dir.mkdir(parents=True)
        (accounts_dir / "accounts.json").write_text("[]")

        mock_dbfs_client.file_exists.return_value = True

        with (
            patch("src.upload.upload_bronze.get_databricks_host", return_value="host"),
            patch("src.upload.upload_bronze.get_databricks_token", return_value="token"),
            patch("src.upload.upload_bronze.BASE_OUTPUT_DIR", str(tmp_path)),
            patch("src.upload.upload_bronze.DBFSClient", return_value=mock_dbfs_client),
        ):
            from src.upload.upload_bronze import upload_bronze
            result = upload_bronze("2026-04-04")

        assert result == 0
        mock_dbfs_client.put_file.assert_not_called()

    def test_uploads_multiple_match_files(self, tmp_path, mock_dbfs_client):
        matches_dir = tmp_path / "matches" / "dt=2026-04-04"
        matches_dir.mkdir(parents=True)
        (matches_dir / "BR1_100.json").write_text("{}")
        (matches_dir / "BR1_200.json").write_text("{}")
        (matches_dir / "BR1_300.json").write_text("{}")

        with (
            patch("src.upload.upload_bronze.get_databricks_host", return_value="host"),
            patch("src.upload.upload_bronze.get_databricks_token", return_value="token"),
            patch("src.upload.upload_bronze.BASE_OUTPUT_DIR", str(tmp_path)),
            patch("src.upload.upload_bronze.DBFSClient", return_value=mock_dbfs_client),
        ):
            from src.upload.upload_bronze import upload_bronze
            result = upload_bronze("2026-04-04")

        assert result == 3

    def test_skips_nonexistent_directories(self, tmp_path, mock_dbfs_client):
        with (
            patch("src.upload.upload_bronze.get_databricks_host", return_value="host"),
            patch("src.upload.upload_bronze.get_databricks_token", return_value="token"),
            patch("src.upload.upload_bronze.BASE_OUTPUT_DIR", str(tmp_path)),
            patch("src.upload.upload_bronze.DBFSClient", return_value=mock_dbfs_client),
        ):
            from src.upload.upload_bronze import upload_bronze
            result = upload_bronze("2026-04-04")

        assert result == 0
        mock_dbfs_client.put_file.assert_not_called()

    def test_counts_failed_uploads(self, tmp_path, mock_dbfs_client):
        accounts_dir = tmp_path / "accounts" / "dt=2026-04-04"
        accounts_dir.mkdir(parents=True)
        (accounts_dir / "accounts.json").write_text("[]")

        mock_dbfs_client.put_file.return_value = False

        with (
            patch("src.upload.upload_bronze.get_databricks_host", return_value="host"),
            patch("src.upload.upload_bronze.get_databricks_token", return_value="token"),
            patch("src.upload.upload_bronze.BASE_OUTPUT_DIR", str(tmp_path)),
            patch("src.upload.upload_bronze.DBFSClient", return_value=mock_dbfs_client),
        ):
            from src.upload.upload_bronze import upload_bronze
            result = upload_bronze("2026-04-04")

        assert result == 0
