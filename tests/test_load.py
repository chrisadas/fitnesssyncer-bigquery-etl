from unittest.mock import MagicMock, call, patch

import pytest


@pytest.fixture(autouse=True)
def env_vars(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("BQ_DATASET", "test_dataset")


@pytest.fixture
def mock_bq_client():
    with patch("load.bigquery.Client") as mock_cls:
        yield mock_cls.return_value


@pytest.fixture
def loader(mock_bq_client):
    from load import BigQueryLoader
    return BigQueryLoader()


def make_row(item_id="item1", source_id="1"):
    return {
        "item_id": item_id,
        "source_id": source_id,
        "source_type": "Garmin",
        "date_utc": "2024-01-01T00:00:00+00:00",
        "date_ms": 1704067200000,
        "extra": {},
        "synced_at": "2024-06-01T00:00:00+00:00",
    }


class TestGetSyncState:
    def test_returns_source_id_to_ms_mapping(self, loader, mock_bq_client):
        mock_bq_client.query.return_value = [
            MagicMock(source_id="1", last_synced_ms=1000),
            MagicMock(source_id="2", last_synced_ms=2000),
        ]
        result = loader.get_sync_state()
        assert result == {"1": 1000, "2": 2000}

    def test_returns_empty_dict_when_no_state(self, loader, mock_bq_client):
        mock_bq_client.query.return_value = []
        assert loader.get_sync_state() == {}

    def test_queries_sync_state_table(self, loader, mock_bq_client):
        mock_bq_client.query.return_value = []
        loader.get_sync_state()
        sql = mock_bq_client.query.call_args[0][0]
        assert "_sync_state" in sql


class TestUpsertRows:
    def test_skips_on_empty_list(self, loader, mock_bq_client):
        loader.upsert_rows([])
        mock_bq_client.load_table_from_json.assert_not_called()

    def test_loads_to_staging_then_merges(self, loader, mock_bq_client):
        load_job = MagicMock()
        mock_bq_client.load_table_from_json.return_value = load_job
        mock_bq_client.get_table.return_value = MagicMock()
        merge_job = MagicMock()
        mock_bq_client.query.return_value = merge_job

        loader.upsert_rows([make_row()])

        mock_bq_client.load_table_from_json.assert_called_once()
        load_job.result.assert_called_once()
        merge_job.result.assert_called_once()

    def test_merge_sql_targets_source_items(self, loader, mock_bq_client):
        mock_bq_client.load_table_from_json.return_value = MagicMock()
        mock_bq_client.get_table.return_value = MagicMock()
        merge_job = MagicMock()
        mock_bq_client.query.return_value = merge_job

        loader.upsert_rows([make_row()])

        sql = mock_bq_client.query.call_args[0][0]
        assert "MERGE" in sql
        assert "source_items" in sql

    def test_deletes_staging_table_on_success(self, loader, mock_bq_client):
        mock_bq_client.load_table_from_json.return_value = MagicMock()
        mock_bq_client.get_table.return_value = MagicMock()
        mock_bq_client.query.return_value = MagicMock()

        loader.upsert_rows([make_row()])

        mock_bq_client.delete_table.assert_called_once()

    def test_deletes_staging_table_on_load_failure(self, loader, mock_bq_client):
        mock_bq_client.load_table_from_json.side_effect = RuntimeError("load failed")

        with pytest.raises(RuntimeError, match="load failed"):
            loader.upsert_rows([make_row()])

        mock_bq_client.delete_table.assert_called_once()

    def test_deletes_staging_table_on_merge_failure(self, loader, mock_bq_client):
        mock_bq_client.load_table_from_json.return_value = MagicMock()
        mock_bq_client.get_table.return_value = MagicMock()
        mock_bq_client.query.return_value.result.side_effect = RuntimeError("merge failed")

        with pytest.raises(RuntimeError, match="merge failed"):
            loader.upsert_rows([make_row()])

        mock_bq_client.delete_table.assert_called_once()

    def test_uses_unique_staging_table_per_call(self, loader, mock_bq_client):
        mock_bq_client.load_table_from_json.return_value = MagicMock()
        mock_bq_client.get_table.return_value = MagicMock()
        mock_bq_client.query.return_value = MagicMock()

        loader.upsert_rows([make_row("a")])
        loader.upsert_rows([make_row("b")])

        staging_tables = [
            call_args[0][1]
            for call_args in mock_bq_client.load_table_from_json.call_args_list
        ]
        assert staging_tables[0] != staging_tables[1]


class TestUpdateSyncState:
    def test_executes_merge_query(self, loader, mock_bq_client):
        source = {"id": 42, "name": "My Garmin", "type": "Garmin"}
        job = MagicMock()
        mock_bq_client.query.return_value = job

        loader.update_sync_state(source, last_synced_ms=9999)

        mock_bq_client.query.assert_called_once()
        sql = mock_bq_client.query.call_args[0][0]
        assert "MERGE" in sql
        assert "_sync_state" in sql
        job.result.assert_called_once()

    def test_passes_correct_parameters(self, loader, mock_bq_client):
        source = {"id": 42, "name": "My Garmin", "type": "Garmin"}
        mock_bq_client.query.return_value = MagicMock()

        loader.update_sync_state(source, last_synced_ms=9999)

        job_config = mock_bq_client.query.call_args[1]["job_config"]
        params = {p.name: p.value for p in job_config.query_parameters}
        assert params["source_id"] == "42"
        assert params["source_name"] == "My Garmin"
        assert params["source_type"] == "Garmin"
        assert params["last_synced_ms"] == 9999

    def test_source_id_cast_to_string(self, loader, mock_bq_client):
        source = {"id": 7, "name": "X", "type": "Y"}
        mock_bq_client.query.return_value = MagicMock()

        loader.update_sync_state(source, last_synced_ms=0)

        job_config = mock_bq_client.query.call_args[1]["job_config"]
        params = {p.name: p.value for p in job_config.query_parameters}
        assert params["source_id"] == "7"
        assert isinstance(params["source_id"], str)
