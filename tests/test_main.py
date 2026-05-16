from unittest.mock import MagicMock, call, patch

import pytest

import main as main_mod
from main import run


@pytest.fixture(autouse=True)
def env_vars(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("BQ_DATASET", "test_dataset")


def make_source(id=1, name="Garmin", type="Garmin"):
    return {"id": id, "name": name, "type": type, "enabled": True}


def make_item(item_id="item1", date_ms=1000):
    return {"itemId": item_id, "date": date_ms, "links": []}


@pytest.fixture
def mock_deps():
    with patch("main.get_access_token", return_value="token"), \
         patch("main.FitnessSyncerClient") as mock_client_cls, \
         patch("main.BigQueryLoader") as mock_loader_cls:
        mock_client = mock_client_cls.return_value
        mock_loader = mock_loader_cls.return_value
        mock_loader.get_sync_state.return_value = {}
        yield mock_client, mock_loader


class TestRunHappyPath:
    def test_loads_items_for_each_source(self, mock_deps):
        mock_client, mock_loader = mock_deps
        mock_client.list_sources.return_value = [make_source()]
        mock_client.get_items.return_value = iter([make_item("x", 1000)])

        run()

        mock_loader.upsert_rows.assert_called()

    def test_updates_sync_state_with_max_date(self, mock_deps):
        mock_client, mock_loader = mock_deps
        source = make_source()
        mock_client.list_sources.return_value = [source]
        mock_client.get_items.return_value = iter([
            make_item("a", 1000),
            make_item("b", 3000),
            make_item("c", 2000),
        ])

        run()

        mock_loader.update_sync_state.assert_called_once_with(source, 3000)

    def test_no_items_skips_upsert_and_state_update(self, mock_deps):
        mock_client, mock_loader = mock_deps
        mock_client.list_sources.return_value = [make_source()]
        mock_client.get_items.return_value = iter([])

        run()

        mock_loader.upsert_rows.assert_not_called()
        mock_loader.update_sync_state.assert_not_called()

    def test_uses_last_ms_plus_one_as_start(self, mock_deps):
        mock_client, mock_loader = mock_deps
        mock_client.list_sources.return_value = [make_source(id=1)]
        mock_client.get_items.return_value = iter([make_item("x", 5000)])
        mock_loader.get_sync_state.return_value = {"1": 3000}

        run()

        mock_client.get_items.assert_called_once_with("1", start_ms=3001)

    def test_no_prior_state_passes_no_start_ms(self, mock_deps):
        mock_client, mock_loader = mock_deps
        mock_client.list_sources.return_value = [make_source(id=1)]
        mock_client.get_items.return_value = iter([make_item("x", 1000)])
        mock_loader.get_sync_state.return_value = {}

        run()

        mock_client.get_items.assert_called_once_with("1", start_ms=None)

    def test_processes_multiple_sources_independently(self, mock_deps):
        mock_client, mock_loader = mock_deps
        sources = [make_source(id=1, name="A"), make_source(id=2, name="B")]
        mock_client.list_sources.return_value = sources
        mock_client.get_items.side_effect = [
            iter([make_item("a", 1000)]),
            iter([make_item("b", 2000)]),
        ]

        run()

        assert mock_loader.update_sync_state.call_count == 2


class TestRunBatching:
    def test_flushes_full_batches(self, mock_deps, monkeypatch):
        monkeypatch.setattr(main_mod, "BATCH_SIZE", 2)
        mock_client, mock_loader = mock_deps
        mock_client.list_sources.return_value = [make_source()]
        mock_client.get_items.return_value = iter([
            make_item("a", 1000),
            make_item("b", 2000),
            make_item("c", 3000),
        ])

        run()

        assert mock_loader.upsert_rows.call_count == 2  # batch of 2, then batch of 1

    def test_flushes_final_partial_batch(self, mock_deps, monkeypatch):
        monkeypatch.setattr(main_mod, "BATCH_SIZE", 10)
        mock_client, mock_loader = mock_deps
        mock_client.list_sources.return_value = [make_source()]
        mock_client.get_items.return_value = iter([make_item("a", 1000)])

        run()

        mock_loader.upsert_rows.assert_called_once()


class TestRunEdgeCases:
    def test_items_with_null_date_do_not_advance_watermark(self, mock_deps):
        mock_client, mock_loader = mock_deps
        source = make_source()
        mock_client.list_sources.return_value = [source]
        mock_client.get_items.return_value = iter([
            make_item("a", 5000),
            {"itemId": "b", "date": None, "links": []},
        ])
        mock_loader.get_sync_state.return_value = {"1": 5000}

        run()

        mock_loader.update_sync_state.assert_called_once_with(source, 5000)

    def test_no_sources_does_nothing(self, mock_deps):
        mock_client, mock_loader = mock_deps
        mock_client.list_sources.return_value = []

        run()

        mock_loader.upsert_rows.assert_not_called()
        mock_loader.update_sync_state.assert_not_called()
