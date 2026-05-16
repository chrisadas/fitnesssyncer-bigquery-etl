from unittest.mock import MagicMock, patch

import pytest

from client import FitnessSyncerClient, PAGE_SIZE


@pytest.fixture
def client():
    return FitnessSyncerClient("test-token")


def mock_response(data, status=200):
    resp = MagicMock()
    resp.ok = status < 400
    resp.status_code = status
    resp.json.return_value = data
    resp.url = "https://api.fitnesssyncer.com/api/test"
    resp.text = str(data)
    return resp


class TestListSources:
    def test_returns_enabled_sources(self, client):
        sources = [{"id": 1, "enabled": True}, {"id": 2, "enabled": False}]
        with patch.object(client._session, "get", return_value=mock_response({"items": sources})):
            result = client.list_sources()
        assert [s["id"] for s in result] == [1]

    def test_missing_enabled_key_defaults_to_included(self, client):
        sources = [{"id": 1}, {"id": 2, "enabled": False}]
        with patch.object(client._session, "get", return_value=mock_response({"items": sources})):
            result = client.list_sources()
        assert [s["id"] for s in result] == [1]

    def test_empty_items(self, client):
        with patch.object(client._session, "get", return_value=mock_response({"items": []})):
            assert client.list_sources() == []

    def test_raises_on_http_error(self, client):
        with patch.object(client._session, "get", return_value=mock_response({}, status=401)):
            with pytest.raises(RuntimeError, match="401"):
                client.list_sources()

    def test_raises_on_server_error(self, client):
        with patch.object(client._session, "get", return_value=mock_response({}, status=500)):
            with pytest.raises(RuntimeError, match="500"):
                client.list_sources()


class TestGetItems:
    def test_single_page_yields_all_items(self, client):
        items = [{"id": i} for i in range(5)]
        with patch.object(client._session, "get", return_value=mock_response({"items": items})):
            result = list(client.get_items("src1"))
        assert result == items

    def test_paginates_until_short_page(self, client):
        page1 = [{"id": i} for i in range(PAGE_SIZE)]
        page2 = [{"id": i} for i in range(3)]
        responses = [mock_response({"items": page1}), mock_response({"items": page2})]
        with patch.object(client._session, "get", side_effect=responses):
            result = list(client.get_items("src1"))
        assert len(result) == PAGE_SIZE + 3

    def test_offset_advances_on_pagination(self, client):
        page1 = [{"id": i} for i in range(PAGE_SIZE)]
        page2 = [{"id": i} for i in range(2)]
        responses = [mock_response({"items": page1}), mock_response({"items": page2})]
        with patch.object(client._session, "get", side_effect=responses) as mock_get:
            list(client.get_items("src1"))
        assert mock_get.call_args_list[0][1]["params"]["offset"] == 0
        assert mock_get.call_args_list[1][1]["params"]["offset"] == PAGE_SIZE

    def test_empty_source_yields_nothing(self, client):
        with patch.object(client._session, "get", return_value=mock_response({"items": []})):
            assert list(client.get_items("src1")) == []

    def test_start_ms_sets_date_params(self, client):
        with patch.object(client._session, "get", return_value=mock_response({"items": []})) as mock_get:
            list(client.get_items("src1", start_ms=1000))
        params = mock_get.call_args[1]["params"]
        assert params["startDate"] == 1000
        assert "endDate" in params

    def test_no_start_ms_omits_date_params(self, client):
        with patch.object(client._session, "get", return_value=mock_response({"items": []})) as mock_get:
            list(client.get_items("src1"))
        params = mock_get.call_args[1]["params"]
        assert "startDate" not in params
        assert "endDate" not in params

    def test_full_page_triggers_next_request(self, client):
        full_page = [{"id": i} for i in range(PAGE_SIZE)]
        empty_page = []
        responses = [mock_response({"items": full_page}), mock_response({"items": empty_page})]
        with patch.object(client._session, "get", side_effect=responses) as mock_get:
            list(client.get_items("src1"))
        assert mock_get.call_count == 2

    def test_short_page_stops_pagination(self, client):
        short_page = [{"id": i} for i in range(PAGE_SIZE - 1)]
        with patch.object(client._session, "get", return_value=mock_response({"items": short_page})) as mock_get:
            list(client.get_items("src1"))
        assert mock_get.call_count == 1
