from unittest.mock import MagicMock, patch

import pytest
import requests as req


@pytest.fixture(autouse=True)
def env_vars(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("GCS_TOKEN_BUCKET", "test-bucket")
    monkeypatch.setenv("GCS_TOKEN_OBJECT", "token.txt")
    monkeypatch.setenv("FITNESSSYNCER_CLIENT_ID", "client-id")
    monkeypatch.setenv("FITNESSSYNCER_CLIENT_SECRET", "client-secret")


def make_gcs_mock(refresh_token="old-refresh-token"):
    blob = MagicMock()
    blob.download_as_text.return_value = f"{refresh_token}\n"
    bucket = MagicMock()
    bucket.blob.return_value = blob
    gcs = MagicMock()
    gcs.bucket.return_value = bucket
    return gcs, blob


def make_token_response(access_token="new-access", refresh_token="new-refresh"):
    resp = MagicMock()
    resp.json.return_value = {"access_token": access_token, "refresh_token": refresh_token}
    return resp


class TestGetAccessToken:
    def test_returns_access_token(self):
        from auth import get_access_token

        gcs, _ = make_gcs_mock()
        with patch("auth.storage.Client", return_value=gcs), \
             patch("auth.requests.post", return_value=make_token_response("my-access")):
            token = get_access_token()

        assert token == "my-access"

    def test_sends_stored_refresh_token(self):
        from auth import get_access_token

        gcs, _ = make_gcs_mock("stored-refresh-token")
        with patch("auth.storage.Client", return_value=gcs), \
             patch("auth.requests.post", return_value=make_token_response()) as mock_post:
            get_access_token()

        data = mock_post.call_args[1]["data"]
        assert data["refresh_token"] == "stored-refresh-token"
        assert data["grant_type"] == "refresh_token"

    def test_persists_rotated_refresh_token(self):
        from auth import get_access_token

        gcs, blob = make_gcs_mock()
        with patch("auth.storage.Client", return_value=gcs), \
             patch("auth.requests.post", return_value=make_token_response(refresh_token="rotated-token")):
            get_access_token()

        blob.upload_from_string.assert_called_once_with("rotated-token")

    def test_strips_whitespace_from_stored_token(self):
        from auth import get_access_token

        gcs, _ = make_gcs_mock("token-with-newline\n")
        with patch("auth.storage.Client", return_value=gcs), \
             patch("auth.requests.post", return_value=make_token_response()) as mock_post:
            get_access_token()

        data = mock_post.call_args[1]["data"]
        assert data["refresh_token"] == "token-with-newline"

    def test_raises_on_http_error(self):
        from auth import get_access_token

        gcs, _ = make_gcs_mock()
        bad_resp = MagicMock()
        bad_resp.raise_for_status.side_effect = req.HTTPError("401 Unauthorized")
        with patch("auth.storage.Client", return_value=gcs), \
             patch("auth.requests.post", return_value=bad_resp):
            with pytest.raises(req.HTTPError):
                get_access_token()

    def test_includes_client_credentials(self):
        from auth import get_access_token

        gcs, _ = make_gcs_mock()
        with patch("auth.storage.Client", return_value=gcs), \
             patch("auth.requests.post", return_value=make_token_response()) as mock_post:
            get_access_token()

        data = mock_post.call_args[1]["data"]
        assert data["client_id"] == "client-id"
        assert data["client_secret"] == "client-secret"
