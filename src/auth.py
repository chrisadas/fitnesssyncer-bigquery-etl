import os
import requests
from google.cloud import storage

TOKEN_URL = "https://api.fitnesssyncer.com/api/oauth/access_token"


def _gcs_client():
    return storage.Client(project=os.environ["GCP_PROJECT_ID"])


def _read_refresh_token() -> str:
    bucket = _gcs_client().bucket(os.environ["GCS_TOKEN_BUCKET"])
    return bucket.blob(os.environ["GCS_TOKEN_OBJECT"]).download_as_text().strip()


def _write_refresh_token(token: str) -> None:
    bucket = _gcs_client().bucket(os.environ["GCS_TOKEN_BUCKET"])
    bucket.blob(os.environ["GCS_TOKEN_OBJECT"]).upload_from_string(token)


def get_access_token() -> str:
    """Exchange the stored refresh token for a new access token, persisting the rotated refresh token."""
    refresh_token = _read_refresh_token()
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": os.environ["FITNESSSYNCER_CLIENT_ID"],
            "client_secret": os.environ["FITNESSSYNCER_CLIENT_SECRET"],
            "redirect_uri": "https://personal.fitnesssyncer.com/",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    _write_refresh_token(data["refresh_token"])
    return data["access_token"]
