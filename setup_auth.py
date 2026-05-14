"""
One-time OAuth2 PKCE setup. Run this locally to bootstrap credentials:

    python setup_auth.py

After authorizing in the browser, the redirect will fail to load (connection
refused) — that's expected. Copy the full URL from the browser's address bar
and paste it into the terminal prompt.

Requires .env (copy from .env.example) with CLIENT_ID, CLIENT_SECRET,
GCS_TOKEN_BUCKET, GCS_TOKEN_OBJECT, and GCP_PROJECT_ID filled in.

The redirect_uri used matches the one registered in the FitnessSyncer developer
portal: https://personal.fitnesssyncer.com/
"""

import base64
import hashlib
import os
import secrets
import urllib.parse

import requests
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

AUTH_URL = "https://www.fitnesssyncer.com/api/oauth/authorize"
TOKEN_URL = "https://api.fitnesssyncer.com/api/oauth/access_token"
REDIRECT_URI = "https://personal.fitnesssyncer.com/"


def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


def _build_auth_url(challenge: str, state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": os.environ["FITNESSSYNCER_CLIENT_ID"].strip().strip('"').strip("'"),
        "redirect_uri": REDIRECT_URI,
        "scope": "source_read destination_read sources source_data_read",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    return f"{AUTH_URL}?{urllib.parse.urlencode(params)}"


def _parse_callback_url(raw: str) -> dict:
    parsed = urllib.parse.urlparse(raw.strip())
    qs = urllib.parse.parse_qs(parsed.query)
    return {
        "code": qs.get("code", [None])[0],
        "state": qs.get("state", [None])[0],
    }


def _exchange_code(code: str, verifier: str) -> dict:
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": os.environ["FITNESSSYNCER_CLIENT_ID"].strip().strip('"').strip("'"),
            "client_secret": os.environ["FITNESSSYNCER_CLIENT_SECRET"],
            "redirect_uri": REDIRECT_URI,
            "code_verifier": verifier,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _write_to_gcs(token: str) -> None:
    client = storage.Client(project=os.environ["GCP_PROJECT_ID"])
    bucket = client.bucket(os.environ["GCS_TOKEN_BUCKET"])
    bucket.blob(os.environ["GCS_TOKEN_OBJECT"]).upload_from_string(token)
    print(
        f"Refresh token written to gs://{os.environ['GCS_TOKEN_BUCKET']}/"
        f"{os.environ['GCS_TOKEN_OBJECT']}"
    )


def _check_gcs_access() -> None:
    """Fail early if we can't write to GCS — avoids wasting an OAuth flow."""
    client = storage.Client(project=os.environ["GCP_PROJECT_ID"])
    bucket = client.bucket(os.environ["GCS_TOKEN_BUCKET"])
    if not bucket.exists():
        raise RuntimeError(
            f"GCS bucket gs://{bucket.name} not found or no access. "
            "Check GCS_TOKEN_BUCKET and run `gcloud auth application-default login`."
        )


def main():
    _check_gcs_access()
    verifier, challenge = _pkce_pair()
    state = secrets.token_urlsafe(16)
    url = _build_auth_url(challenge, state)

    print("\nGenerated authorization URL:")
    print(f"\n  {url}\n")
    print("Parameters:")
    for k, v in urllib.parse.parse_qs(urllib.parse.urlparse(url).query).items():
        print(f"  {k} = {v[0]}")
    print(
        "After you authorize, you'll be redirected to personal.fitnesssyncer.com."
        "\nCopy the full URL from the address bar (it will contain ?code=...) and paste it here.\n"
    )

    raw = input("Paste the redirect URL: ").strip()
    callback = _parse_callback_url(raw)

    if not callback.get("code"):
        raise RuntimeError("No 'code' found in the URL — did you copy the full address bar URL?")
    if callback.get("state") != state:
        raise RuntimeError("State mismatch — possible CSRF, or you pasted an old URL")

    tokens = _exchange_code(callback["code"], verifier)
    _write_to_gcs(tokens["refresh_token"])
    print("Setup complete. The pipeline is ready to run.")


if __name__ == "__main__":
    main()
