import time
from typing import Generator
import requests

BASE_URL = "https://api.fitnesssyncer.com/api"
PAGE_SIZE = 100


class FitnessSyncerClient:
    def __init__(self, access_token: str):
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {access_token}"

    def _get(self, path: str, **params) -> dict:
        resp = self._session.get(f"{BASE_URL}{path}", params=params, timeout=30)
        if not resp.ok:
            raise RuntimeError(
                f"GET {resp.url} → {resp.status_code}: {resp.text}"
            )
        return resp.json()

    def list_sources(self) -> list[dict]:
        data = self._get("/providers/sources/")
        return [s for s in data.get("items", []) if s.get("enabled", True)]

    def get_items(
        self,
        source_id: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> Generator[dict, None, None]:
        """Yield all items for a source, paginating automatically."""
        params = {"limit": PAGE_SIZE, "offset": 0}
        if start_ms is not None:
            params["startDate"] = start_ms
            params["endDate"] = end_ms if end_ms is not None else int(time.time() * 1000)
        elif end_ms is not None:
            params["endDate"] = end_ms

        while True:
            data = self._get(f"/providers/sources/{source_id}/items/", **params)
            items = data.get("items", [])
            yield from items
            if len(items) < PAGE_SIZE:
                break
            params["offset"] += PAGE_SIZE
