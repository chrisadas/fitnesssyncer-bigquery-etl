import json
from datetime import datetime, timezone

KNOWN_FIELDS = {"itemId", "date", "links"}


def to_bq_row(item: dict, source: dict, synced_at: datetime) -> dict:
    date_ms = item.get("date")
    extra = {k: v for k, v in item.items() if k not in KNOWN_FIELDS}

    return {
        "item_id": item["itemId"],
        "source_id": str(source["id"]),
        "source_type": source.get("type", ""),
        "date_utc": _ms_to_iso(date_ms) if date_ms is not None else None,
        "date_ms": date_ms,
        "extra": json.dumps(extra),
        "synced_at": synced_at.isoformat(),
    }


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
