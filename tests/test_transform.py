from datetime import datetime, timezone

import pytest

from transform import _ms_to_iso, to_bq_row

MS_2024_JAN_1 = 1704067200000  # 2024-01-01T00:00:00Z
SYNCED_AT = datetime(2024, 6, 1, tzinfo=timezone.utc)


def make_source(id=42, type="Garmin", name="My Garmin"):
    return {"id": id, "type": type, "name": name, "enabled": True}


def make_item(item_id="abc123", date=MS_2024_JAN_1, **extra):
    return {"itemId": item_id, "date": date, "links": [], **extra}


class TestToBqRow:
    def test_maps_core_fields(self):
        row = to_bq_row(make_item(), make_source(), SYNCED_AT)
        assert row["item_id"] == "abc123"
        assert row["source_id"] == "42"
        assert row["source_type"] == "Garmin"
        assert row["date_ms"] == MS_2024_JAN_1
        assert row["date_utc"] == "2024-01-01T00:00:00+00:00"
        assert row["synced_at"] == "2024-06-01T00:00:00+00:00"

    def test_null_date_produces_none_fields(self):
        item = {**make_item(), "date": None}
        row = to_bq_row(item, make_source(), SYNCED_AT)
        assert row["date_ms"] is None
        assert row["date_utc"] is None

    def test_missing_date_produces_none_fields(self):
        item = {"itemId": "x", "links": []}
        row = to_bq_row(item, make_source(), SYNCED_AT)
        assert row["date_ms"] is None
        assert row["date_utc"] is None

    def test_extra_fields_captured(self):
        row = to_bq_row(make_item(calories=500, steps=10000), make_source(), SYNCED_AT)
        assert row["extra"] == {"calories": 500, "steps": 10000}

    def test_known_fields_excluded_from_extra(self):
        row = to_bq_row(make_item(), make_source(), SYNCED_AT)
        assert "itemId" not in row["extra"]
        assert "date" not in row["extra"]
        assert "links" not in row["extra"]

    def test_no_extra_fields_gives_empty_dict(self):
        row = to_bq_row(make_item(), make_source(), SYNCED_AT)
        assert row["extra"] == {}

    def test_source_id_cast_to_string(self):
        source = make_source(id=99)
        row = to_bq_row(make_item(), source, SYNCED_AT)
        assert row["source_id"] == "99"
        assert isinstance(row["source_id"], str)

    def test_missing_source_type_defaults_to_empty_string(self):
        source = {"id": 1, "name": "X"}
        row = to_bq_row(make_item(), source, SYNCED_AT)
        assert row["source_type"] == ""


class TestMsToIso:
    def test_epoch_zero(self):
        assert _ms_to_iso(0) == "1970-01-01T00:00:00+00:00"

    def test_known_timestamp(self):
        assert _ms_to_iso(MS_2024_JAN_1) == "2024-01-01T00:00:00+00:00"

    def test_sub_second_precision_truncated(self):
        result = _ms_to_iso(MS_2024_JAN_1 + 500)
        assert result.startswith("2024-01-01T00:00:00")
