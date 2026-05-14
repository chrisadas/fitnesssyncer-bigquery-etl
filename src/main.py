import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

from auth import get_access_token
from client import FitnessSyncerClient
from load import BigQueryLoader
from transform import to_bq_row

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BATCH_SIZE = 500


def run() -> None:
    log.info("Starting FitnessSyncer ETL")
    access_token = get_access_token()
    client = FitnessSyncerClient(access_token)
    loader = BigQueryLoader()

    sync_state = loader.get_sync_state()
    sources = client.list_sources()
    log.info("Found %d enabled sources", len(sources))

    synced_at = datetime.now(tz=timezone.utc)

    for source in sources:
        source_id = str(source["id"])
        last_ms = sync_state.get(source_id)
        start_ms = (last_ms + 1) if last_ms is not None else None

        log.info(
            "Syncing source %s (%s) from %s",
            source.get("name"),
            source.get("type"),
            last_ms or "beginning",
        )

        batch: list[dict] = []
        max_date_ms = last_ms or 0
        total = 0

        for item in client.get_items(source_id, start_ms=start_ms):
            row = to_bq_row(item, source, synced_at)
            batch.append(row)
            if row["date_ms"] and row["date_ms"] > max_date_ms:
                max_date_ms = row["date_ms"]

            if len(batch) >= BATCH_SIZE:
                loader.upsert_rows(batch)
                total += len(batch)
                batch = []

        if batch:
            loader.upsert_rows(batch)
            total += len(batch)

        if total > 0:
            loader.update_sync_state(source, max_date_ms)
            log.info("Loaded %d items for source %s", total, source.get("name"))
        else:
            log.info("No new items for source %s", source.get("name"))

    log.info("ETL complete")


if __name__ == "__main__":
    run()
