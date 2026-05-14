import os
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

SOURCE_ITEMS_SCHEMA = [
    bigquery.SchemaField("item_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_type", "STRING"),
    bigquery.SchemaField("date_utc", "TIMESTAMP"),
    bigquery.SchemaField("date_ms", "INT64"),
    bigquery.SchemaField("extra", "JSON"),
    bigquery.SchemaField("synced_at", "TIMESTAMP", mode="REQUIRED"),
]

SYNC_STATE_SCHEMA = [
    bigquery.SchemaField("source_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_name", "STRING"),
    bigquery.SchemaField("source_type", "STRING"),
    bigquery.SchemaField("last_synced_ms", "INT64"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]


class BigQueryLoader:
    def __init__(self):
        self._client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
        self._dataset = os.environ["BQ_DATASET"]
        self._ensure_dataset()
        self._ensure_table("source_items", SOURCE_ITEMS_SCHEMA)
        self._ensure_table("_sync_state", SYNC_STATE_SCHEMA)

    def _ref(self, table: str) -> str:
        return f"{os.environ['GCP_PROJECT_ID']}.{self._dataset}.{table}"

    def _ensure_dataset(self) -> None:
        ds = bigquery.Dataset(f"{os.environ['GCP_PROJECT_ID']}.{self._dataset}")
        ds.location = "US"
        self._client.create_dataset(ds, exists_ok=True)

    def _ensure_table(self, name: str, schema: list) -> None:
        table = bigquery.Table(self._ref(name), schema=schema)
        self._client.create_table(table, exists_ok=True)

    def get_sync_state(self) -> dict[str, int]:
        """Return {source_id: last_synced_ms} for all known sources."""
        query = f"SELECT source_id, last_synced_ms FROM `{self._ref('_sync_state')}`"
        return {row.source_id: row.last_synced_ms for row in self._client.query(query)}

    def upsert_rows(self, rows: list[dict]) -> None:
        """Batch-load rows into a staging table then MERGE into source_items."""
        if not rows:
            return

        staging = self._ref(f"_staging_{uuid.uuid4().hex[:8]}")
        job_config = bigquery.LoadJobConfig(
            schema=SOURCE_ITEMS_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        try:
            load_job = self._client.load_table_from_json(rows, staging, job_config=job_config)
            load_job.result()

            staging_table = self._client.get_table(staging)
            staging_table.expires = datetime.now(tz=timezone.utc) + timedelta(hours=1)
            self._client.update_table(staging_table, ["expires"])

            merge_sql = f"""
                MERGE `{self._ref('source_items')}` T
                USING `{staging}` S ON T.item_id = S.item_id
                WHEN MATCHED THEN UPDATE SET
                    source_id = S.source_id,
                    source_type = S.source_type,
                    date_utc = S.date_utc,
                    date_ms = S.date_ms,
                    extra = S.extra,
                    synced_at = S.synced_at
                WHEN NOT MATCHED THEN INSERT ROW
            """
            self._client.query(merge_sql).result()
        finally:
            self._client.delete_table(staging, not_found_ok=True)

    def update_sync_state(self, source: dict, last_synced_ms: int) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        merge_sql = f"""
            MERGE `{self._ref('_sync_state')}` T
            USING (SELECT @source_id AS source_id) S ON T.source_id = S.source_id
            WHEN MATCHED THEN UPDATE SET
                source_name = @source_name,
                source_type = @source_type,
                last_synced_ms = @last_synced_ms,
                updated_at = @updated_at
            WHEN NOT MATCHED THEN INSERT
                (source_id, source_name, source_type, last_synced_ms, updated_at)
            VALUES
                (@source_id, @source_name, @source_type, @last_synced_ms, @updated_at)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source_id", "STRING", str(source["id"])),
                bigquery.ScalarQueryParameter("source_name", "STRING", source.get("name", "")),
                bigquery.ScalarQueryParameter("source_type", "STRING", source.get("type", "")),
                bigquery.ScalarQueryParameter("last_synced_ms", "INT64", last_synced_ms),
                bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", now),
            ]
        )
        self._client.query(merge_sql, job_config=job_config).result()
