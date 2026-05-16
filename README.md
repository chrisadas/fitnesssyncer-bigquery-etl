# FitnessSyncer → BigQuery ETL

Incremental ETL pipeline that pulls fitness data from [FitnessSyncer](https://www.fitnesssyncer.com) and loads it into Google BigQuery for analytics. Runs on a daily GitHub Actions schedule.

## What it does

- Pulls all enabled data sources (activities, sleep, weight, etc.) via FitnessSyncer's OAuth2 API
- Loads items into a single `source_items` table in BigQuery with type-specific fields in a JSON column
- Tracks per-source sync state for incremental updates — only fetches new items after the first run
- Idempotent: re-running is safe (MERGE on `item_id` prevents duplicates)

## Architecture

```
GitHub Actions (daily cron)
  └─ src/main.py
       ├─ auth.py       → POST /api/oauth/access_token (refresh_token grant)
       ├─ client.py     → GET /api/providers/sources/
       │                → GET /api/providers/sources/{id}/items/ (paginated)
       ├─ transform.py  → Normalize to BigQuery row dicts
       └─ load.py       → Load to staging → MERGE into source_items
                        → Update _sync_state per source
```

The FitnessSyncer refresh token rotates on every use. To make this work in CI, the live refresh token is stored in a GCS object that the pipeline reads at start and writes back after each refresh.

## BigQuery schema

**`source_items`** — single table for all data types:

| Column | Type | Notes |
|--------|------|-------|
| `item_id` | STRING | Primary key (from FitnessSyncer `itemId`) |
| `source_id` | STRING | Source the item came from |
| `source_type` | STRING | `Activity`, `Weight`, `Sleep`, `Temperature`, etc. |
| `date_utc` | TIMESTAMP | Item timestamp |
| `date_ms` | INT64 | Raw epoch ms |
| `extra` | JSON | Type-specific fields (distance, calories, weight_kg, etc.) |
| `synced_at` | TIMESTAMP | When the row was loaded |

**`_sync_state`** — incremental position per source.

## Setup

### Prerequisites

- A FitnessSyncer account with linked data sources
- A GCP project with billing enabled
- A GCS bucket (for storing the refresh token)
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

### 1. Register a FitnessSyncer OAuth app

1. Go to https://www.fitnesssyncer.com/account/developer/app
2. Set redirect URL to `https://personal.fitnesssyncer.com/`
3. Enable PKCE
4. Save the Client ID and Client Secret (the secret is only shown once)

### 2. Provision GCP resources

```bash
PROJECT_ID=your-project-id
SA_NAME=fitnesssyncer-etl
SA_EMAIL=${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com
BUCKET=your-token-bucket

# Create GCS bucket
gcloud storage buckets create gs://$BUCKET --project=$PROJECT_ID --location=US

# Create service account
gcloud iam service-accounts create $SA_NAME --project=$PROJECT_ID \
  --display-name="FitnessSyncer ETL"

# Grant permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.jobUser"
gcloud storage buckets add-iam-policy-binding gs://$BUCKET \
  --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin"

# Generate a key for CI
gcloud iam service-accounts keys create sa-key.json --iam-account=$SA_EMAIL
```

### 3. Local setup

```bash
git clone https://github.com/chrisadas/fitnesssyncer-bigquery-etl
cd fitnesssyncer-bigquery-etl

uv venv && source .venv/bin/activate
uv pip install -r requirements.txt

cp .env.example .env
# Edit .env: fill in CLIENT_ID, CLIENT_SECRET, GCS_TOKEN_BUCKET, GCP_PROJECT_ID

gcloud auth application-default login   # for local GCP access
python setup_auth.py                     # one-time OAuth flow
python src/main.py                       # run the ETL
```

`setup_auth.py` prints an authorization URL. Open it in a browser, authorize, copy the redirect URL from the browser's address bar (it contains `?code=...`), and paste it into the prompt.

### 4. GitHub Actions setup

Add these to the repo (Settings → Secrets and variables → Actions):

**Secrets:**
- `FITNESSSYNCER_CLIENT_ID`
- `FITNESSSYNCER_CLIENT_SECRET`
- `GCS_TOKEN_BUCKET`
- `GCP_PROJECT_ID`
- `GCP_CREDENTIALS_JSON` (contents of `sa-key.json`)

**Variables:**
- `BQ_DATASET` (e.g. `fitnesssyncer`)

Then trigger the workflow manually from the Actions tab to verify, or wait for the daily 06:00 UTC cron.

After setup, delete the local `sa-key.json` — it's covered by `.gitignore` but no reason to keep it around.

## Querying

```sql
-- Item counts by type
SELECT source_type, COUNT(*) AS items
FROM `PROJECT.fitnesssyncer.source_items`
GROUP BY source_type
ORDER BY items DESC;

-- Inspect available fields for a type
SELECT extra
FROM `PROJECT.fitnesssyncer.source_items`
WHERE source_type = 'Activity'
ORDER BY date_utc DESC
LIMIT 1;

-- Sleep stages per night (minutes), per source
SELECT
  DATE(TIMESTAMP_MILLIS(CAST(JSON_VALUE(s.extra, '$.bedTime') AS INT64))) AS night,
  st.source_name,
  ROUND(CAST(JSON_VALUE(s.extra, '$.lightSleepMinutes') AS FLOAT64), 1) AS light_min,
  ROUND(CAST(JSON_VALUE(s.extra, '$.deepSleepMinutes')  AS FLOAT64), 1) AS deep_min,
  ROUND(CAST(JSON_VALUE(s.extra, '$.remSleepMinutes')   AS FLOAT64), 1) AS rem_min,
  ROUND(CAST(JSON_VALUE(s.extra, '$.awakeMinutes')      AS FLOAT64), 1) AS awake_min,
  ROUND(
    CAST(JSON_VALUE(s.extra, '$.lightSleepMinutes') AS FLOAT64)
    + CAST(JSON_VALUE(s.extra, '$.deepSleepMinutes') AS FLOAT64)
    + CAST(JSON_VALUE(s.extra, '$.remSleepMinutes')  AS FLOAT64), 1
  ) AS total_sleep_min
FROM `PROJECT.fitnesssyncer.source_items` AS s
LEFT JOIN `PROJECT.fitnesssyncer._sync_state` AS st ON s.source_id = st.source_id
WHERE s.source_type = 'Sleep'
ORDER BY night DESC, st.source_name;

-- Sync state per source
SELECT source_name, source_type,
       TIMESTAMP_MILLIS(last_synced_ms) AS last_synced,
       updated_at
FROM `PROJECT.fitnesssyncer._sync_state`
ORDER BY updated_at DESC;
```

## Project layout

```
src/
  auth.py        # OAuth2 token refresh, GCS-backed refresh token storage
  client.py      # FitnessSyncer API client + paginated item generator
  transform.py   # Item → BigQuery row normalization
  load.py        # BigQuery dataset/tables, staging + MERGE, state mgmt
  main.py        # Orchestrates the pipeline
setup_auth.py    # One-time interactive OAuth flow
.github/workflows/etl.yml    # Daily cron + manual trigger
```
