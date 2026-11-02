# AdPulse

**Reliable data. Clear decisions.**

AdPulse is a local API data reliability and marketing intelligence demonstration. Google Ads, Meta Ads, and LinkedIn Ads are **simulated providers**. Every returned record is deterministic demonstration data; no provider credentials, accounts, paid APIs, or internet connection are required for the core workflow.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

The Streamlit process starts the FastAPI simulator on `127.0.0.1:8765` automatically. The UI is served by Streamlit's default local port. SQLite persists to `adpulse.sqlite3` in this project directory; set `ADPULSE_DB` to choose another location. Freshness is configurable with `ADPULSE_FRESHNESS_DAYS` (default: 3 calendar days).

## Demo workflows

- **Overview / Pipeline Operations:** run all providers for a reporting date. Each provider returns seven deterministic campaigns over two pages. Repeating a run inserts no duplicates; it updates matching records with a new `updated_at` timestamp. Choose “Modified dataset” to change campaign 1 metrics and see UPSERT updates.
- **Failure Injection Lab:** configure a platform, response scenario, failures before recovery, retry count, and backoff. Requests are real GET calls to the local simulator; 429 and 503 include `Retry-After`. 400/403 are terminal. Timeout injection delays the local response beyond the client's request timeout, then permits subsequent requests.
- **Database Explorer / Execution History:** inspect stored records, rejected payloads, every pipeline run, and individual HTTP attempts.
- **Marketing Analytics:** view spend, CTR, CPA and reporting trends from SQLite records.

## Architecture

`adpulse/providers.py` contains the three FastAPI response shapes and controllable failure state. `adpulse/etl.py` owns extraction/pagination/retry, normalization, validation, and orchestration. `adpulse/db.py` creates SQLite tables, persists run/request/rejection history, and performs transactional parameterized UPSERTs. `app.py` is a Streamlit presentation layer over those persisted results.

SQLite is suitable for this single-user local demonstration. PostgreSQL is a sensible production choice when concurrent workloads and larger data volumes require it. Production provider integrations would replace local endpoints with authenticated connectors; token expiry handling would need a credential refresh flow. Freshness is considered fresh when the latest reporting date is within the configured calendar-day threshold.
