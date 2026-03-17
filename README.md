# Scout

Real-time Slack ingestion and search engine data pipeline.

## Slack Listener

The `slack_listener` module provides two ingestion modes:

- **Polling** (dev) — uses a user token (`xoxp-` / `xoxe.xoxp-`), no Slack app needed
- **Socket Mode** (production) — real-time events via WebSocket, requires a Slack app

## Quick start

### 1. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install .
```

### 2. Configure

Copy the example and add your Slack token:

```bash
cp .env.example .env
```

Edit `.env` and set `SLACK_TOKEN` to your user token (`xoxp-...` or `xoxe.xoxp-...`).
The defaults are ready for local dev — poll mode, local JSONL storage, no backfill.

### 3. Run

```bash
scout-slack-listener
```

The service will:
- Auto-discover channels you're a member of
- Poll every 5 seconds for new messages
- Write to `./data/slack_messages.jsonl`
- Download PDF/DOCX/PPTX attachments to `./data/files/`
- Track progress in `./data/watermarks.json` (resume after restart)

Stop with `Ctrl+C`.

## Project structure

```
scout/
├── pyproject.toml
├── Dockerfile
├── README.md
├── .env.example
└── scout/
    ├── __init__.py
    └── slack_listener/
        ├── __init__.py
        ├── __main__.py            # python -m scout.slack_listener
        ├── app.py                 # entry point, .env loader, mode router
        ├── config.py              # env-var configuration
        ├── transforms.py          # message → document schema
        ├── user_cache.py          # user_id → name resolution
        ├── file_downloader.py     # attachment downloads
        ├── storage/
        │   ├── base.py            # StorageWriter ABC
        │   ├── factory.py         # get_writer()
        │   ├── local.py           # JSONL on disk
        │   ├── delta.py           # Delta Lake via Spark / SQL Connector
        │   └── opensearch.py      # OpenSearch index
        └── ingestion/
            ├── poller.py          # polling mode (dev)
            └── socket_handler.py  # Socket Mode (production)
```

## Configuration

The `.env` file is loaded automatically on startup. Environment variables
set in the shell take precedence over `.env` values.

| Variable | Default | Description |
|---|---|---|
| `INGEST_MODE` | `poll` | `poll` or `socket` |
| `SLACK_TOKEN` | *(required)* | `xoxp-` / `xoxe.xoxp-` (poll) or `xoxb-` (socket) |
| `SLACK_APP_TOKEN` | | `xapp-` token (socket mode only) |
| `POLL_INTERVAL` | `5` | Seconds between polls |
| `SLACK_START_FROM` | | History floor: ISO date (`2025-03-01`), `now`, or epoch. Prevents full backfill on first run |
| `SLACK_CHANNEL_IDS` | | Comma-separated channel IDs (auto-discovers if empty) |
| `STORAGE_BACKEND` | `local` | `local`, `delta`, or `opensearch` |
| `LOCAL_DATA_DIR` | `./data` | Directory for JSONL, files, and watermarks |
| `DELTA_TABLE_NAME` | `agentsearch.default.slack_messages` | Delta table name |
| `DATABRICKS_HOST` | | Workspace URL (standalone delta) |
| `DATABRICKS_TOKEN` | | PAT (standalone delta) |
| `DATABRICKS_HTTP_PATH` | | SQL warehouse path (standalone delta) |
| `FILE_STORAGE_BACKEND` | `local` | `local`, `databricks_volume`, or `object_store` |
| `SLACK_FILES_PATH` | `/Volumes/.../slack_files/` | Databricks volume path |
| `OPENSEARCH_HOST` | `localhost` | OpenSearch host |
| `OPENSEARCH_PORT` | `9200` | OpenSearch port |
| `BATCH_FLUSH_SECONDS` | `2` | Micro-batch flush interval (socket mode) |
| `BATCH_MAX_SIZE` | `100` | Micro-batch max size (socket mode) |

## Install with optional backends

```bash
pip install .                  # core only (local storage)
pip install ".[delta]"         # + Delta Lake support
pip install ".[opensearch]"    # + OpenSearch support
pip install ".[all]"           # everything
```

## Running

```bash
# Simplest — uses .env, polls, writes to ./data/
scout-slack-listener

# Override a setting inline
POLL_INTERVAL=10 scout-slack-listener

# Specific channels only
SLACK_CHANNEL_IDS=C01ABC123,C02DEF456 scout-slack-listener

# Via module
python -m scout.slack_listener
```

## First run behavior

| `SLACK_START_FROM` | Watermarks exist? | What happens |
|---|---|---|
| `now` | No | Starts capturing from this moment, no backfill |
| `2025-03-01` | No | Pulls messages from March 1 onward |
| *(empty)* | No | Full history pull (can be slow/large) |
| *(any)* | Yes | Resumes from saved watermark (always wins) |

Watermarks are saved after each successful write. In local mode they
live at `./data/watermarks.json`. In delta/opensearch mode they are
derived from the data itself and always survive redeployment.

## Docker

```bash
docker build -t scout .
docker run --env-file .env scout
```

## Production setup (Socket Mode)

1. Create a Slack app at https://api.slack.com/apps
2. Add bot scopes: `channels:history`, `channels:read`, `users:read`
3. Enable Socket Mode — generates the `xapp-` token
4. Subscribe to bot events: `message.channels`
5. Install the app and invite the bot to channels (`/invite @BotName`)

```bash
INGEST_MODE=socket \
  SLACK_TOKEN=xoxb-... \
  SLACK_APP_TOKEN=xapp-... \
  STORAGE_BACKEND=delta \
  scout-slack-listener
```

The bot must be a member of each channel it listens to. Use `channels:join`
scope for auto-join, or invite manually. Private channels always require
manual invite.
