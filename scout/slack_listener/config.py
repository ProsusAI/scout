"""
Configuration for the Slack listener service.
All values are read from environment variables with sensible defaults.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    # Ingestion mode
    ingest_mode: str = "poll"  # "poll" or "socket"

    # Slack credentials
    slack_token: str = ""       # xoxp- (poll) or xoxb- (socket)
    slack_app_token: str = ""   # xapp- (socket mode only)
    poll_interval: float = 30.0
    slack_channel_ids: list[str] = field(default_factory=list)

    # Storage backend
    storage_backend: str = "local"  # "local", "delta", or "opensearch"

    # Local file storage
    local_data_dir: str = "./data"

    # Delta Lake
    delta_table_name: str = "agentsearch.default.slack_messages"
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_http_path: str = ""

    # File storage
    file_storage_backend: str = "local"  # "local", "databricks_volume", or "object_store"
    slack_files_path: str = "/Volumes/agentsearch/default/slack_files/"
    object_store_prefix: str = ""

    # OpenSearch
    opensearch_host: str = "localhost"
    opensearch_port: int = 9200

    # History bounds
    start_from: str = ""  # ISO date (2025-01-01), epoch ts, or "now" to skip backfill

    # Batching
    batch_flush_seconds: float = 2.0
    batch_max_size: int = 100

    @classmethod
    def from_env(cls) -> Config:
        channel_ids_raw = os.environ.get("SLACK_CHANNEL_IDS", "")
        channel_ids = [c.strip() for c in channel_ids_raw.split(",") if c.strip()]

        return cls(
            ingest_mode=os.environ.get("INGEST_MODE", "poll").lower(),
            slack_token=os.environ.get("SLACK_TOKEN", ""),
            slack_app_token=os.environ.get("SLACK_APP_TOKEN", ""),
            poll_interval=float(os.environ.get("POLL_INTERVAL", "30")),
            slack_channel_ids=channel_ids,
            storage_backend=os.environ.get("STORAGE_BACKEND", "local").lower(),
            local_data_dir=os.environ.get("LOCAL_DATA_DIR", "./data"),
            delta_table_name=os.environ.get("DELTA_TABLE_NAME", "agentsearch.default.slack_messages"),
            databricks_host=os.environ.get("DATABRICKS_HOST", ""),
            databricks_token=os.environ.get("DATABRICKS_TOKEN", ""),
            databricks_http_path=os.environ.get("DATABRICKS_HTTP_PATH", ""),
            file_storage_backend=os.environ.get("FILE_STORAGE_BACKEND", "local").lower(),
            slack_files_path=os.environ.get("SLACK_FILES_PATH", "/Volumes/agentsearch/default/slack_files/"),
            object_store_prefix=os.environ.get("OBJECT_STORE_PREFIX", ""),
            opensearch_host=os.environ.get("OPENSEARCH_HOST", "localhost"),
            opensearch_port=int(os.environ.get("OPENSEARCH_PORT", "9200")),
            start_from=os.environ.get("SLACK_START_FROM", ""),
            batch_flush_seconds=float(os.environ.get("BATCH_FLUSH_SECONDS", "2")),
            batch_max_size=int(os.environ.get("BATCH_MAX_SIZE", "100")),
        )

    def validate(self) -> None:
        if not self.slack_token:
            raise ValueError("SLACK_TOKEN is required")
        if self.ingest_mode == "socket" and not self.slack_app_token:
            raise ValueError("SLACK_APP_TOKEN is required for socket mode")
        if self.storage_backend == "delta" and not self._has_spark():
            if not all([self.databricks_host, self.databricks_token, self.databricks_http_path]):
                raise ValueError(
                    "Delta storage outside Databricks requires DATABRICKS_HOST, "
                    "DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH"
                )

    @property
    def start_from_ts(self) -> str | None:
        """Resolve SLACK_START_FROM into a Slack-compatible epoch timestamp string."""
        val = self.start_from.strip()
        if not val:
            return None
        if val.lower() == "now":
            import time
            return str(time.time())
        try:
            return str(float(val))
        except ValueError:
            pass
        from datetime import datetime, timezone
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                dt = datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
                return str(dt.timestamp())
            except ValueError:
                continue
        raise ValueError(
            f"SLACK_START_FROM='{val}' is not valid. "
            "Use ISO date (2025-01-01), datetime (2025-01-01T00:00:00), epoch, or 'now'."
        )

    @property
    def files_dir(self) -> str:
        if self.file_storage_backend == "local":
            return os.path.join(self.local_data_dir, "files")
        if self.file_storage_backend == "databricks_volume":
            return self.slack_files_path
        return self.object_store_prefix

    @staticmethod
    def _has_spark() -> bool:
        try:
            from pyspark.sql import SparkSession  # noqa: F401
            return SparkSession.getActiveSession() is not None
        except ImportError:
            return False
