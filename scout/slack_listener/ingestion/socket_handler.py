"""
Socket Mode event handler for real-time Slack ingestion.
Requires a bot token (xoxb-) and app-level token (xapp-).
"""
from __future__ import annotations

import logging
import threading
import time

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient

from scout.slack_listener.config import Config
from scout.slack_listener.file_downloader import download_message_files
from scout.slack_listener.storage import StorageWriter
from scout.slack_listener.transforms import message_to_doc
from scout.slack_listener.user_cache import UserCache

logger = logging.getLogger(__name__)


class _BatchQueue:
    """Thread-safe queue that flushes docs to storage on interval or size threshold."""

    def __init__(self, writer: StorageWriter, flush_seconds: float, max_size: int) -> None:
        self._writer = writer
        self._flush_seconds = flush_seconds
        self._max_size = max_size
        self._upsert_buf: list[dict] = []
        self._delete_buf: list[str] = []
        self._lock = threading.Lock()
        self._total_written = 0
        self._total_deleted = 0

        self._timer = threading.Thread(target=self._flush_loop, daemon=True)
        self._timer.start()

    def enqueue_upsert(self, doc: dict) -> None:
        with self._lock:
            self._upsert_buf.append(doc)
            if len(self._upsert_buf) >= self._max_size:
                self._flush_locked()

    def enqueue_delete(self, doc_id: str) -> None:
        with self._lock:
            self._delete_buf.append(doc_id)
            if len(self._delete_buf) >= self._max_size:
                self._flush_locked()

    @property
    def stats(self) -> tuple[int, int]:
        return self._total_written, self._total_deleted

    def _flush_loop(self) -> None:
        while True:
            time.sleep(self._flush_seconds)
            with self._lock:
                self._flush_locked()

    def _flush_locked(self) -> None:
        if self._upsert_buf:
            batch = self._upsert_buf[:]
            self._upsert_buf.clear()
            try:
                written = self._writer.upsert_batch(batch)
                self._total_written += written
                logger.info("Flushed %d upsert(s) | Total: %d", written, self._total_written)
            except Exception:
                logger.exception("Failed to flush upsert batch (%d docs)", len(batch))

        if self._delete_buf:
            batch = self._delete_buf[:]
            self._delete_buf.clear()
            try:
                deleted = self._writer.delete_batch(batch)
                self._total_deleted += deleted
                logger.info("Flushed %d delete(s) | Total: %d", deleted, self._total_deleted)
            except Exception:
                logger.exception("Failed to flush delete batch (%d ids)", len(batch))


def run_socket_mode(config: Config, writer: StorageWriter) -> None:
    """Start the Socket Mode listener."""
    app = App(token=config.slack_token)
    client = WebClient(token=config.slack_token)
    user_cache = UserCache(client)

    channel_name_cache: dict[str, str] = {}
    queue = _BatchQueue(writer, config.batch_flush_seconds, config.batch_max_size)

    def _resolve_channel(channel_id: str) -> str:
        if channel_id in channel_name_cache:
            return channel_name_cache[channel_id]
        try:
            info = client.conversations_info(channel=channel_id)
            name = (info.get("channel") or {}).get("name", channel_id)
        except Exception:
            name = channel_id
        channel_name_cache[channel_id] = name
        return name

    _SKIP_SUBTYPES = {"channel_join", "channel_leave", "channel_topic", "channel_purpose"}

    @app.event("message")
    def handle_message(event: dict, say) -> None:
        subtype = event.get("subtype") or ""
        channel_id = event.get("channel", "")

        if subtype == "message_deleted":
            deleted_ts = event.get("deleted_ts")
            if deleted_ts:
                doc_id = f"{channel_id}_{deleted_ts}"
                queue.enqueue_delete(doc_id)
                logger.info("Queued delete: %s", doc_id)
            return

        if subtype == "message_changed":
            msg = event.get("message", {})
        else:
            msg = event

        ts = msg.get("ts")
        if not ts or subtype in _SKIP_SUBTYPES:
            return

        channel_name = _resolve_channel(channel_id)
        user_id = msg.get("user", "")
        user_name, real_name = user_cache.resolve(user_id) if user_id else ("", "Unknown")
        file_paths = download_message_files(msg, config.slack_token, config.files_dir)

        doc = message_to_doc(
            channel_id=channel_id,
            channel_name=channel_name,
            user_name=user_name,
            real_name=real_name,
            msg=msg,
            file_paths=file_paths,
        )
        queue.enqueue_upsert(doc)
        preview = doc["text"][:60] + ("..." if len(doc["text"]) > 60 else "")
        logger.info("Queued upsert: #%s <%s> %s", channel_name, user_name, preview)

    logger.info("Starting Socket Mode listener...")
    handler = SocketModeHandler(app, config.slack_app_token)
    handler.start()
