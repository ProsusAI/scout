"""
Download Slack file attachments with magic-byte validation.
Adapted from the Databricks Slack Scraper notebook.
"""
from __future__ import annotations

import logging
import os
import re
import time

import requests

logger = logging.getLogger(__name__)

SUPPORTED_MIMES: dict[str, str] = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "text/csv": ".csv",
    "application/csv": ".csv",
    "image/png": ".png",
    "image/jpeg": ".jpeg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
}

MAGIC_BYTES: dict[str, bytes] = {
    "application/pdf": b"%PDF",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": b"PK\x03\x04",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": b"PK\x03\x04",
    "image/png": b"\x89PNG",
    "image/jpeg": b"\xff\xd8\xff",
    "image/gif": b"GIF8",
}

_MAX_RETRIES = 5
_SANITIZE_RE = re.compile(r"[^\w\-.]")


def _sanitize_filename(name: str) -> str:
    return _SANITIZE_RE.sub("_", name)


def _download_bytes(
    url: str, token: str, mime_type: str | None = None
) -> bytes | None:
    headers = {"Authorization": f"Bearer {token}"}
    retry_delay = 2

    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    logger.warning("HTML response for %s — likely expired token", url)
                    return None

                expected = MAGIC_BYTES.get(mime_type) if mime_type else None
                if expected and not resp.content.startswith(expected):
                    preview = resp.content[:40].decode("utf-8", errors="replace")
                    logger.warning("Magic bytes mismatch for %s: %r", url, preview)
                    return None

                return resp.content

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", retry_delay))
                logger.warning("Rate limited, waiting %ds (attempt %d/%d)", wait, attempt + 1, _MAX_RETRIES)
                time.sleep(wait)
                retry_delay *= 2
                continue

            resp.raise_for_status()

        except Exception as e:
            logger.error("Download attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt == _MAX_RETRIES - 1:
                return None
            time.sleep(retry_delay)
            retry_delay *= 2

    return None


def download_message_files(
    msg: dict, token: str, target_dir: str
) -> list[str]:
    """
    Download supported file attachments from a Slack message.
    Returns list of saved file paths. Skips files that already exist.
    """
    files = msg.get("files") or []
    if not files:
        return []

    os.makedirs(target_dir, exist_ok=True)
    saved_paths: list[str] = []

    for f in files:
        mime = f.get("mimetype")
        if mime not in SUPPORTED_MIMES:
            continue

        file_id = f.get("id", "unknown")
        raw_name = f.get("name", "file")
        filename = f"{file_id}_{_sanitize_filename(raw_name)}"
        target_path = os.path.join(target_dir, filename)

        if os.path.exists(target_path):
            saved_paths.append(target_path)
            continue

        url = f.get("url_private_download")
        if not url:
            logger.warning("No download URL for file %s", file_id)
            continue

        content = _download_bytes(url, token, mime_type=mime)
        if content:
            with open(target_path, "wb") as fh:
                fh.write(content)
            saved_paths.append(target_path)
            logger.info("Saved %s (%d bytes)", filename, len(content))
        else:
            logger.warning("Failed to download file %s", file_id)

    return saved_paths


def download_thread_files(
    messages: list[dict], token: str, target_dir: str
) -> list[str]:
    """
    Download supported attachments across all messages in a thread.
    Returns de-duplicated saved paths while preserving first-seen order.
    """
    seen: set[str] = set()
    saved_paths: list[str] = []

    for msg in messages:
        for path in download_message_files(msg, token, target_dir):
            if path in seen:
                continue
            seen.add(path)
            saved_paths.append(path)

    return saved_paths
