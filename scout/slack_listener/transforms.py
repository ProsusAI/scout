"""
Transform Slack message payloads into documents matching the Databricks notebook schema.
"""
from __future__ import annotations

from datetime import datetime, timezone


def _extract_full_text(msg: dict) -> str:
    """Combine top-level text with attachment/shared-message text."""
    parts: list[str] = []

    top_text = (msg.get("text") or "").strip()
    if top_text:
        parts.append(top_text)

    for att in msg.get("attachments") or []:
        att_text = (att.get("text") or att.get("fallback") or "").strip()
        if att_text and att_text not in parts:
            parts.append(att_text)

    return "\n\n".join(parts)


def message_to_doc(
    channel_id: str,
    channel_name: str,
    user_name: str,
    real_name: str,
    msg: dict,
    file_paths: list[str] | None = None,
) -> dict:
    """
    Build a document from a Slack message.

    Schema matches the Databricks Slack Scraper notebook:
    doc_id, channel_name, user_name, real_name, ts, text,
    dbfs_path, subtype, is_bot, ingested_at.
    """
    ts = msg.get("ts", "")
    text = _extract_full_text(msg)
    subtype = msg.get("subtype") or ""
    is_bot = subtype == "bot_message" or bool(msg.get("bot_id"))

    return {
        "doc_id": f"{channel_id}_{ts}",
        "channel_name": channel_name,
        "user_name": user_name,
        "real_name": real_name,
        "ts": ts,
        "text": text,
        "dbfs_path": file_paths or [],
        "subtype": subtype,
        "is_bot": is_bot,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
