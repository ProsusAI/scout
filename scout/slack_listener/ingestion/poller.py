"""
Polling-based Slack ingestion using conversations_history.
Works with a user token (xoxp-), no Slack app required.
"""
from __future__ import annotations

import logging
import signal
import time

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from scout.slack_listener.config import Config
from scout.slack_listener.file_downloader import download_message_files
from scout.slack_listener.storage import StorageWriter
from scout.slack_listener.transforms import message_to_doc
from scout.slack_listener.user_cache import UserCache

logger = logging.getLogger(__name__)


API_PAUSE = 1.2  # seconds between Slack API calls to stay under rate limits

_SKIP_SUBTYPES = frozenset({
    "channel_join",
    "channel_leave",
    "channel_topic",
    "channel_purpose",
    "channel_name",
    "channel_archive",
    "channel_unarchive",
    "group_join",
    "group_leave",
    "group_topic",
    "group_purpose",
    "group_name",
    "group_archive",
    "group_unarchive",
    "pinned_item",
    "unpinned_item",
})


def _rate_pause() -> None:
    time.sleep(API_PAUSE)


def _list_channels(client: WebClient) -> list[tuple[str, str]]:
    """Return [(channel_id, channel_name)] for public channels the token owner is in."""
    channels: list[tuple[str, str]] = []
    cursor = None
    while True:
        try:
            resp = client.conversations_list(types="public_channel", limit=200, cursor=cursor or "")
        except SlackApiError as e:
            if e.response.status_code == 429:
                wait = int(e.response.headers.get("Retry-After", 10))
                logger.warning("Rate limited listing channels, waiting %ds", wait)
                time.sleep(wait)
                continue
            raise
        for ch in resp.get("channels", []):
            if ch.get("is_member"):
                channels.append((ch["id"], ch.get("name", ch["id"])))
        cursor = (resp.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break
        _rate_pause()
    return channels


def _resolve_channel_names(
    client: WebClient, channel_ids: list[str]
) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for cid in channel_ids:
        try:
            info = client.conversations_info(channel=cid)
            name = (info.get("channel") or {}).get("name", cid)
        except SlackApiError:
            name = cid
        result.append((cid, name))
        _rate_pause()
    return result


def _poll_channel(
    client: WebClient, channel_id: str, oldest: str | None
) -> list[dict]:
    messages: list[dict] = []
    cursor = None
    while True:
        params: dict = {"channel": channel_id, "limit": 200}
        if oldest:
            params["oldest"] = oldest
        if cursor:
            params["cursor"] = cursor
        try:
            resp = client.conversations_history(**params)
        except SlackApiError as e:
            if e.response.status_code == 429:
                wait = int(e.response.headers.get("Retry-After", 10))
                logger.warning("Rate limited on %s, waiting %ds", channel_id, wait)
                time.sleep(wait)
                continue
            raise
        for msg in resp.get("messages", []):
            if msg.get("type") == "message" and msg.get("subtype") not in _SKIP_SUBTYPES:
                messages.append(msg)
        cursor = (resp.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break
        _rate_pause()
    return messages


def run_poller(config: Config, writer: StorageWriter) -> None:
    """Main polling loop. Runs until interrupted."""
    client = WebClient(token=config.slack_token)
    user_cache = UserCache(client)

    if config.slack_channel_ids:
        channels = _resolve_channel_names(client, config.slack_channel_ids)
    else:
        channels = _list_channels(client)

    if not channels:
        logger.error("No channels found. Check token permissions or set SLACK_CHANNEL_IDS.")
        return

    logger.info(
        "Polling %d channel(s): %s (every %.0fs)",
        len(channels),
        ", ".join(f"#{n}" for _, n in channels),
        config.poll_interval,
    )

    watermarks = writer.load_watermarks()
    channel_name_map = {cid: name for cid, name in channels}
    start_floor = config.start_from_ts

    id_watermarks: dict[str, str | None] = {}
    for cid, cname in channels:
        saved = watermarks.get(cid) or watermarks.get(cname)
        id_watermarks[cid] = saved or start_floor

    if start_floor:
        logger.info("Start floor: %s (channels without watermarks will start here)", start_floor)
    elif not watermarks:
        logger.info("No watermarks and no SLACK_START_FROM — first poll will pull full history")

    running = True

    def _handle_signal(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    total_indexed = 0
    poll_count = 0

    while running:
        poll_count += 1
        cycle_docs: list[dict] = []
        pending_watermarks: dict[str, str] = {}

        for idx, (cid, cname) in enumerate(channels):
            if idx > 0:
                _rate_pause()
            try:
                raw_msgs = _poll_channel(client, cid, id_watermarks.get(cid))
            except SlackApiError as e:
                logger.warning("Skipping #%s this cycle (error: %s)", cname, e)
                continue
            except Exception as e:
                logger.error("Unexpected error polling #%s: %s", cname, e)
                continue

            if not raw_msgs:
                continue

            logger.info("#%s: %d new message(s)", cname, len(raw_msgs))

            for msg in raw_msgs:
                user_id = msg.get("user", "")
                user_name, real_name = user_cache.resolve(user_id) if user_id else (user_id, "Unknown")
                file_paths = download_message_files(msg, config.slack_token, config.files_dir)
                doc = message_to_doc(
                    channel_id=cid,
                    channel_name=cname,
                    user_name=user_name,
                    real_name=real_name,
                    msg=msg,
                    file_paths=file_paths,
                )
                cycle_docs.append(doc)

            latest_ts = max(m.get("ts", "0") for m in raw_msgs)
            pending_watermarks[cid] = latest_ts

        if cycle_docs:
            try:
                written = writer.upsert_batch(cycle_docs)
            except Exception as e:
                logger.error("Batch write failed, watermarks NOT advanced: %s", e)
                written = 0
            else:
                for cid, ts in pending_watermarks.items():
                    id_watermarks[cid] = ts
                wm_to_save = {cid: ts for cid, ts in id_watermarks.items() if ts}
                writer.save_watermarks(wm_to_save)

            total_indexed += written
            logger.info("Wrote %d doc(s) | Total: %d | Poll #%d", written, total_indexed, poll_count)
        else:
            if poll_count % 12 == 0:
                logger.info("Still listening... (%d total, %d polls)", total_indexed, poll_count)

        if running:
            time.sleep(config.poll_interval)

    logger.info("Poller stopped. %d documents indexed across %d polls.", total_indexed, poll_count)
