"""
Cached resolution of Slack user_id → (user_name, real_name).
Refreshes periodically to pick up new users.
"""
from __future__ import annotations

import logging
import time

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)

REFRESH_INTERVAL = 1800  # 30 minutes


class UserCache:
    def __init__(self, client: WebClient) -> None:
        self._client = client
        self._cache: dict[str, tuple[str, str]] = {}
        self._last_refresh: float = 0.0
        self._refresh()

    def resolve(self, user_id: str) -> tuple[str, str]:
        """Return (user_name, real_name) for a user_id, refreshing if stale."""
        if time.monotonic() - self._last_refresh > REFRESH_INTERVAL:
            self._refresh()

        if user_id in self._cache:
            return self._cache[user_id]

        try:
            resp = self._client.users_info(user=user_id)
            user = resp.get("user", {})
            entry = (user.get("name", user_id), user.get("real_name", "Unknown"))
            self._cache[user_id] = entry
            return entry
        except SlackApiError as e:
            if e.response.status_code == 429:
                wait = int(e.response.headers.get("Retry-After", 5))
                logger.warning("Rate limited looking up user %s, waiting %ds", user_id, wait)
                time.sleep(wait)
                return self.resolve(user_id)
            return (user_id, "Unknown")

    def _refresh(self) -> None:
        new_cache: dict[str, tuple[str, str]] = {}
        try:
            cursor = None
            while True:
                resp = self._client.users_list(limit=500, cursor=cursor or "")
                for user in resp.get("members", []):
                    uid = user.get("id")
                    if uid:
                        new_cache[uid] = (
                            user.get("name", uid),
                            user.get("real_name", "Unknown"),
                        )
                cursor = (resp.get("response_metadata") or {}).get("next_cursor")
                if not cursor:
                    break
                time.sleep(1.2)
            self._cache = new_cache
            logger.info("User cache refreshed: %d users", len(self._cache))
        except SlackApiError as e:
            if e.response.status_code == 429:
                wait = int(e.response.headers.get("Retry-After", 10))
                logger.warning("Rate limited refreshing users, waiting %ds", wait)
                time.sleep(wait)
            else:
                logger.warning("Failed to refresh user cache: %s", e)
        self._last_refresh = time.monotonic()
