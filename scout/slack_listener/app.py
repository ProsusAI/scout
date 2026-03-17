"""
Slack listener service entry point.

Selects ingestion mode based on INGEST_MODE env var:
  poll   — polls conversations_history with watermarks (dev, user token)
  socket — real-time Socket Mode via slack_bolt (production, bot+app tokens)
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from scout.slack_listener.config import Config
from scout.slack_listener.storage import get_writer


def _load_dotenv() -> None:
    """Load .env file from the current directory or the slack_listener project root."""
    for candidate in [Path.cwd() / ".env", Path(__file__).resolve().parent.parent / ".env"]:
        if candidate.is_file():
            with open(candidate) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    if not os.environ.get(key):
                        os.environ[key] = value
            break


def main() -> None:
    _load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("slack_listener")

    config = Config.from_env()
    try:
        config.validate()
    except ValueError as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    writer = get_writer(config)
    logger.info("mode=%s  storage=%s", config.ingest_mode, config.storage_backend)

    if config.ingest_mode == "poll":
        from scout.slack_listener.ingestion.poller import run_poller
        run_poller(config, writer)
    elif config.ingest_mode == "socket":
        from scout.slack_listener.ingestion.socket_handler import run_socket_mode
        run_socket_mode(config, writer)
    else:
        logger.error("Unknown INGEST_MODE: %s (expected 'poll' or 'socket')", config.ingest_mode)
        sys.exit(1)
