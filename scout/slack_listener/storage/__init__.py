"""Storage backends for the Slack listener service."""
from scout.slack_listener.storage.base import StorageWriter
from scout.slack_listener.storage.factory import get_writer

__all__ = ["StorageWriter", "get_writer"]
