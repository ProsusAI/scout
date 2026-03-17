"""Factory function to select storage backend from config."""
from __future__ import annotations

from scout.slack_listener.config import Config
from scout.slack_listener.storage.base import StorageWriter


def get_writer(config: Config) -> StorageWriter:
    if config.storage_backend == "local":
        from scout.slack_listener.storage.local import LocalFileWriter
        return LocalFileWriter(config.local_data_dir)
    if config.storage_backend == "delta":
        from scout.slack_listener.storage.delta import DeltaLakeWriter
        return DeltaLakeWriter(config)
    if config.storage_backend == "opensearch":
        from scout.slack_listener.storage.opensearch import OpenSearchWriter
        return OpenSearchWriter(config)
    raise ValueError(f"Unknown storage backend: {config.storage_backend}")
