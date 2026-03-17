"""Abstract base class for storage writers."""
from __future__ import annotations

from abc import ABC, abstractmethod


class StorageWriter(ABC):
    @abstractmethod
    def upsert_batch(self, docs: list[dict]) -> int:
        """Upsert documents. Returns count of documents written."""

    @abstractmethod
    def delete_batch(self, doc_ids: list[str]) -> int:
        """Delete documents by doc_id. Returns count deleted."""

    @abstractmethod
    def load_watermarks(self) -> dict[str, str]:
        """Load per-channel high-water marks."""

    @abstractmethod
    def save_watermarks(self, watermarks: dict[str, str]) -> None:
        """Persist per-channel high-water marks."""
