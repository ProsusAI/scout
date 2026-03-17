"""Local JSONL file storage backend. Zero external dependencies."""
from __future__ import annotations

import json
import logging
import os
import tempfile

from scout.slack_listener.storage.base import StorageWriter

logger = logging.getLogger(__name__)


class LocalFileWriter(StorageWriter):
    def __init__(self, data_dir: str) -> None:
        self._data_dir = data_dir
        self._jsonl_path = os.path.join(data_dir, "slack_messages.jsonl")
        self._watermarks_path = os.path.join(data_dir, "watermarks.json")
        os.makedirs(data_dir, exist_ok=True)

    def upsert_batch(self, docs: list[dict]) -> int:
        if not docs:
            return 0
        existing = self._load_all()
        for doc in docs:
            doc_id = doc["doc_id"]
            prev = existing.get(doc_id)
            if prev is None or prev.get("text") != doc.get("text") or prev.get("dbfs_path") != doc.get("dbfs_path"):
                existing[doc_id] = doc
        self._write_all(existing)
        return len(docs)

    def delete_batch(self, doc_ids: list[str]) -> int:
        if not doc_ids:
            return 0
        existing = self._load_all()
        deleted = sum(1 for did in doc_ids if existing.pop(did, None) is not None)
        if deleted:
            self._write_all(existing)
        return deleted

    def load_watermarks(self) -> dict[str, str]:
        if not os.path.exists(self._watermarks_path):
            return {}
        with open(self._watermarks_path) as f:
            return json.load(f)

    def save_watermarks(self, watermarks: dict[str, str]) -> None:
        self._atomic_write(self._watermarks_path, json.dumps(watermarks, indent=2))

    # -- internal helpers --

    def _load_all(self) -> dict[str, dict]:
        if not os.path.exists(self._jsonl_path):
            return {}
        docs: dict[str, dict] = {}
        with open(self._jsonl_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    doc = json.loads(line)
                    docs[doc["doc_id"]] = doc
        return docs

    def _write_all(self, docs: dict[str, dict]) -> None:
        content = "".join(json.dumps(doc, default=str) + "\n" for doc in docs.values())
        self._atomic_write(self._jsonl_path, content)

    def _atomic_write(self, path: str, content: str) -> None:
        fd, tmp = tempfile.mkstemp(dir=self._data_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
            os.replace(tmp, path)
        except Exception:
            os.unlink(tmp)
            raise
