"""OpenSearch storage backend."""
from __future__ import annotations

import logging

from scout.slack_listener.config import Config
from scout.slack_listener.storage.base import StorageWriter

logger = logging.getLogger(__name__)


class OpenSearchWriter(StorageWriter):
    def __init__(self, config: Config) -> None:
        from opensearchpy import OpenSearch
        self._client = OpenSearch(
            hosts=[{"host": config.opensearch_host, "port": config.opensearch_port}],
            use_ssl=False,
            verify_certs=False,
        )
        self._index = "slack-messages"

    def upsert_batch(self, docs: list[dict]) -> int:
        if not docs:
            return 0
        from opensearchpy import helpers

        def gen():
            for doc in docs:
                yield {"_index": self._index, "_id": doc["doc_id"], "_source": doc}

        success, _ = helpers.bulk(self._client, gen(), raise_on_error=False, raise_on_exception=False)
        return success

    def delete_batch(self, doc_ids: list[str]) -> int:
        deleted = 0
        for doc_id in doc_ids:
            try:
                self._client.delete(index=self._index, id=doc_id, ignore=[404])
                deleted += 1
            except Exception as e:
                logger.warning("Failed to delete %s: %s", doc_id, e)
        return deleted

    def load_watermarks(self) -> dict[str, str]:
        try:
            resp = self._client.search(
                index=self._index,
                body={
                    "size": 0,
                    "aggs": {
                        "channels": {
                            "terms": {"field": "channel_name", "size": 1000},
                            "aggs": {"max_ts": {"max": {"field": "ts"}}},
                        }
                    },
                },
            )
            result = {}
            for bucket in resp["aggregations"]["channels"]["buckets"]:
                val = bucket["max_ts"]["value"]
                if val:
                    result[bucket["key"]] = str(val)
            return result
        except Exception:
            return {}

    def save_watermarks(self, watermarks: dict[str, str]) -> None:
        pass  # Watermarks are derived from the index
