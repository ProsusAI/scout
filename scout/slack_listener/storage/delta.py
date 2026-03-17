"""Delta Lake storage backend via Spark or databricks-sql-connector."""
from __future__ import annotations

import logging

from scout.slack_listener.config import Config
from scout.slack_listener.storage.base import StorageWriter

logger = logging.getLogger(__name__)


class DeltaLakeWriter(StorageWriter):
    def __init__(self, config: Config) -> None:
        self._table = config.delta_table_name
        self._spark = self._get_spark()
        self._connector = None

        if self._spark is None:
            from databricks import sql as dbsql
            self._connector = dbsql.connect(
                server_hostname=config.databricks_host.replace("https://", ""),
                http_path=config.databricks_http_path,
                access_token=config.databricks_token,
            )

    def upsert_batch(self, docs: list[dict]) -> int:
        if not docs:
            return 0
        if self._spark is not None:
            return self._upsert_spark(docs)
        return self._upsert_connector(docs)

    def delete_batch(self, doc_ids: list[str]) -> int:
        if not doc_ids:
            return 0
        placeholders = ", ".join(f"'{did}'" for did in doc_ids)
        sql = f"DELETE FROM {self._table} WHERE doc_id IN ({placeholders})"
        if self._spark is not None:
            self._spark.sql(sql)
        else:
            with self._connector.cursor() as cursor:
                cursor.execute(sql)
        return len(doc_ids)

    def load_watermarks(self) -> dict[str, str]:
        sql = f"SELECT channel_name, MAX(ts) as max_ts FROM {self._table} GROUP BY channel_name"
        try:
            if self._spark is not None:
                rows = self._spark.sql(sql).collect()
                return {r["channel_name"]: r["max_ts"] for r in rows if r["max_ts"]}
            else:
                with self._connector.cursor() as cursor:
                    cursor.execute(sql)
                    return {r[0]: r[1] for r in cursor.fetchall() if r[1]}
        except Exception:
            return {}

    def save_watermarks(self, watermarks: dict[str, str]) -> None:
        pass  # Watermarks are derived from the table itself

    def _upsert_spark(self, docs: list[dict]) -> int:
        from pyspark.sql import Row
        from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

        schema = StructType([
            StructField("doc_id", StringType()),
            StructField("channel_name", StringType()),
            StructField("user_name", StringType()),
            StructField("real_name", StringType()),
            StructField("ts", StringType()),
            StructField("text", StringType()),
            StructField("dbfs_path", ArrayType(StringType())),
            StructField("subtype", StringType()),
            StructField("is_bot", BooleanType()),
            StructField("ingested_at", StringType()),
        ])

        rows = [Row(**{k: (list(v) if isinstance(v, list) else v) for k, v in d.items()}) for d in docs]
        df = self._spark.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView("_incoming_slack")

        self._spark.sql(f"""
            MERGE INTO {self._table} AS target
            USING _incoming_slack AS source
            ON target.doc_id = source.doc_id
            WHEN MATCHED AND (target.text != source.text
                 OR array_sort(target.dbfs_path) != array_sort(source.dbfs_path))
              THEN UPDATE SET
                target.text = source.text,
                target.dbfs_path = source.dbfs_path,
                target.ingested_at = source.ingested_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        return len(docs)

    def _upsert_connector(self, docs: list[dict]) -> int:
        with self._connector.cursor() as cursor:
            for doc in docs:
                dbfs_path_sql = "ARRAY(" + ", ".join(f"'{p}'" for p in doc.get("dbfs_path", [])) + ")"
                cursor.execute(f"""
                    MERGE INTO {self._table} AS target
                    USING (SELECT
                        '{doc["doc_id"]}' as doc_id,
                        '{doc.get("channel_name", "")}' as channel_name,
                        '{doc.get("user_name", "")}' as user_name,
                        '{doc.get("real_name", "")}' as real_name,
                        '{doc.get("ts", "")}' as ts,
                        '{doc.get("text", "").replace("'", "''")}' as text,
                        {dbfs_path_sql} as dbfs_path,
                        '{doc.get("subtype", "")}' as subtype,
                        {str(doc.get("is_bot", False)).lower()} as is_bot,
                        '{doc.get("ingested_at", "")}' as ingested_at
                    ) AS source
                    ON target.doc_id = source.doc_id
                    WHEN MATCHED AND (target.text != source.text
                         OR array_sort(target.dbfs_path) != array_sort(source.dbfs_path))
                      THEN UPDATE SET
                        target.text = source.text,
                        target.dbfs_path = source.dbfs_path,
                        target.ingested_at = source.ingested_at
                    WHEN NOT MATCHED THEN INSERT *
                """)
        return len(docs)

    @staticmethod
    def _get_spark():
        try:
            from pyspark.sql import SparkSession
            return SparkSession.getActiveSession()
        except ImportError:
            return None
