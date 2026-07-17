"""State persistence. LocalJsonStateStore backs dev/tests; DeltaStateStore is
the thin in-tenant implementation (verified during the golden-set run).
Upsert tables are keyed JSON files; append tables are JSONL."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone


class StateStore:
    def upsert(self, table: str, key: str, row: dict) -> None:
        raise NotImplementedError

    def get(self, table: str, key: str) -> dict | None:
        raise NotImplementedError

    def scan(self, table: str) -> list[dict]:
        raise NotImplementedError

    def append(self, table: str, row: dict) -> None:
        raise NotImplementedError


class LocalJsonStateStore(StateStore):
    def __init__(self, root_dir: str):
        self.root = root_dir
        os.makedirs(root_dir, exist_ok=True)

    def _kv_path(self, table: str) -> str:
        return os.path.join(self.root, f"{table}.json")

    def _log_path(self, table: str) -> str:
        return os.path.join(self.root, f"{table}.jsonl")

    def _load_kv(self, table: str) -> dict:
        try:
            with open(self._kv_path(table)) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def upsert(self, table, key, row):
        data = self._load_kv(table)
        data[key] = row
        with open(self._kv_path(table), "w") as f:
            json.dump(data, f, indent=1, default=str)

    def get(self, table, key):
        return self._load_kv(table).get(key)

    def scan(self, table):
        rows = list(self._load_kv(table).values())
        try:
            with open(self._log_path(table)) as f:
                rows += [json.loads(line) for line in f if line.strip()]
        except FileNotFoundError:
            pass
        return rows

    def append(self, table, row):
        with open(self._log_path(table), "a") as f:
            f.write(json.dumps(row, default=str) + "\n")


class DeltaStateStore(StateStore):
    """Thin Delta-backed store. Rows are stored as JSON payloads so the Delta
    schema never changes as row dicts evolve. Verified in-tenant."""

    def __init__(self, spark, config):
        self.spark = spark
        self.ns = f"{config.catalog}.{config.control_schema}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.ns}")

    def _ensure_kv(self, table):
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.ns}.{table} "
                       "(key STRING, payload STRING, updated_at TIMESTAMP)")

    def _ensure_log(self, table):
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.ns}.{table} "
                       "(payload STRING, ts TIMESTAMP)")

    def upsert(self, table, key, row):
        self._ensure_kv(table)
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(row, default=str).replace("'", "''")
        safe_key = key.replace("'", "''")
        self.spark.sql(f"""
            MERGE INTO {self.ns}.{table} t
            USING (SELECT '{safe_key}' AS key, '{payload}' AS payload,
                          TIMESTAMP'{now}' AS updated_at) s
            ON t.key = s.key
            WHEN MATCHED THEN UPDATE SET payload = s.payload, updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *""")

    def get(self, table, key):
        self._ensure_kv(table)
        safe_key = key.replace("'", "''")
        rows = self.spark.sql(
            f"SELECT payload FROM {self.ns}.{table} WHERE key = '{safe_key}'").collect()
        return json.loads(rows[0]["payload"]) if rows else None

    def scan(self, table):
        self._ensure_kv(table)
        return [json.loads(r["payload"])
                for r in self.spark.sql(f"SELECT payload FROM {self.ns}.{table}").collect()]

    def append(self, table, row):
        self._ensure_log(table)
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(row, default=str).replace("'", "''")
        self.spark.sql(f"INSERT INTO {self.ns}.{table} "
                       f"VALUES ('{payload}', TIMESTAMP'{now}')")
