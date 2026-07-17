"""State persistence. LocalJsonStateStore backs dev/tests; DeltaStateStore is
the thin in-tenant implementation (verified during the golden-set run).
Upsert tables are keyed JSON files; append tables are JSONL."""
from __future__ import annotations

import json
import os


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
    schema never changes as row dicts evolve. Values are passed via named
    parameter markers (spark.sql args=..., Spark 3.4+), never spliced into
    SQL text, so payload content cannot corrupt the statement. Verified
    in-tenant.

    A table name is either key/value-shaped (upsert/get) or append-log-shaped
    (append); using one name for both raises instead of silently creating the
    wrong schema. scan() works on either shape."""

    KV_COLS = ("key", "payload", "updated_at")
    LOG_COLS = ("payload", "ts")

    def __init__(self, spark, config):
        self.spark = spark
        self.ns = f"{config.catalog}.{config.control_schema}"
        self._ensured: dict[str, str] = {}  # table -> "kv" | "log"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.ns}")

    def _cols(self, table: str) -> tuple:
        return tuple(f.name for f in
                     self.spark.table(f"{self.ns}.{table}").schema.fields)

    def _ensure(self, table: str, kind: str) -> None:
        cached = self._ensured.get(table)
        if cached == kind:
            return
        if cached is not None:
            raise ValueError(
                f"table {table!r} already used as {cached}; cannot reuse as {kind}")
        ddl = ("(key STRING, payload STRING, updated_at TIMESTAMP)"
               if kind == "kv" else "(payload STRING, ts TIMESTAMP)")
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.ns}.{table} {ddl}")
        expected = self.KV_COLS if kind == "kv" else self.LOG_COLS
        cols = self._cols(table)
        if cols != expected:
            raise ValueError(
                f"table {self.ns}.{table} has columns {cols}, expected {expected} "
                f"for {kind} usage — was it created by the wrong accessor?")
        self._ensured[table] = kind

    def upsert(self, table, key, row):
        self._ensure(table, "kv")
        self.spark.sql(
            f"""MERGE INTO {self.ns}.{table} t
                USING (SELECT :k AS key, :p AS payload,
                              current_timestamp() AS updated_at) s
                ON t.key = s.key
                WHEN MATCHED THEN UPDATE SET payload = s.payload,
                                             updated_at = s.updated_at
                WHEN NOT MATCHED THEN INSERT *""",
            args={"k": key, "p": json.dumps(row, default=str)})

    def get(self, table, key):
        self._ensure(table, "kv")
        rows = self.spark.sql(
            f"SELECT payload FROM {self.ns}.{table} WHERE key = :k",
            args={"k": key}).collect()
        return json.loads(rows[0]["payload"]) if rows else None

    def scan(self, table):
        kind = self._ensured.get(table)
        if kind is None:
            if not self.spark.catalog.tableExists(f"{self.ns}.{table}"):
                return []
            kind = "kv" if self._cols(table) == self.KV_COLS else "log"
            self._ensured[table] = kind
        order_col = "updated_at" if kind == "kv" else "ts"
        return [json.loads(r["payload"]) for r in self.spark.sql(
            f"SELECT payload FROM {self.ns}.{table} ORDER BY {order_col}").collect()]

    def append(self, table, row):
        self._ensure(table, "log")
        self.spark.sql(
            f"INSERT INTO {self.ns}.{table} VALUES (:p, current_timestamp())",
            args={"p": json.dumps(row, default=str)})
