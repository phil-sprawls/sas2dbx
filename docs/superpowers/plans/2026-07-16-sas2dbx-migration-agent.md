# sas2dbx Migration Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the SAS→Databricks migration pipeline from the approved spec (`docs/superpowers/specs/2026-07-16-sas2dbx-migration-agent-design.md`): translate SAS programs to Spark SQL/PySpark via the company AI gateway, execute in a sandbox, diff against SAS ground truth, self-repair with nested budgets, and emit parity certificates or triage reports.

**Architecture:** Deterministic Python package `sas_migrate/` driven by two Databricks notebooks. The LLM is called at exactly two points (translate, repair). All state flows through a `StateStore` interface (local JSON for dev/tests, Delta in-tenant). All LLM traffic flows through a `BaseGateway` interface (`MockGateway` for dev, `RestGatewayClient` filled in in-tenant).

**Tech Stack:** Python 3.10+, pyspark + pandas + requests/urllib (all in Databricks Runtime — zero runtime installs). Dev-only: pytest, pyspark (local mode). `pyreadstat` optional behind a guarded import.

## Global Constraints

- **Runtime dependencies:** ONLY Databricks Runtime built-ins (`pyspark`, `pandas`, stdlib). No `pip install` on the cluster. JFrog governs company packages.
- **Dev dependencies:** `pytest`, `pyspark`, `pandas` local only, never required on cluster.
- **Gateway models:** default `claude-opus-4-6`; `gpt-sol-5-6` selectable per stage via config. Model names are config values, never hardcoded in logic.
- **Parity:** order-insensitive; exact match on non-floats; float relative tolerance default `1e-9` (per-program override allowed, always recorded).
- **Repair budgets:** inner "make it run" = 3 attempts/step; outer "make it match" = 5 attempts/program. Exhaustion routes to triage with the loop type recorded.
- **Token budgets:** per-program cap 500,000; per-batch cap 20,000,000. Hitting a cap routes to triage (`failure_mode="budget"`), never silently continues.
- **Sandbox:** generated code may only write to `sandbox_<program_id>` schema.
- **SAS normalization (both sides before diff):** `.` missings → NULL, SAS 1960 epoch dates → ISO, trailing-blank padding trimmed.
- **In-tenant fill-in points** (the ONLY code that changes at work): `RestGatewayClient._build_request` / `._parse_response`, and `DeltaStateStore` smoke run.
- Local Spark tests use two-part names (`schema.table`); in-tenant code prepends the catalog from config.

## File Structure

```
sas2dbx/
├── pyproject.toml               # package metadata + dev extras
├── README.md                    # Task 14
├── sas_migrate/
│   ├── __init__.py
│   ├── config.py                # Task 1  — MigrationConfig dataclass
│   ├── gateway.py               # Task 2  — BaseGateway, RestGatewayClient, MockGateway, TokenBudget
│   ├── statestore.py            # Task 3  — StateStore, LocalJsonStateStore, DeltaStateStore
│   ├── inventory.py             # Task 4  — ProgramRecord, Inventory
│   ├── landing.py               # Task 5  — SAS normalization + readers + snapshot hashing
│   ├── preprocess.py            # Task 6  — %include, %let, step splitting
│   ├── translate.py             # Tasks 7–8 — prompts, response parsing, Translator
│   ├── validate.py              # Tasks 9–10 — value comparison, Spark diff, DiffReport
│   ├── execute.py               # Task 11 — Executor, sandbox guard
│   ├── repair.py                # Task 12 — nested repair loops
│   ├── report.py                # Task 13 — parity certificate, triage report
│   └── pipeline.py              # Task 14 — migrate_program / migrate_batch
├── notebooks/
│   ├── Migrate_One.py           # Task 14 — Databricks source-format notebook
│   └── Migrate_Batch.py         # Task 14
└── tests/
    ├── conftest.py              # Task 9 adds shared local SparkSession fixture
    ├── test_config.py           # Task 1
    ├── test_gateway.py          # Task 2
    ├── test_statestore.py       # Task 3
    ├── test_inventory.py        # Task 4
    ├── test_landing.py          # Task 5
    ├── test_preprocess.py       # Task 6
    ├── test_translate_prompts.py# Task 7
    ├── test_translate_parse.py  # Task 8
    ├── test_validate_values.py  # Task 9
    ├── test_validate_spark.py   # Task 10
    ├── test_execute.py          # Task 11
    ├── test_repair.py           # Task 12
    ├── test_report.py           # Task 13
    └── fixtures/
        ├── simple_etl.sas       # Task 6
        └── included_macro.sas   # Task 6
```

Interface dependency order: config → gateway/statestore (parallel) → inventory → landing → preprocess → translate → validate → execute → repair → report → pipeline.

---

### Task 1: Scaffold + MigrationConfig

**Files:**
- Create: `pyproject.toml`, `sas_migrate/__init__.py`, `sas_migrate/config.py`, `tests/test_config.py`, `.gitignore`

**Interfaces:**
- Produces: `MigrationConfig` dataclass — every later task takes a `config: MigrationConfig` argument. Field names below are load-bearing; later tasks reference them verbatim.

- [ ] **Step 1: Write scaffold files**

`pyproject.toml`:
```toml
[project]
name = "sas-migrate"
version = "0.1.0"
description = "SAS to Databricks migration agent with output data parity validation"
requires-python = ">=3.10"
dependencies = []  # runtime uses Databricks Runtime built-ins only (JFrog constraint)

[project.optional-dependencies]
dev = ["pytest>=7", "pyspark>=3.4", "pandas>=1.5"]

[tool.pytest.ini_options]
testpaths = ["tests"]

[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[tool.setuptools]
packages = ["sas_migrate"]
```

`.gitignore`:
```
__pycache__/
*.pyc
.pytest_cache/
venv/
.venv/
spark-warehouse/
metastore_db/
derby.log
*.egg-info/
```

`sas_migrate/__init__.py`:
```python
"""sas2dbx: SAS -> Databricks migration agent with output data parity validation."""
```

- [ ] **Step 2: Write the failing test**

`tests/test_config.py`:
```python
from sas_migrate.config import MigrationConfig


def test_defaults_match_spec():
    c = MigrationConfig()
    assert c.float_rel_tol == 1e-9
    assert c.max_run_repairs == 3
    assert c.max_match_repairs == 5
    assert c.per_program_token_cap == 500_000
    assert c.per_batch_token_cap == 20_000_000
    assert c.default_model == "claude-opus-4-6"
    assert c.gateway_circuit_breaker_threshold == 5


def test_sandbox_schema_is_per_program():
    c = MigrationConfig()
    assert c.sandbox_schema("prog_001") == "sandbox_prog_001"
```

- [ ] **Step 3: Set up venv and run test to verify it fails**

```bash
cd ~/dev/sas2dbx && python3 -m venv .venv && .venv/bin/pip install -q -e ".[dev]" && .venv/bin/pytest tests/test_config.py -v
```
Expected: FAIL / collection error — `No module named 'sas_migrate.config'`.

- [ ] **Step 4: Implement `sas_migrate/config.py`**

```python
from dataclasses import dataclass


@dataclass
class MigrationConfig:
    # Unity Catalog layout (catalog prepended in-tenant; local tests use 2-part names)
    catalog: str = "sas_migration"
    control_schema: str = "control"
    ground_truth_schema: str = "ground_truth"
    staging_schema: str = "staging_inputs"

    # Gateway / models — names are config, never hardcoded in logic
    gateway_base_url: str = ""
    default_model: str = "claude-opus-4-6"
    alt_model: str = "gpt-sol-5-6"
    gateway_max_retries: int = 4
    gateway_circuit_breaker_threshold: int = 5
    max_tokens_per_call: int = 8192

    # Parity
    float_rel_tol: float = 1e-9

    # Repair budgets
    max_run_repairs: int = 3      # inner loop: make it run (per step)
    max_match_repairs: int = 5    # outer loop: make it match (per program)

    # Token budgets
    per_program_token_cap: int = 500_000
    per_batch_token_cap: int = 20_000_000

    # Execution
    step_timeout_seconds: int = 1800

    def sandbox_schema(self, program_id: str) -> str:
        return f"sandbox_{program_id}"
```

- [ ] **Step 5: Run test to verify it passes**

```bash
.venv/bin/pytest tests/test_config.py -v
```
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: scaffold package with MigrationConfig"
```

---

### Task 2: Gateway — BaseGateway, RestGatewayClient, MockGateway, TokenBudget

**Files:**
- Create: `sas_migrate/gateway.py`, `tests/test_gateway.py`

**Interfaces:**
- Consumes: `MigrationConfig` (Task 1).
- Produces:
  - `GatewayResponse(text: str, input_tokens: int, output_tokens: int)` dataclass
  - `BaseGateway.complete(system: str, messages: list[dict], *, model: str | None = None, max_tokens: int | None = None, purpose: str = "", program_id: str = "") -> GatewayResponse` — `messages` items are `{"role": "user"|"assistant", "content": str}`
  - `MockGateway(responses: list[str])` — returns scripted responses in order; records `.calls` (list of dicts with system/messages/model/purpose)
  - `RestGatewayClient(config, auth_token, transport=None, on_call=None)` — `_build_request`/`_parse_response` raise `NotImplementedError` (in-tenant fill-in); `transport` is injectable for tests: `transport(payload: dict) -> dict`
  - Exceptions: `GatewayError`, `CircuitOpenError`, `TokenBudgetExceeded`
  - `TokenBudget(cap: int)` with `.charge(n: int)` (raises `TokenBudgetExceeded`) and `.used`

- [ ] **Step 1: Write the failing tests**

`tests/test_gateway.py`:
```python
import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import (
    CircuitOpenError, GatewayError, GatewayResponse, MockGateway,
    RestGatewayClient, TokenBudget, TokenBudgetExceeded,
)


def test_mock_gateway_returns_scripted_responses_and_records_calls():
    gw = MockGateway(["first", "second"])
    r1 = gw.complete("sys", [{"role": "user", "content": "a"}], purpose="translate")
    r2 = gw.complete("sys", [{"role": "user", "content": "b"}])
    assert (r1.text, r2.text) == ("first", "second")
    assert gw.calls[0]["purpose"] == "translate"
    assert len(gw.calls) == 2


def test_token_budget_raises_when_exceeded():
    b = TokenBudget(100)
    b.charge(60)
    with pytest.raises(TokenBudgetExceeded):
        b.charge(50)
    assert b.used == 110  # charge recorded even when it trips


def test_rest_client_retries_transient_failures_then_succeeds():
    attempts = []

    def flaky_transport(payload):
        attempts.append(payload)
        if len(attempts) < 3:
            raise GatewayError("transient 503")
        return {"text": "ok", "input_tokens": 10, "output_tokens": 5}

    cfg = MigrationConfig(gateway_max_retries=4)
    client = RestGatewayClient(cfg, auth_token="t", transport=flaky_transport,
                               retry_sleep=lambda s: None)
    client._build_request = lambda **kw: {"payload": True}
    client._parse_response = lambda raw: GatewayResponse(raw["text"], raw["input_tokens"], raw["output_tokens"])
    resp = client.complete("sys", [{"role": "user", "content": "hi"}])
    assert resp.text == "ok"
    assert len(attempts) == 3


def test_circuit_breaker_opens_after_consecutive_failures():
    def dead_transport(payload):
        raise GatewayError("down")

    cfg = MigrationConfig(gateway_max_retries=1, gateway_circuit_breaker_threshold=2)
    client = RestGatewayClient(cfg, auth_token="t", transport=dead_transport,
                               retry_sleep=lambda s: None)
    client._build_request = lambda **kw: {}
    client._parse_response = lambda raw: None
    for _ in range(2):
        with pytest.raises(GatewayError):
            client.complete("sys", [{"role": "user", "content": "x"}])
    with pytest.raises(CircuitOpenError):
        client.complete("sys", [{"role": "user", "content": "x"}])


def test_on_call_logging_hook_receives_record():
    records = []
    gw = MockGateway(["hello"], on_call=records.append)
    gw.complete("sys", [{"role": "user", "content": "x"}], purpose="repair", program_id="p1")
    assert records[0]["purpose"] == "repair"
    assert records[0]["program_id"] == "p1"
    assert records[0]["output_tokens"] > 0


def test_rest_client_build_request_is_in_tenant_fill_in():
    client = RestGatewayClient(MigrationConfig(), auth_token="t")
    with pytest.raises(NotImplementedError):
        client.complete("sys", [{"role": "user", "content": "x"}])
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_gateway.py -v
```
Expected: collection error — `No module named 'sas_migrate.gateway'`.

- [ ] **Step 3: Implement `sas_migrate/gateway.py`**

```python
"""All LLM traffic flows through BaseGateway. RestGatewayClient is the ONLY
module that will know the company gateway's REST contract; its _build_request
and _parse_response are filled in in-tenant."""
from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Callable

from sas_migrate.config import MigrationConfig


class GatewayError(Exception):
    """Transport or protocol failure talking to the gateway."""


class CircuitOpenError(GatewayError):
    """Too many consecutive failures; halting to protect the shared gateway."""


class TokenBudgetExceeded(GatewayError):
    """A token cap was hit; caller must route the program to triage."""


@dataclass
class GatewayResponse:
    text: str
    input_tokens: int
    output_tokens: int


class TokenBudget:
    def __init__(self, cap: int):
        self.cap = cap
        self.used = 0

    def charge(self, n: int) -> None:
        self.used += n
        if self.used > self.cap:
            raise TokenBudgetExceeded(f"token budget exceeded: {self.used}/{self.cap}")


class BaseGateway:
    def __init__(self, on_call: Callable[[dict], None] | None = None):
        self._on_call = on_call

    def complete(self, system: str, messages: list[dict], *, model: str | None = None,
                 max_tokens: int | None = None, purpose: str = "",
                 program_id: str = "") -> GatewayResponse:
        raise NotImplementedError

    def _log(self, *, model: str, purpose: str, program_id: str,
             resp: GatewayResponse, latency_s: float) -> None:
        if self._on_call:
            self._on_call({
                "ts": time.time(), "model": model, "purpose": purpose,
                "program_id": program_id, "input_tokens": resp.input_tokens,
                "output_tokens": resp.output_tokens, "latency_s": round(latency_s, 3),
            })


class MockGateway(BaseGateway):
    """Scripted gateway for dev and tests. Raises if the script runs out."""

    def __init__(self, responses: list[str], on_call: Callable[[dict], None] | None = None):
        super().__init__(on_call)
        self._responses = list(responses)
        self.calls: list[dict] = []

    def complete(self, system, messages, *, model=None, max_tokens=None,
                 purpose="", program_id=""):
        if not self._responses:
            raise GatewayError("MockGateway script exhausted")
        self.calls.append({"system": system, "messages": messages, "model": model,
                           "purpose": purpose, "program_id": program_id})
        text = self._responses.pop(0)
        resp = GatewayResponse(text, input_tokens=len(str(messages)) // 4,
                               output_tokens=max(1, len(text) // 4))
        self._log(model=model or "mock", purpose=purpose, program_id=program_id,
                  resp=resp, latency_s=0.0)
        return resp


class RestGatewayClient(BaseGateway):
    def __init__(self, config: MigrationConfig, auth_token: str,
                 transport: Callable[[dict], dict] | None = None,
                 on_call: Callable[[dict], None] | None = None,
                 retry_sleep: Callable[[float], None] = time.sleep):
        super().__init__(on_call)
        self.config = config
        self.auth_token = auth_token
        self._transport = transport or self._http_post
        self._retry_sleep = retry_sleep
        self._consecutive_failures = 0

    # ------------- IN-TENANT FILL-IN POINTS -------------
    def _build_request(self, *, system: str, messages: list[dict], model: str,
                       max_tokens: int) -> dict:
        raise NotImplementedError(
            "Fill in with the company AI gateway request contract (in-tenant).")

    def _parse_response(self, raw: dict) -> GatewayResponse:
        raise NotImplementedError(
            "Fill in with the company AI gateway response contract (in-tenant).")
    # ----------------------------------------------------

    def _http_post(self, payload: dict) -> dict:
        req = urllib.request.Request(
            self.config.gateway_base_url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {self.auth_token}"},
            method="POST")
        try:
            with urllib.request.urlopen(req, timeout=300) as f:
                return json.loads(f.read().decode())
        except Exception as e:  # noqa: BLE001 - normalize all transport errors
            raise GatewayError(f"gateway transport error: {e}") from e

    def complete(self, system, messages, *, model=None, max_tokens=None,
                 purpose="", program_id=""):
        threshold = self.config.gateway_circuit_breaker_threshold
        if self._consecutive_failures >= threshold:
            raise CircuitOpenError(
                f"{self._consecutive_failures} consecutive gateway failures; halting")
        model = model or self.config.default_model
        max_tokens = max_tokens or self.config.max_tokens_per_call
        payload = self._build_request(system=system, messages=messages,
                                      model=model, max_tokens=max_tokens)
        last_err: Exception | None = None
        for attempt in range(self.config.gateway_max_retries):
            start = time.time()
            try:
                raw = self._transport(payload)
                resp = self._parse_response(raw)
                self._consecutive_failures = 0
                self._log(model=model, purpose=purpose, program_id=program_id,
                          resp=resp, latency_s=time.time() - start)
                return resp
            except GatewayError as e:
                last_err = e
                self._retry_sleep(2 ** attempt)
        self._consecutive_failures += 1
        raise GatewayError(f"gateway failed after retries: {last_err}")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_gateway.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: gateway layer with retries, circuit breaker, token budget, mock"
```

---

### Task 3: StateStore — LocalJsonStateStore + DeltaStateStore

**Files:**
- Create: `sas_migrate/statestore.py`, `tests/test_statestore.py`

**Interfaces:**
- Produces:
  - `StateStore` base: `upsert(table: str, key: str, row: dict) -> None`, `get(table: str, key: str) -> dict | None`, `scan(table: str) -> list[dict]`, `append(table: str, row: dict) -> None`
  - `LocalJsonStateStore(root_dir: str)` — full implementation, used by all tests
  - `DeltaStateStore(spark, config)` — thin Delta implementation, verified in-tenant. Tables live in `{catalog}.{control_schema}` as `(key STRING, payload STRING, updated_at TIMESTAMP)` for upsert tables and `(payload STRING, ts TIMESTAMP)` for append tables; rows are JSON in `payload`.

- [ ] **Step 1: Write the failing tests**

`tests/test_statestore.py`:
```python
from sas_migrate.statestore import LocalJsonStateStore


def test_upsert_get_roundtrip(tmp_path):
    s = LocalJsonStateStore(str(tmp_path))
    s.upsert("inventory", "p1", {"program_id": "p1", "status": "registered"})
    s.upsert("inventory", "p1", {"program_id": "p1", "status": "landed"})
    assert s.get("inventory", "p1")["status"] == "landed"
    assert s.get("inventory", "missing") is None


def test_scan_returns_all_rows(tmp_path):
    s = LocalJsonStateStore(str(tmp_path))
    s.upsert("inventory", "p1", {"program_id": "p1"})
    s.upsert("inventory", "p2", {"program_id": "p2"})
    assert {r["program_id"] for r in s.scan("inventory")} == {"p1", "p2"}
    assert s.scan("empty_table") == []


def test_append_accumulates(tmp_path):
    s = LocalJsonStateStore(str(tmp_path))
    s.append("llm_calls", {"purpose": "translate"})
    s.append("llm_calls", {"purpose": "repair"})
    assert [r["purpose"] for r in s.scan("llm_calls")] == ["translate", "repair"]


def test_persistence_across_instances(tmp_path):
    LocalJsonStateStore(str(tmp_path)).upsert("inventory", "p1", {"x": 1})
    assert LocalJsonStateStore(str(tmp_path)).get("inventory", "p1") == {"x": 1}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_statestore.py -v
```
Expected: collection error — no module `sas_migrate.statestore`.

- [ ] **Step 3: Implement `sas_migrate/statestore.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_statestore.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: StateStore with local JSON and thin Delta implementations"
```

---

### Task 4: Inventory

**Files:**
- Create: `sas_migrate/inventory.py`, `tests/test_inventory.py`

**Interfaces:**
- Consumes: `StateStore` (Task 3).
- Produces:
  - `ProgramRecord(program_id, sas_path, owner, inputs: dict, ground_truth: dict, status="registered", error=None, failure_mode=None, float_rel_tol=None)` — `inputs` maps SAS `libref.table` → catalog table; `ground_truth` maps SAS output table name → ground-truth catalog table; `float_rel_tol` is the per-program tolerance override (None = config default).
  - `Inventory(store)`: `register(rec) -> bool` (False if already present — idempotent), `get(program_id) -> ProgramRecord | None`, `set_status(program_id, status, error=None, failure_mode=None)`, `pending() -> list[ProgramRecord]` (everything not `parity_pass`).
  - `STATUSES = ("registered", "landed", "translated", "validating", "parity_pass", "triage")` — `set_status` raises `ValueError` on unknown status.

- [ ] **Step 1: Write the failing tests**

`tests/test_inventory.py`:
```python
import pytest

from sas_migrate.inventory import Inventory, ProgramRecord
from sas_migrate.statestore import LocalJsonStateStore


def make_rec(pid="p1"):
    return ProgramRecord(
        program_id=pid, sas_path=f"/sas/{pid}.sas", owner="phil",
        inputs={"work.customers": "staging_inputs.customers_ab12"},
        ground_truth={"final_report": "ground_truth.p1_final_report"})


def test_register_is_idempotent(tmp_path):
    inv = Inventory(LocalJsonStateStore(str(tmp_path)))
    assert inv.register(make_rec()) is True
    assert inv.register(make_rec()) is False  # second call no-ops
    assert inv.get("p1").status == "registered"


def test_set_status_and_reload(tmp_path):
    inv = Inventory(LocalJsonStateStore(str(tmp_path)))
    inv.register(make_rec())
    inv.set_status("p1", "triage", error="diff on final_report",
                   failure_mode="diverged")
    rec = inv.get("p1")
    assert rec.status == "triage"
    assert rec.failure_mode == "diverged"


def test_set_status_rejects_unknown_status(tmp_path):
    inv = Inventory(LocalJsonStateStore(str(tmp_path)))
    inv.register(make_rec())
    with pytest.raises(ValueError):
        inv.set_status("p1", "done")


def test_pending_excludes_parity_pass(tmp_path):
    inv = Inventory(LocalJsonStateStore(str(tmp_path)))
    inv.register(make_rec("p1"))
    inv.register(make_rec("p2"))
    inv.set_status("p1", "parity_pass")
    assert [r.program_id for r in inv.pending()] == ["p2"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_inventory.py -v
```
Expected: collection error — no module `sas_migrate.inventory`.

- [ ] **Step 3: Implement `sas_migrate/inventory.py`**

```python
"""Program inventory: registration, status tracking, resumability.
A killed batch run resumes by processing Inventory.pending()."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from sas_migrate.statestore import StateStore

STATUSES = ("registered", "landed", "translated", "validating",
            "parity_pass", "triage")
TABLE = "inventory"


@dataclass
class ProgramRecord:
    program_id: str
    sas_path: str
    owner: str
    inputs: dict = field(default_factory=dict)        # sas libref.table -> catalog table
    ground_truth: dict = field(default_factory=dict)  # sas output name -> catalog table
    status: str = "registered"
    error: str | None = None
    failure_mode: str | None = None   # never_ran | diverged | budget | None
    float_rel_tol: float | None = None  # per-program override; None = config default


class Inventory:
    def __init__(self, store: StateStore):
        self.store = store

    def register(self, rec: ProgramRecord) -> bool:
        if self.store.get(TABLE, rec.program_id) is not None:
            return False
        self.store.upsert(TABLE, rec.program_id, asdict(rec))
        return True

    def get(self, program_id: str) -> ProgramRecord | None:
        row = self.store.get(TABLE, program_id)
        return ProgramRecord(**row) if row else None

    def set_status(self, program_id: str, status: str, error: str | None = None,
                   failure_mode: str | None = None) -> None:
        if status not in STATUSES:
            raise ValueError(f"unknown status {status!r}; expected one of {STATUSES}")
        rec = self.get(program_id)
        if rec is None:
            raise KeyError(f"program {program_id!r} not registered")
        rec.status, rec.error, rec.failure_mode = status, error, failure_mode
        self.store.upsert(TABLE, program_id, asdict(rec))

    def pending(self) -> list[ProgramRecord]:
        return [ProgramRecord(**row) for row in self.store.scan(TABLE)
                if row.get("status") != "parity_pass"]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_inventory.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: inventory with idempotent registration and resumable pending()"
```

---

### Task 5: Landing — SAS normalization, readers, snapshot hashing

**Files:**
- Create: `sas_migrate/landing.py`, `tests/test_landing.py`

**Interfaces:**
- Consumes: nothing internal (pandas only).
- Produces:
  - `SAS_EPOCH` (`datetime.date(1960, 1, 1)`)
  - `sas_date_to_iso(v) -> str | None` — SAS numeric date → `"YYYY-MM-DD"`; None/NaN → None
  - `normalize_value(v) -> object` — NaN / `"."` / `""` / whitespace-only strings → None; strings right-trimmed; other values passthrough
  - `normalize_frame(df: pd.DataFrame, date_cols: list[str] | None = None) -> pd.DataFrame` — applies `normalize_value` everywhere and `sas_date_to_iso` to `date_cols`
  - `content_hash(df: pd.DataFrame) -> str` — order-insensitive sha256 (first 12 hex chars) used in snapshot table names
  - `read_source(path: str) -> pd.DataFrame` — dispatches on extension: `.csv`, `.parquet` (zero-dep), `.sas7bdat` via guarded `pyreadstat` import raising `MissingDependencyError` with a JFrog-aware message when absent

- [ ] **Step 1: Write the failing tests**

`tests/test_landing.py`:
```python
import math

import pandas as pd
import pytest

from sas_migrate.landing import (
    MissingDependencyError, content_hash, normalize_frame, normalize_value,
    read_source, sas_date_to_iso,
)


def test_sas_date_epoch_is_1960():
    assert sas_date_to_iso(0) == "1960-01-01"
    assert sas_date_to_iso(24107) == "2026-01-01"
    assert sas_date_to_iso(None) is None
    assert sas_date_to_iso(float("nan")) is None


def test_normalize_value_missings_and_padding():
    assert normalize_value(float("nan")) is None
    assert normalize_value(".") is None
    assert normalize_value("   ") is None
    assert normalize_value("abc   ") == "abc"
    assert normalize_value(42) == 42


def test_normalize_frame_applies_dates_and_missings():
    df = pd.DataFrame({"d": [0.0, None], "name": ["bob  ", "."]})
    out = normalize_frame(df, date_cols=["d"])
    assert out["d"].tolist() == ["1960-01-01", None]
    assert out["name"].tolist() == ["bob", None]


def test_content_hash_is_order_insensitive():
    a = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    b = pd.DataFrame({"x": [2, 1], "y": ["b", "a"]})
    c = pd.DataFrame({"x": [1, 3], "y": ["a", "b"]})
    assert content_hash(a) == content_hash(b)
    assert content_hash(a) != content_hash(c)
    assert len(content_hash(a)) == 12


def test_read_source_csv(tmp_path):
    p = tmp_path / "in.csv"
    p.write_text("x,y\n1,a\n2,b\n")
    df = read_source(str(p))
    assert len(df) == 2


def test_read_source_sas7bdat_without_pyreadstat_raises_helpful_error(tmp_path):
    p = tmp_path / "in.sas7bdat"
    p.write_bytes(b"")
    with pytest.raises((MissingDependencyError, Exception)) as exc_info:
        read_source(str(p))
    # If pyreadstat IS installed the empty file fails differently; the
    # MissingDependencyError branch is what we assert on when it's absent.
    if isinstance(exc_info.value, MissingDependencyError):
        assert "pyreadstat" in str(exc_info.value)
        assert "CSV/Parquet" in str(exc_info.value)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_landing.py -v
```
Expected: collection error — no module `sas_migrate.landing`.

- [ ] **Step 3: Implement `sas_migrate/landing.py`**

```python
"""Landing utilities: SAS-quirk normalization applied to BOTH sides of every
comparison (spec section 3), source readers, and snapshot content hashing.
Zero-dependency path is CSV/Parquet exported from the SAS side; pyreadstat is
optional (JFrog-governed) for direct .sas7bdat reads."""
from __future__ import annotations

import hashlib
import math
from datetime import date, timedelta

import pandas as pd

SAS_EPOCH = date(1960, 1, 1)


class MissingDependencyError(Exception):
    pass


def sas_date_to_iso(v) -> str | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    return (SAS_EPOCH + timedelta(days=int(f))).isoformat()


def normalize_value(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str):
        s = v.rstrip()
        if s in ("", "."):
            return None
        return s
    return v


def normalize_frame(df: pd.DataFrame, date_cols: list[str] | None = None) -> pd.DataFrame:
    out = df.copy()
    for col in date_cols or []:
        out[col] = out[col].map(sas_date_to_iso)
    for col in out.columns:
        if col in (date_cols or []):
            continue
        out[col] = out[col].map(normalize_value)
    return out.astype(object).where(pd.notnull(out), None)


def content_hash(df: pd.DataFrame) -> str:
    """Order-insensitive content hash for snapshot naming (point-in-time id)."""
    lines = sorted(
        "\x1f".join("" if v is None or (isinstance(v, float) and math.isnan(v))
                    else str(v) for v in row)
        for row in df.itertuples(index=False, name=None))
    digest = hashlib.sha256("\n".join([",".join(df.columns), *lines]).encode())
    return digest.hexdigest()[:12]


def read_source(path: str) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    if lower.endswith(".parquet"):
        return pd.read_parquet(path)
    if lower.endswith(".sas7bdat"):
        try:
            import pyreadstat  # noqa: PLC0415 - optional, JFrog-governed
        except ImportError as e:
            raise MissingDependencyError(
                "Reading .sas7bdat requires pyreadstat, which needs JFrog approval. "
                "Zero-dependency fallback: export the dataset from SAS as CSV/Parquet "
                "(PROC EXPORT) and land that instead.") from e
        df, _meta = pyreadstat.read_sas7bdat(path)
        return df
    raise ValueError(f"unsupported source format: {path}")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_landing.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: landing with SAS normalization, readers, snapshot hashing"
```

---

### Task 6: Preprocessor — %include, %let, step splitting

**Files:**
- Create: `sas_migrate/preprocess.py`, `tests/test_preprocess.py`, `tests/fixtures/simple_etl.sas`, `tests/fixtures/included_macro.sas`

**Interfaces:**
- Produces:
  - `SasStep(index: int, kind: str, code: str)` — `kind` in `("global", "data", "proc", "macro")`; `"global"` steps (libname/options/title lines) are context only, never translated
  - `resolve_includes(source: str, base_dir: str, max_depth: int = 10) -> str`
  - `expand_lets(source: str) -> str` — substitutes `&name` / `&name.` from `%let name = value;` declarations, strips the `%let` lines
  - `split_steps(source: str) -> list[SasStep]`
  - `preprocess(path: str) -> tuple[list[SasStep], str]` — returns (steps, full expanded program text)

- [ ] **Step 1: Write fixtures**

`tests/fixtures/simple_etl.sas`:
```sas
options nodate;
libname mylib '/data/mylib';
%let cutoff = 2024-01-01;

data work.filtered;
  set mylib.customers;
  where signup_date >= "&cutoff."d;
run;

proc sql;
  create table work.summary as
  select region, count(*) as n
  from work.filtered
  group by region;
quit;

proc means data=work.filtered noprint;
  var balance;
  output out=work.stats mean=avg_balance;
run;
```

`tests/fixtures/included_macro.sas`:
```sas
%macro dedupe(tbl);
  proc sort data=&tbl nodupkey; by id; run;
%mend;
```

- [ ] **Step 2: Write the failing tests**

`tests/test_preprocess.py`:
```python
import os

from sas_migrate.preprocess import (
    expand_lets, preprocess, resolve_includes, split_steps,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def test_expand_lets_substitutes_and_strips():
    src = "%let cutoff = 2024-01-01;\nwhere d >= \"&cutoff.\"d and x > &cutoff;\n"
    out = expand_lets(src)
    assert '"2024-01-01"d' in out
    assert "x > 2024-01-01" in out
    assert "%let" not in out


def test_resolve_includes_inlines_file():
    src = "%include 'included_macro.sas';\ndata a; set b; run;\n"
    out = resolve_includes(src, base_dir=FIXTURES)
    assert "%macro dedupe" in out
    assert "%include" not in out


def test_split_steps_finds_boundaries_and_kinds():
    steps, _ = preprocess(os.path.join(FIXTURES, "simple_etl.sas"))
    kinds = [s.kind for s in steps]
    assert kinds == ["global", "data", "proc", "proc"]
    assert "create table work.summary" in steps[2].code
    assert steps[1].index == 1


def test_macro_block_is_one_step():
    with open(os.path.join(FIXTURES, "included_macro.sas")) as f:
        steps = split_steps(f.read())
    assert len(steps) == 1
    assert steps[0].kind == "macro"
    assert "%mend" in steps[0].code
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_preprocess.py -v
```
Expected: collection error — no module `sas_migrate.preprocess`.

- [ ] **Step 4: Implement `sas_migrate/preprocess.py`**

```python
"""Deterministic preprocessing — no SAS parser. Resolve %include (file I/O the
LLM cannot do), expand simple %let variables (mechanical bookkeeping the LLM is
bad at), split at DATA/PROC boundaries (step-scoped LLM calls raise accuracy
and make repair attributable). Complex %macro bodies pass through whole."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass

INCLUDE_RE = re.compile(r"%include\s+['\"]([^'\"]+)['\"]\s*;", re.IGNORECASE)
LET_RE = re.compile(r"^\s*%let\s+(\w+)\s*=\s*([^;]*);\s*$", re.IGNORECASE | re.MULTILINE)
STEP_START_RE = re.compile(r"^\s*(data\b|proc\s+\w+|%macro\b)", re.IGNORECASE)
STEP_END_RE = re.compile(r"^\s*(run|quit)\s*;", re.IGNORECASE)
MACRO_END_RE = re.compile(r"^\s*%mend\b.*;", re.IGNORECASE)


@dataclass
class SasStep:
    index: int
    kind: str   # global | data | proc | macro
    code: str


def resolve_includes(source: str, base_dir: str, max_depth: int = 10) -> str:
    if max_depth <= 0:
        raise RecursionError("%include nesting exceeds max depth")

    def repl(m: re.Match) -> str:
        path = m.group(1)
        if not os.path.isabs(path):
            path = os.path.join(base_dir, path)
        with open(path) as f:
            return resolve_includes(f.read(), os.path.dirname(path), max_depth - 1)

    return INCLUDE_RE.sub(repl, source)


def expand_lets(source: str) -> str:
    lets = {name: value.strip() for name, value in LET_RE.findall(source)}
    out = LET_RE.sub("", source)
    # Longest name first so &prefix doesn't clobber &prefixlonger.
    for name in sorted(lets, key=len, reverse=True):
        out = re.sub(rf"&{re.escape(name)}\.?", lets[name].replace("\\", "\\\\"),
                     out, flags=re.IGNORECASE)
    return out


def split_steps(source: str) -> list[SasStep]:
    steps: list[SasStep] = []
    current: list[str] = []
    kind = "global"

    def flush():
        nonlocal current, kind
        code = "\n".join(current).strip()
        if code:
            steps.append(SasStep(index=len(steps), kind=kind, code=code))
        current, kind = [], "global"

    for line in source.splitlines():
        start = STEP_START_RE.match(line)
        if start and kind == "global":
            flush()
            token = start.group(1).lower()
            kind = "macro" if token.startswith("%macro") else \
                   "data" if token.startswith("data") else "proc"
        current.append(line)
        if kind == "macro" and MACRO_END_RE.match(line):
            flush()
        elif kind in ("data", "proc") and STEP_END_RE.match(line):
            flush()
    flush()
    return steps


def preprocess(path: str) -> tuple[list[SasStep], str]:
    with open(path) as f:
        source = f.read()
    expanded = expand_lets(resolve_includes(source, os.path.dirname(path)))
    return split_steps(expanded), expanded
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_preprocess.py -v
```
Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: deterministic preprocessor (include, let, step splitting)"
```

---

### Task 7: Translator prompts (assembly only)

**Files:**
- Create: `sas_migrate/translate.py` (prompt half), `tests/test_translate_prompts.py`

**Interfaces:**
- Consumes: `SasStep` (Task 6).
- Produces:
  - `CRIBSHEET: str` — SAS↔Spark gotcha reference embedded in every translation prompt
  - `SYSTEM_PROMPT: str`
  - `build_translation_prompt(step: SasStep, full_program: str, table_schemas: dict[str, str], input_mappings: dict[str, str], sandbox_schema: str) -> list[dict]` — returns `messages` for `BaseGateway.complete`; `table_schemas` maps table name → DDL-ish schema string
  - `build_run_repair_prompt(code: str, language: str, error_text: str) -> list[dict]`
  - `build_match_repair_prompt(code: str, language: str, diff_text: str) -> list[dict]`

- [ ] **Step 1: Write the failing tests**

`tests/test_translate_prompts.py`:
```python
from sas_migrate.preprocess import SasStep
from sas_migrate.translate import (
    CRIBSHEET, SYSTEM_PROMPT, build_match_repair_prompt,
    build_run_repair_prompt, build_translation_prompt,
)


def _prompt_text(messages):
    return "\n".join(m["content"] for m in messages)


def test_translation_prompt_contains_step_context_and_rules():
    step = SasStep(index=2, kind="proc", code="proc sql;\ncreate table work.s as select 1;\nquit;")
    messages = build_translation_prompt(
        step, full_program="/* whole program here */",
        table_schemas={"staging_inputs.customers_ab12": "id BIGINT, region STRING"},
        input_mappings={"mylib.customers": "staging_inputs.customers_ab12"},
        sandbox_schema="sandbox_p1")
    text = _prompt_text(messages)
    assert "create table work.s" in text          # the step being translated
    assert "/* whole program here */" in text      # full program as reference
    assert "id BIGINT, region STRING" in text      # live schemas
    assert "mylib.customers" in text               # libref mapping
    assert "sandbox_p1" in text                    # write target rule
    assert "1960" in CRIBSHEET                     # date epoch gotcha present
    assert "Spark SQL" in SYSTEM_PROMPT


def test_run_repair_prompt_includes_traceback():
    messages = build_run_repair_prompt("SELECT bad", "sql", "AnalysisException: bad")
    text = _prompt_text(messages)
    assert "AnalysisException" in text and "SELECT bad" in text


def test_match_repair_prompt_includes_diff():
    messages = build_match_repair_prompt("SELECT 1", "sql", "col n: 4 mismatches (0.4%)")
    text = _prompt_text(messages)
    assert "4 mismatches" in text and "SELECT 1" in text
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_translate_prompts.py -v
```
Expected: collection error — no module `sas_migrate.translate`.

- [ ] **Step 3: Implement the prompt half of `sas_migrate/translate.py`**

```python
"""Translation prompts and (Task 8) response parsing + Translator.
Step-scoped calls: the LLM sees the whole program as reference but is asked to
translate exactly one step. Output contract: a ```json header block then one
```sql or ```python code block."""
from __future__ import annotations

from sas_migrate.preprocess import SasStep

CRIBSHEET = """\
SAS -> Spark gotchas you MUST account for:
- SAS dates are days since 1960-01-01 (Spark: days since 1970-01-01). Landed \
tables already store ISO dates; never re-apply epoch offsets.
- SAS missing (.) sorts LOWEST and compares as less-than everything; Spark NULL \
comparisons yield NULL. Rewrite predicates so missing/NULL semantics match SAS.
- DATA step implicit loop with RETAIN keeps values across rows; translate to \
window functions (e.g., last(col, ignoreNulls) over ordered window) or explicit joins.
- FIRST.var / LAST.var -> row_number() over (partition by var order by ...) = 1 \
(or descending for LAST).
- PROC MEANS/SUMMARY default statistics are n, mean, std, min, max; NOPRINT + \
OUTPUT OUT= creates a table that includes _TYPE_ and _FREQ_ columns.
- PROC SORT NODUPKEY keeps the FIRST occurrence -> row_number()=1 pattern, not \
dropDuplicates (which is nondeterministic about which row it keeps).
- SAS character comparisons ignore trailing blanks; landed tables are \
right-trimmed, so use plain equality.
- Numeric/character implicit conversion is automatic in SAS; make every cast \
explicit in Spark SQL.
"""

SYSTEM_PROMPT = """\
You are an expert SAS-to-Databricks migration engineer. Translate one SAS step
at a time into Spark SQL (strongly preferred) or PySpark (only when relational
SQL cannot express the logic, e.g. iterative macro logic or statistical procs).

Rules:
- Emit exactly two fenced blocks: first a ```json header, then one ```sql or
  ```python code block. No prose outside the blocks.
- Header format: {"language": "sql"|"pyspark", "inputs": [tables read],
  "outputs": [tables written]}
- Write output tables ONLY into the sandbox schema you are given. Read inputs
  ONLY from the mapped tables you are given.
- SQL may contain multiple statements separated by semicolons. PySpark code
  receives a `spark` SparkSession variable.
- Preserve SAS semantics exactly; the output will be diffed cell-by-cell
  against SAS ground truth.

""" + CRIBSHEET


def _schemas_block(table_schemas: dict[str, str]) -> str:
    if not table_schemas:
        return "(no table schemas available)"
    return "\n".join(f"- {t}: {s}" for t, s in sorted(table_schemas.items()))


def build_translation_prompt(step: SasStep, full_program: str,
                             table_schemas: dict[str, str],
                             input_mappings: dict[str, str],
                             sandbox_schema: str) -> list[dict]:
    mappings = "\n".join(f"- SAS `{k}` -> `{v}`" for k, v in sorted(input_mappings.items())) \
               or "(none)"
    content = f"""\
Translate STEP {step.index} (kind={step.kind}) of the SAS program below.

## SAS libref -> catalog table mappings
{mappings}

## Live schemas of available tables (including tables created by earlier steps)
{_schemas_block(table_schemas)}

## Sandbox schema (ALL outputs must be written here)
{sandbox_schema}

## Full original program (REFERENCE ONLY — translate only the step)
```sas
{full_program}
```

## The step to translate
```sas
{step.code}
```"""
    return [{"role": "user", "content": content}]


def build_run_repair_prompt(code: str, language: str, error_text: str) -> list[dict]:
    content = f"""\
The following generated {language} code failed to execute. Fix it and return
the same two-block format (```json header then code block). Do not change
which tables it reads or writes.

## Code
```{language}
{code}
```

## Execution error
```
{error_text}
```"""
    return [{"role": "user", "content": content}]


def build_match_repair_prompt(code: str, language: str, diff_text: str) -> list[dict]:
    content = f"""\
The following generated {language} code runs, but its output does NOT match
the SAS ground truth. Analyze the diff, find the semantic divergence from SAS
behavior (check the gotcha list), and return corrected code in the same
two-block format. Do not change which tables it reads or writes.

## Code
```{language}
{code}
```

## Diff vs SAS ground truth
```
{diff_text}
```"""
    return [{"role": "user", "content": content}]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_translate_prompts.py -v
```
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: translation and repair prompt assembly with SAS gotcha cribsheet"
```

---

### Task 8: Translator — response parsing + Translator class

**Files:**
- Modify: `sas_migrate/translate.py` (append)
- Create: `tests/test_translate_parse.py`

**Interfaces:**
- Consumes: `BaseGateway` (Task 2), prompts (Task 7), `TokenBudget` (Task 2).
- Produces:
  - `TranslatedStep(step_index: int, language: str, code: str, inputs: list[str], outputs: list[str])`
  - `TranslationParseError(Exception)` — carries `.raw`
  - `parse_translation_response(text: str, step_index: int) -> TranslatedStep`
  - `Translator(gateway, config, budget: TokenBudget)`:
    - `translate(step, full_program, table_schemas, input_mappings, sandbox_schema, program_id) -> TranslatedStep`
    - `repair_run(tstep, error_text, program_id) -> TranslatedStep`
    - `repair_match(tstep, diff_text, program_id) -> TranslatedStep`
    - All three charge `budget` with input+output tokens; a parse failure retries once with the parse error appended, then raises `TranslationParseError`.

- [ ] **Step 1: Write the failing tests**

`tests/test_translate_parse.py`:
```python
import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import MockGateway, TokenBudget
from sas_migrate.preprocess import SasStep
from sas_migrate.translate import (
    TranslationParseError, Translator, parse_translation_response,
)

GOOD = """\
```json
{"language": "sql", "inputs": ["staging_inputs.customers_ab12"], "outputs": ["sandbox_p1.filtered"]}
```
```sql
CREATE TABLE sandbox_p1.filtered AS
SELECT * FROM staging_inputs.customers_ab12 WHERE signup_date >= '2024-01-01';
```"""

GOOD_PYSPARK = """\
```json
{"language": "pyspark", "inputs": [], "outputs": ["sandbox_p1.stats"]}
```
```python
df = spark.table("sandbox_p1.filtered")
df.describe().write.saveAsTable("sandbox_p1.stats")
```"""


def test_parse_good_sql_response():
    t = parse_translation_response(GOOD, step_index=1)
    assert t.language == "sql"
    assert t.outputs == ["sandbox_p1.filtered"]
    assert "CREATE TABLE sandbox_p1.filtered" in t.code
    assert t.step_index == 1


def test_parse_pyspark_response():
    t = parse_translation_response(GOOD_PYSPARK, step_index=2)
    assert t.language == "pyspark"
    assert 'spark.table("sandbox_p1.filtered")' in t.code


def test_parse_missing_header_raises():
    with pytest.raises(TranslationParseError):
        parse_translation_response("```sql\nSELECT 1;\n```", step_index=0)


def _translator(responses):
    return Translator(MockGateway(responses), MigrationConfig(),
                      TokenBudget(500_000))


def test_translator_translate_returns_step():
    tr = _translator([GOOD])
    step = SasStep(index=1, kind="data", code="data f; set c; run;")
    t = tr.translate(step, "prog", {}, {}, "sandbox_p1", "p1")
    assert t.language == "sql"


def test_translator_retries_parse_failure_once_then_raises():
    tr = _translator(["not parseable", "still not parseable"])
    step = SasStep(index=0, kind="data", code="data a; run;")
    with pytest.raises(TranslationParseError):
        tr.translate(step, "prog", {}, {}, "sandbox_p1", "p1")
    assert len(tr.gateway.calls) == 2  # retried exactly once


def test_translator_charges_budget():
    tr = _translator([GOOD])
    step = SasStep(index=0, kind="data", code="data a; run;")
    tr.translate(step, "prog", {}, {}, "sandbox_p1", "p1")
    assert tr.budget.used > 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_translate_parse.py -v
```
Expected: ImportError — `parse_translation_response` etc. not defined.

- [ ] **Step 3: Append to `sas_migrate/translate.py`**

```python
import json
import re
from dataclasses import dataclass

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import BaseGateway, TokenBudget

JSON_BLOCK_RE = re.compile(r"```json\s*\n(.*?)```", re.DOTALL)
CODE_BLOCK_RE = re.compile(r"```(sql|python|pyspark)\s*\n(.*?)```", re.DOTALL)


class TranslationParseError(Exception):
    def __init__(self, msg: str, raw: str = ""):
        super().__init__(msg)
        self.raw = raw


@dataclass
class TranslatedStep:
    step_index: int
    language: str            # "sql" | "pyspark"
    code: str
    inputs: list[str]
    outputs: list[str]


def parse_translation_response(text: str, step_index: int) -> TranslatedStep:
    header_m = JSON_BLOCK_RE.search(text)
    if not header_m:
        raise TranslationParseError("missing ```json header block", raw=text)
    try:
        header = json.loads(header_m.group(1))
    except json.JSONDecodeError as e:
        raise TranslationParseError(f"bad json header: {e}", raw=text) from e
    code_m = CODE_BLOCK_RE.search(text, header_m.end())
    if not code_m:
        raise TranslationParseError("missing code block after header", raw=text)
    language = header.get("language", "")
    if language not in ("sql", "pyspark"):
        raise TranslationParseError(f"language must be sql|pyspark, got {language!r}",
                                    raw=text)
    return TranslatedStep(step_index=step_index, language=language,
                          code=code_m.group(2).strip(),
                          inputs=list(header.get("inputs", [])),
                          outputs=list(header.get("outputs", [])))


class Translator:
    def __init__(self, gateway: BaseGateway, config: MigrationConfig,
                 budget: TokenBudget):
        self.gateway = gateway
        self.config = config
        self.budget = budget

    def _call(self, messages: list[dict], step_index: int, purpose: str,
              program_id: str) -> TranslatedStep:
        resp = self.gateway.complete(SYSTEM_PROMPT, messages,
                                     model=self.config.default_model,
                                     purpose=purpose, program_id=program_id)
        self.budget.charge(resp.input_tokens + resp.output_tokens)
        try:
            return parse_translation_response(resp.text, step_index)
        except TranslationParseError as first_err:
            retry = messages + [
                {"role": "assistant", "content": resp.text},
                {"role": "user", "content":
                    f"Your response could not be parsed: {first_err}. Reply again "
                    "with EXACTLY one ```json header block then one code block."}]
            resp2 = self.gateway.complete(SYSTEM_PROMPT, retry,
                                          model=self.config.default_model,
                                          purpose=purpose + "_reparse",
                                          program_id=program_id)
            self.budget.charge(resp2.input_tokens + resp2.output_tokens)
            return parse_translation_response(resp2.text, step_index)

    def translate(self, step, full_program, table_schemas, input_mappings,
                  sandbox_schema, program_id) -> TranslatedStep:
        messages = build_translation_prompt(step, full_program, table_schemas,
                                            input_mappings, sandbox_schema)
        return self._call(messages, step.index, "translate", program_id)

    def repair_run(self, tstep: TranslatedStep, error_text: str,
                   program_id: str) -> TranslatedStep:
        messages = build_run_repair_prompt(tstep.code, tstep.language, error_text)
        return self._call(messages, tstep.step_index, "repair_run", program_id)

    def repair_match(self, tstep: TranslatedStep, diff_text: str,
                     program_id: str) -> TranslatedStep:
        messages = build_match_repair_prompt(tstep.code, tstep.language, diff_text)
        return self._call(messages, tstep.step_index, "repair_match", program_id)
```

- [ ] **Step 4: Run all translate tests to verify they pass**

```bash
.venv/bin/pytest tests/test_translate_parse.py tests/test_translate_prompts.py -v
```
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: translation response parsing and Translator with budget + reparse retry"
```

---

### Task 9: Validator part 1 — value comparison + conftest Spark fixture

**Files:**
- Create: `sas_migrate/validate.py` (pure half), `tests/test_validate_values.py`, `tests/conftest.py`

**Interfaces:**
- Produces:
  - `values_match(a, b, rel_tol: float = 1e-9) -> bool` — None==None true; NaN==NaN true; floats via relative tolerance (abs_tol 1e-12 backstop); everything else `==`
  - `ColumnDiff(column: str, mismatches: int, total: int, samples: list[str])` with `.rate` property
  - `TableDiff(table: str, gt_rows: int, out_rows: int, missing_rows: int, extra_rows: int, column_diffs: list[ColumnDiff], error: str | None = None)` with `.passed` property
  - `DiffReport(program_id: str, table_diffs: list[TableDiff])` with `.passed` property and `.to_text(max_chars: int = 6000) -> str` (the exact text fed to the repair LLM)
  - `tests/conftest.py` provides session-scoped `spark` fixture (local mode) for Tasks 10–11

- [ ] **Step 1: Write the failing tests**

`tests/test_validate_values.py`:
```python
from sas_migrate.validate import ColumnDiff, DiffReport, TableDiff, values_match


def test_values_match_nulls_and_exact():
    assert values_match(None, None)
    assert not values_match(None, 0)
    assert values_match("abc", "abc")
    assert not values_match("abc", "abd")
    assert values_match(7, 7)


def test_values_match_float_relative_tolerance():
    assert values_match(1000000.0, 1000000.0000001, rel_tol=1e-9)   # within
    assert not values_match(1000000.0, 1000000.01, rel_tol=1e-9)    # outside
    assert values_match(float("nan"), float("nan"))
    assert values_match(0.0, 1e-13)  # abs_tol backstop near zero


def test_table_diff_passed_logic():
    ok = TableDiff("t", 10, 10, 0, 0, [ColumnDiff("x", 0, 10, [])])
    bad = TableDiff("t", 10, 10, 0, 0, [ColumnDiff("x", 3, 10, ["r1: 1 != 2"])])
    counts = TableDiff("t", 10, 9, 1, 0, [])
    assert ok.passed and not bad.passed and not counts.passed


def test_diff_report_to_text_mentions_columns_and_truncates():
    d = DiffReport("p1", [TableDiff("t", 10, 10, 0, 0,
                                    [ColumnDiff("balance", 4, 10, ["id=3: 1.0 != 2.0"])])])
    text = d.to_text()
    assert "balance" in text and "4/10" in text and "id=3" in text
    assert not d.passed
    assert len(d.to_text(max_chars=100)) <= 120  # truncation honored (+ marker)
```

- [ ] **Step 2: Write `tests/conftest.py`** (needed from Task 10 on; adding now keeps one commit)

```python
import tempfile

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    # Fresh warehouse dir per session: leftover table paths from a previous
    # run would make saveAsTable fail with "path already exists".
    s = (SparkSession.builder.master("local[2]")
         .appName("sas2dbx-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="sas2dbx-wh-"))
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_validate_values.py -v
```
Expected: collection error — no module `sas_migrate.validate`.

- [ ] **Step 4: Implement pure half of `sas_migrate/validate.py`**

```python
"""Parity validation — the trust anchor. Pure comparison logic here; Spark
comparison driver appended in Task 10."""
from __future__ import annotations

import math
from dataclasses import dataclass, field


def values_match(a, b, rel_tol: float = 1e-9) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return a == b
        if math.isnan(fa) and math.isnan(fb):
            return True
        return math.isclose(fa, fb, rel_tol=rel_tol, abs_tol=1e-12)
    return a == b


@dataclass
class ColumnDiff:
    column: str
    mismatches: int
    total: int
    samples: list[str] = field(default_factory=list)

    @property
    def rate(self) -> float:
        return self.mismatches / self.total if self.total else 0.0


@dataclass
class TableDiff:
    table: str
    gt_rows: int
    out_rows: int
    missing_rows: int   # in ground truth, absent from output
    extra_rows: int     # in output, absent from ground truth
    column_diffs: list[ColumnDiff] = field(default_factory=list)
    error: str | None = None

    @property
    def passed(self) -> bool:
        return (self.error is None and self.gt_rows == self.out_rows
                and self.missing_rows == 0 and self.extra_rows == 0
                and all(c.mismatches == 0 for c in self.column_diffs))


@dataclass
class DiffReport:
    program_id: str
    table_diffs: list[TableDiff] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.table_diffs) and all(t.passed for t in self.table_diffs)

    def to_text(self, max_chars: int = 6000) -> str:
        lines: list[str] = []
        for t in self.table_diffs:
            status = "PASS" if t.passed else "FAIL"
            lines.append(f"[{status}] table {t.table}: ground_truth={t.gt_rows} rows, "
                         f"output={t.out_rows} rows, missing={t.missing_rows}, "
                         f"extra={t.extra_rows}")
            if t.error:
                lines.append(f"  error: {t.error}")
            for c in t.column_diffs:
                if c.mismatches:
                    lines.append(f"  column {c.column}: {c.mismatches}/{c.total} "
                                 f"mismatched ({c.rate:.2%})")
                    lines.extend(f"    sample: {s}" for s in c.samples[:5])
        text = "\n".join(lines) or "(no tables compared)"
        if len(text) > max_chars:
            text = text[:max_chars] + "\n...(truncated)"
        return text
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_validate_values.py -v
```
Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: value comparison and DiffReport structures; local spark test fixture"
```

---

### Task 10: Validator part 2 — Spark comparison driver

**Files:**
- Modify: `sas_migrate/validate.py` (append)
- Create: `tests/test_validate_spark.py`

**Interfaces:**
- Consumes: `spark` fixture (Task 9), structures above.
- Produces:
  - `compare_tables(spark, gt_table: str, out_table: str, keys: list[str] | None = None, rel_tol: float = 1e-9, sample_limit: int = 5) -> TableDiff`
    - Keyed path: full outer join on `keys`, per-column mismatch counts using true relative tolerance for float columns, samples like `"id=3: balance 1.0 != 2.0"`.
    - Keyless path: rows hashed with floats normalized via `format_string('%.9e')` (documented proxy for rel tolerance), multiset diff on hashes; column_diffs empty, missing/extra counts populated.
    - Schema mismatch (column sets differ after lowercase normalization) → `TableDiff.error` set, no comparison attempted.
  - `validate_program(spark, program_id: str, table_pairs: list[tuple[str, str, list[str] | None]], rel_tol: float = 1e-9) -> DiffReport` — `table_pairs` is `(gt_table, out_table, keys)`

  **Validator self-test requirement (spec §6):** the tests below are the corruption-injection tests — dropped row, perturbed float beyond tolerance, swapped value — each must be caught.

- [ ] **Step 1: Write the failing tests**

`tests/test_validate_spark.py`:
```python
import pytest

from sas_migrate.validate import compare_tables, validate_program


@pytest.fixture()
def base_tables(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS vt")
    spark.sql("DROP TABLE IF EXISTS vt.gt")
    spark.createDataFrame(
        [(1, "east", 100.0), (2, "west", 200.5), (3, "east", 300.25)],
        "id INT, region STRING, balance DOUBLE").write.saveAsTable("vt.gt")
    yield
    for t in ("gt", "out"):
        spark.sql(f"DROP TABLE IF EXISTS vt.{t}")


def _write_out(spark, rows):
    spark.sql("DROP TABLE IF EXISTS vt.out")
    spark.createDataFrame(rows, "id INT, region STRING, balance DOUBLE") \
         .write.saveAsTable("vt.out")


def test_identical_tables_pass_keyed_and_keyless(spark, base_tables):
    _write_out(spark, [(3, "east", 300.25), (1, "east", 100.0), (2, "west", 200.5)])
    assert compare_tables(spark, "vt.gt", "vt.out", keys=["id"]).passed
    assert compare_tables(spark, "vt.gt", "vt.out", keys=None).passed  # order-insensitive


def test_float_within_tolerance_passes_keyed(spark, base_tables):
    # Perturb by 1e-5: relative error 1e-7 — outside 1e-9 tolerance, inside 1e-6.
    _write_out(spark, [(1, "east", 100.0 + 1e-5), (2, "west", 200.5), (3, "east", 300.25)])
    assert compare_tables(spark, "vt.gt", "vt.out", keys=["id"], rel_tol=1e-9).passed is False
    assert compare_tables(spark, "vt.gt", "vt.out", keys=["id"], rel_tol=1e-6).passed


def test_corruption_dropped_row_is_caught(spark, base_tables):
    _write_out(spark, [(1, "east", 100.0), (2, "west", 200.5)])
    d = compare_tables(spark, "vt.gt", "vt.out", keys=["id"])
    assert not d.passed and d.missing_rows == 1


def test_corruption_perturbed_float_is_caught_with_sample(spark, base_tables):
    _write_out(spark, [(1, "east", 100.0), (2, "west", 999.9), (3, "east", 300.25)])
    d = compare_tables(spark, "vt.gt", "vt.out", keys=["id"])
    col = next(c for c in d.column_diffs if c.column == "balance")
    assert col.mismatches == 1
    assert any("id=2" in s for s in col.samples)


def test_corruption_swapped_value_is_caught_keyless(spark, base_tables):
    _write_out(spark, [(1, "west", 100.0), (2, "east", 200.5), (3, "east", 300.25)])
    d = compare_tables(spark, "vt.gt", "vt.out", keys=None)
    assert not d.passed and d.missing_rows == 2 and d.extra_rows == 2


def test_schema_mismatch_reports_error(spark, base_tables):
    spark.sql("DROP TABLE IF EXISTS vt.out")
    spark.createDataFrame([(1, "east")], "id INT, region STRING") \
         .write.saveAsTable("vt.out")
    d = compare_tables(spark, "vt.gt", "vt.out")
    assert d.error and "balance" in d.error


def test_validate_program_aggregates(spark, base_tables):
    _write_out(spark, [(1, "east", 100.0), (2, "west", 200.5), (3, "east", 300.25)])
    report = validate_program(spark, "p1", [("vt.gt", "vt.out", ["id"])])
    assert report.passed and report.program_id == "p1"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_validate_spark.py -v
```
Expected: ImportError — `compare_tables` not defined. (First run downloads nothing; local Spark start takes ~30s.)

- [ ] **Step 3: Append Spark driver to `sas_migrate/validate.py`**

```python
FLOAT_TYPES = ("double", "float", "decimal")


def _columns(spark, table: str) -> dict[str, str]:
    return {f.name.lower(): f.dataType.simpleString()
            for f in spark.table(table).schema.fields}


def _is_float(dtype: str) -> bool:
    return any(dtype.startswith(t) for t in FLOAT_TYPES)


def _hash_expr(cols: dict[str, str]) -> str:
    parts = []
    for name, dtype in sorted(cols.items()):
        if _is_float(dtype):
            parts.append(f"coalesce(format_string('%.9e', `{name}`), '\\x00')")
        else:
            parts.append(f"coalesce(cast(`{name}` AS STRING), '\\x00')")
    return f"sha2(concat_ws('\\x1f', {', '.join(parts)}), 256)"


def compare_tables(spark, gt_table: str, out_table: str,
                   keys: list[str] | None = None, rel_tol: float = 1e-9,
                   sample_limit: int = 5) -> TableDiff:
    gt_cols, out_cols = _columns(spark, gt_table), _columns(spark, out_table)
    if set(gt_cols) != set(out_cols):
        only_gt = sorted(set(gt_cols) - set(out_cols))
        only_out = sorted(set(out_cols) - set(gt_cols))
        return TableDiff(out_table, 0, 0, 0, 0, error=(
            f"schema mismatch: ground-truth-only columns {only_gt}, "
            f"output-only columns {only_out}"))
    gt_rows = spark.table(gt_table).count()
    out_rows = spark.table(out_table).count()

    if keys:
        return _compare_keyed(spark, gt_table, out_table, gt_cols, keys,
                              rel_tol, sample_limit, gt_rows, out_rows)
    return _compare_keyless(spark, gt_table, out_table, gt_cols,
                            gt_rows, out_rows)


def _compare_keyed(spark, gt_table, out_table, cols, keys, rel_tol,
                   sample_limit, gt_rows, out_rows) -> TableDiff:
    keys = [k.lower() for k in keys]
    on = " AND ".join(f"g.`{k}` <=> o.`{k}`" for k in keys)
    joined = f"(SELECT * FROM {gt_table}) g FULL OUTER JOIN (SELECT * FROM {out_table}) o ON {on}"
    anchor = keys[0]
    missing = spark.sql(f"SELECT count(*) AS n FROM {joined} WHERE o.`{anchor}` IS NULL "
                        f"AND g.`{anchor}` IS NOT NULL").collect()[0]["n"]
    extra = spark.sql(f"SELECT count(*) AS n FROM {joined} WHERE g.`{anchor}` IS NULL "
                      f"AND o.`{anchor}` IS NOT NULL").collect()[0]["n"]

    column_diffs: list[ColumnDiff] = []
    matched_filter = f"g.`{anchor}` IS NOT NULL AND o.`{anchor}` IS NOT NULL"
    total = spark.sql(f"SELECT count(*) AS n FROM {joined} WHERE {matched_filter}") \
                 .collect()[0]["n"]
    for name, dtype in sorted(cols.items()):
        if name in keys:
            continue
        if _is_float(dtype):
            mismatch = (f"NOT (g.`{name}` <=> o.`{name}`) AND NOT ("
                        f"g.`{name}` IS NOT NULL AND o.`{name}` IS NOT NULL AND "
                        f"abs(g.`{name}` - o.`{name}`) <= "
                        f"greatest(abs(g.`{name}`), abs(o.`{name}`)) * {rel_tol} + 1e-12)")
        else:
            mismatch = f"NOT (g.`{name}` <=> o.`{name}`)"
        rows = spark.sql(
            f"SELECT g.`{anchor}` AS k, g.`{name}` AS gv, o.`{name}` AS ov "
            f"FROM {joined} WHERE {matched_filter} AND ({mismatch}) "
            f"LIMIT {sample_limit + 1}").collect()
        if rows:
            count = spark.sql(f"SELECT count(*) AS n FROM {joined} "
                              f"WHERE {matched_filter} AND ({mismatch})").collect()[0]["n"]
            samples = [f"{anchor}={r['k']}: {name} {r['gv']} != {r['ov']}"
                       for r in rows[:sample_limit]]
            column_diffs.append(ColumnDiff(name, count, total, samples))
        else:
            column_diffs.append(ColumnDiff(name, 0, total))
    return TableDiff(out_table, gt_rows, out_rows, missing, extra, column_diffs)


def _compare_keyless(spark, gt_table, out_table, cols, gt_rows, out_rows) -> TableDiff:
    """Multiset diff on row hashes. Float columns are normalized to 10
    significant digits (%.9e) before hashing — the documented tolerance proxy
    for keyless comparison; keyed comparison uses true relative tolerance."""
    h = _hash_expr(cols)
    diff = spark.sql(f"""
        WITH g AS (SELECT {h} AS h, count(*) AS n FROM {gt_table} GROUP BY 1),
             o AS (SELECT {h} AS h, count(*) AS n FROM {out_table} GROUP BY 1)
        SELECT coalesce(g.n, 0) AS gn, coalesce(o.n, 0) AS onn
        FROM g FULL OUTER JOIN o ON g.h = o.h
        WHERE coalesce(g.n, 0) != coalesce(o.n, 0)""").collect()
    missing = sum(max(r["gn"] - r["onn"], 0) for r in diff)
    extra = sum(max(r["onn"] - r["gn"], 0) for r in diff)
    return TableDiff(out_table, gt_rows, out_rows, missing, extra)


def validate_program(spark, program_id: str,
                     table_pairs: list[tuple[str, str, list[str] | None]],
                     rel_tol: float = 1e-9) -> DiffReport:
    return DiffReport(program_id, [
        compare_tables(spark, gt, out, keys=keys, rel_tol=rel_tol)
        for gt, out, keys in table_pairs])
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_validate_spark.py -v
```
Expected: 7 passed (allow ~1 min for local Spark).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: Spark parity comparison with keyed tolerance and keyless hash diff"
```

---

### Task 11: Executor — sandboxed step execution

**Files:**
- Create: `sas_migrate/execute.py`, `tests/test_execute.py`

**Interfaces:**
- Consumes: `TranslatedStep` (Task 8), `MigrationConfig` (Task 1), `spark`.
- Produces:
  - `StepResult(step_index: int, ok: bool, error: str | None, duration_s: float)`
  - `SandboxViolation(Exception)`
  - `check_sandbox(code: str, sandbox_schema: str) -> None` — regex scan for write statements (`CREATE [OR REPLACE] TABLE/VIEW`, `INSERT INTO/OVERWRITE`, `MERGE INTO`, `DROP TABLE`, `TRUNCATE TABLE`, `.saveAsTable("...")`) whose qualified target is outside `sandbox_schema`; unqualified targets allowed (current schema is the sandbox)
  - `Executor(spark, config, program_id)`:
    - `.reset()` — drop + recreate the sandbox schema (called at the start of every outer repair attempt)
    - `.run_step(tstep) -> StepResult` — sandbox check, then SQL statements split on top-level `;` and run via `spark.sql`, or PySpark run via `exec` with `{"spark": spark}`; all exceptions captured as `StepResult(ok=False, error=<full traceback>)`
    - Timeout: statements run inside a Spark job group; a watchdog thread calls `cancelJobGroup` after `config.step_timeout_seconds` (verified in-tenant; local test covers duration recording only)
  - `split_sql(code: str) -> list[str]` — splits on `;` outside single-quoted strings

- [ ] **Step 1: Write the failing tests**

`tests/test_execute.py`:
```python
import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.execute import Executor, SandboxViolation, check_sandbox, split_sql
from sas_migrate.translate import TranslatedStep


def test_split_sql_respects_quoted_semicolons():
    stmts = split_sql("SELECT 'a;b' AS x; CREATE TABLE t AS SELECT 1;")
    assert len(stmts) == 2
    assert stmts[0] == "SELECT 'a;b' AS x"


def test_check_sandbox_allows_sandbox_and_unqualified_writes():
    check_sandbox("CREATE TABLE sandbox_p1.out AS SELECT 1", "sandbox_p1")
    check_sandbox("CREATE TABLE out AS SELECT 1", "sandbox_p1")
    check_sandbox("SELECT * FROM ground_truth.gt", "sandbox_p1")  # reads are fine


def test_check_sandbox_blocks_writes_outside():
    with pytest.raises(SandboxViolation):
        check_sandbox("DROP TABLE ground_truth.gt", "sandbox_p1")
    with pytest.raises(SandboxViolation):
        check_sandbox('df.write.saveAsTable("staging_inputs.x")', "sandbox_p1")


@pytest.fixture()
def executor(spark):
    ex = Executor(spark, MigrationConfig(), "p1")
    ex.reset()
    yield ex
    spark.sql("DROP SCHEMA IF EXISTS sandbox_p1 CASCADE")


def test_run_sql_step_creates_table(spark, executor):
    t = TranslatedStep(0, "sql",
                       "CREATE TABLE sandbox_p1.a AS SELECT 1 AS x; "
                       "CREATE TABLE sandbox_p1.b AS SELECT x + 1 AS y FROM sandbox_p1.a;",
                       [], ["sandbox_p1.a", "sandbox_p1.b"])
    r = executor.run_step(t)
    assert r.ok and r.duration_s >= 0
    assert spark.table("sandbox_p1.b").collect()[0]["y"] == 2


def test_run_pyspark_step(spark, executor):
    code = 'spark.sql("SELECT 42 AS v").write.saveAsTable("sandbox_p1.pys")'
    r = executor.run_step(TranslatedStep(1, "pyspark", code, [], ["sandbox_p1.pys"]))
    assert r.ok
    assert spark.table("sandbox_p1.pys").collect()[0]["v"] == 42


def test_run_step_captures_error_with_traceback(executor):
    r = executor.run_step(TranslatedStep(2, "sql", "SELECT * FROM nope.missing", [], []))
    assert not r.ok
    assert "nope" in r.error or "TABLE_OR_VIEW_NOT_FOUND" in r.error


def test_reset_clears_sandbox(spark, executor):
    executor.run_step(TranslatedStep(0, "sql",
                      "CREATE TABLE sandbox_p1.tmp AS SELECT 1 AS x", [], []))
    executor.reset()
    assert not spark.catalog.tableExists("sandbox_p1.tmp")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_execute.py -v
```
Expected: collection error — no module `sas_migrate.execute`.

- [ ] **Step 3: Implement `sas_migrate/execute.py`**

```python
"""Sandboxed execution of translated steps. Generated code may only write to
the program's sandbox schema — enforced by regex guard + current-schema
default. Timeout via Spark job-group cancel (watchdog thread); the cancel path
is verified in-tenant on the golden set."""
from __future__ import annotations

import re
import threading
import time
import traceback
from dataclasses import dataclass

from sas_migrate.config import MigrationConfig
from sas_migrate.translate import TranslatedStep


class SandboxViolation(Exception):
    pass


@dataclass
class StepResult:
    step_index: int
    ok: bool
    error: str | None
    duration_s: float


SQL_WRITE_RE = re.compile(
    r"\b(?:create\s+(?:or\s+replace\s+)?(?:table|view)|insert\s+(?:into|overwrite)"
    r"(?:\s+table)?|merge\s+into|drop\s+table(?:\s+if\s+exists)?|truncate\s+table)"
    r"\s+`?([\w.]+)`?", re.IGNORECASE)
PY_WRITE_RE = re.compile(r"\.saveAsTable\(\s*['\"]([\w.]+)['\"]")


def check_sandbox(code: str, sandbox_schema: str) -> None:
    targets = [m.group(1) for m in SQL_WRITE_RE.finditer(code)]
    targets += [m.group(1) for m in PY_WRITE_RE.finditer(code)]
    for t in targets:
        if "." in t and not t.lower().startswith(sandbox_schema.lower() + "."):
            raise SandboxViolation(
                f"write target {t!r} is outside sandbox schema {sandbox_schema!r}")


def split_sql(code: str) -> list[str]:
    stmts, buf, in_str = [], [], False
    for ch in code:
        if ch == "'":
            in_str = not in_str
        if ch == ";" and not in_str:
            s = "".join(buf).strip()
            if s:
                stmts.append(s)
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


class Executor:
    def __init__(self, spark, config: MigrationConfig, program_id: str):
        self.spark = spark
        self.config = config
        self.program_id = program_id
        self.schema = config.sandbox_schema(program_id)

    def reset(self) -> None:
        self.spark.sql(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")
        self.spark.sql(f"CREATE SCHEMA {self.schema}")
        self.spark.catalog.setCurrentDatabase(self.schema)

    def _with_timeout(self, fn) -> None:
        group = f"sas2dbx-{self.program_id}-{time.time()}"
        self.spark.sparkContext.setJobGroup(group, "sas2dbx step", True)
        done = threading.Event()

        def watchdog():
            if not done.wait(self.config.step_timeout_seconds):
                self.spark.sparkContext.cancelJobGroup(group)

        t = threading.Thread(target=watchdog, daemon=True)
        t.start()
        try:
            fn()
        finally:
            done.set()

    def run_step(self, tstep: TranslatedStep) -> StepResult:
        start = time.time()
        try:
            check_sandbox(tstep.code, self.schema)
            self.spark.catalog.setCurrentDatabase(self.schema)
            if tstep.language == "sql":
                def run():
                    for stmt in split_sql(tstep.code):
                        self.spark.sql(stmt)
            else:
                def run():
                    exec(compile(tstep.code, f"<step_{tstep.step_index}>", "exec"),
                         {"spark": self.spark})
            self._with_timeout(run)
            return StepResult(tstep.step_index, True, None, time.time() - start)
        except Exception:  # noqa: BLE001 - full traceback is the repair signal
            return StepResult(tstep.step_index, False, traceback.format_exc(),
                              time.time() - start)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_execute.py -v
```
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: sandboxed executor with SQL splitting, write guard, job-group timeout"
```

---

### Task 12: RepairLoop — nested make-it-run / make-it-match loops

**Files:**
- Create: `sas_migrate/repair.py`, `tests/test_repair.py`

**Interfaces:**
- Consumes: `Translator` (Task 8), `Executor`-shaped object (Task 11), `DiffReport` (Task 9), `TokenBudgetExceeded` (Task 2).
- Produces:
  - `ProgramOutcome(program_id: str, status: str, failure_mode: str | None, outer_attempts: int, total_run_repairs: int, last_error: str | None)` — `status` in `("parity_pass", "triage")`; `failure_mode` in `(None, "never_ran", "diverged", "budget")`
  - `RepairLoop(translator, config, on_attempt: Callable[[dict], None] | None = None)` with:
    - `run(program_id, steps: list[SasStep], full_program: str, table_schemas: dict, input_mappings: dict, executor, validate: Callable[[], DiffReport]) -> tuple[ProgramOutcome, dict[int, TranslatedStep], DiffReport | None]`
  - `executor` needs `.reset()`, `.run_step(tstep) -> StepResult`, `.schema`; `validate` is a zero-arg callable so the loop stays Spark-free and unit-testable
  - Only steps with `kind in ("data", "proc", "macro")` are translated; `"global"` steps skipped
  - Loop shape: translate all → for each outer attempt (max `config.max_match_repairs`): `executor.reset()`; run steps in order with inner repair (max `config.max_run_repairs` per step per outer attempt); if any step still fails → triage `"never_ran"`; else `validate()`; pass → done; fail → `repair_match` on implicated steps (those whose `outputs` intersect diverged tables; fallback = last translated step). `TokenBudgetExceeded` anywhere → triage `"budget"`.
  - Every attempt calls `on_attempt({"program_id", "loop": "run"|"match", "step_index", "outer_attempt", "ok"})`

- [ ] **Step 1: Write the failing tests**

`tests/test_repair.py`:
```python
import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import MockGateway, TokenBudget, TokenBudgetExceeded
from sas_migrate.preprocess import SasStep
from sas_migrate.repair import ProgramOutcome, RepairLoop
from sas_migrate.translate import TranslatedStep, Translator
from sas_migrate.validate import ColumnDiff, DiffReport, TableDiff


GOOD_SQL = """\
```json
{"language": "sql", "inputs": [], "outputs": ["sandbox_p1.out"]}
```
```sql
CREATE TABLE sandbox_p1.out AS SELECT 1 AS x;
```"""

STEPS = [SasStep(0, "global", "options nodate;"),
         SasStep(1, "data", "data out; set in; run;")]


class FakeExecutor:
    """Scripted executor: pop the next ok/fail from script per run_step call."""

    def __init__(self, script):
        self.script = list(script)
        self.schema = "sandbox_p1"
        self.resets = 0

    def reset(self):
        self.resets += 1

    def run_step(self, tstep):
        from sas_migrate.execute import StepResult
        ok = self.script.pop(0) if self.script else True
        return StepResult(tstep.step_index, ok,
                          None if ok else "Traceback: AnalysisException", 0.01)


def _diff(passed):
    if passed:
        return DiffReport("p1", [TableDiff("sandbox_p1.out", 1, 1, 0, 0)])
    return DiffReport("p1", [TableDiff("sandbox_p1.out", 1, 1, 0, 0,
                                       [ColumnDiff("x", 1, 1, ["x 1 != 2"])])])


def _loop(responses, attempts_log=None):
    translator = Translator(MockGateway(responses), MigrationConfig(),
                            TokenBudget(500_000))
    return RepairLoop(translator, MigrationConfig(),
                      on_attempt=(attempts_log.append if attempts_log is not None else None))


def test_happy_path_first_try(  ):
    loop = _loop([GOOD_SQL])
    outcome, translated, diff = loop.run(
        "p1", STEPS, "prog", {}, {}, FakeExecutor([True]), lambda: _diff(True))
    assert outcome.status == "parity_pass"
    assert outcome.outer_attempts == 1
    assert 1 in translated and 0 not in translated  # global step not translated
    assert diff.passed


def test_inner_loop_repairs_run_failure():
    log = []
    loop = _loop([GOOD_SQL, GOOD_SQL], attempts_log=log)  # translate + 1 run-repair
    outcome, _, _ = loop.run(
        "p1", STEPS, "prog", {}, {}, FakeExecutor([False, True]), lambda: _diff(True))
    assert outcome.status == "parity_pass"
    assert outcome.total_run_repairs == 1
    assert any(a["loop"] == "run" and not a["ok"] for a in log)


def test_inner_exhaustion_routes_to_never_ran():
    # translate + 3 run-repairs (max_run_repairs) all still failing
    loop = _loop([GOOD_SQL] * 4)
    outcome, _, _ = loop.run(
        "p1", STEPS, "prog", {}, {}, FakeExecutor([False] * 10), lambda: _diff(True))
    assert outcome.status == "triage"
    assert outcome.failure_mode == "never_ran"


def test_outer_loop_repairs_divergence():
    # translate + 1 match-repair
    loop = _loop([GOOD_SQL, GOOD_SQL])
    diffs = iter([_diff(False), _diff(True)])
    ex = FakeExecutor([True] * 10)
    outcome, _, _ = loop.run("p1", STEPS, "prog", {}, {}, ex, lambda: next(diffs))
    assert outcome.status == "parity_pass"
    assert outcome.outer_attempts == 2
    assert ex.resets == 2  # sandbox reset before every outer attempt


def test_outer_exhaustion_routes_to_diverged():
    loop = _loop([GOOD_SQL] * 10)
    outcome, _, diff = loop.run(
        "p1", STEPS, "prog", {}, {}, FakeExecutor([True] * 50), lambda: _diff(False))
    assert outcome.status == "triage"
    assert outcome.failure_mode == "diverged"
    assert outcome.outer_attempts == MigrationConfig().max_match_repairs
    assert diff is not None and not diff.passed


def test_budget_exhaustion_routes_to_budget():
    translator = Translator(MockGateway([GOOD_SQL] * 10), MigrationConfig(),
                            TokenBudget(1))  # trips on first charge
    loop = RepairLoop(translator, MigrationConfig())
    outcome, _, _ = loop.run(
        "p1", STEPS, "prog", {}, {}, FakeExecutor([True]), lambda: _diff(True))
    assert outcome.status == "triage"
    assert outcome.failure_mode == "budget"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_repair.py -v
```
Expected: collection error — no module `sas_migrate.repair`.

- [ ] **Step 3: Implement `sas_migrate/repair.py`**

```python
"""Nested repair loops (spec 4.2 #8). Inner loop: make it run (traceback as
signal, cheap). Outer loop: make it match (DiffReport as signal, expensive).
Separate budgets; the exhausted loop type is recorded for triage."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import TokenBudgetExceeded
from sas_migrate.preprocess import SasStep
from sas_migrate.translate import TranslatedStep, Translator
from sas_migrate.validate import DiffReport

TRANSLATABLE = ("data", "proc", "macro")


@dataclass
class ProgramOutcome:
    program_id: str
    status: str                 # parity_pass | triage
    failure_mode: str | None    # None | never_ran | diverged | budget
    outer_attempts: int
    total_run_repairs: int
    last_error: str | None


class RepairLoop:
    def __init__(self, translator: Translator, config: MigrationConfig,
                 on_attempt: Callable[[dict], None] | None = None):
        self.translator = translator
        self.config = config
        self._on_attempt = on_attempt or (lambda rec: None)

    def run(self, program_id: str, steps: list[SasStep], full_program: str,
            table_schemas: dict, input_mappings: dict, executor,
            validate: Callable[[], DiffReport]
            ) -> tuple[ProgramOutcome, dict[int, TranslatedStep], DiffReport | None]:
        translated: dict[int, TranslatedStep] = {}
        diff: DiffReport | None = None
        total_run_repairs = 0
        outer = 0
        try:
            for step in steps:
                if step.kind in TRANSLATABLE:
                    translated[step.index] = self.translator.translate(
                        step, full_program, table_schemas, input_mappings,
                        executor.schema, program_id)

            for outer in range(1, self.config.max_match_repairs + 1):
                executor.reset()
                run_error = None
                for idx in sorted(translated):
                    result = executor.run_step(translated[idx])
                    self._on_attempt({"program_id": program_id, "loop": "run",
                                      "step_index": idx, "outer_attempt": outer,
                                      "ok": result.ok})
                    inner = 0
                    while not result.ok and inner < self.config.max_run_repairs:
                        translated[idx] = self.translator.repair_run(
                            translated[idx], result.error, program_id)
                        result = executor.run_step(translated[idx])
                        inner += 1
                        total_run_repairs += 1
                        self._on_attempt({"program_id": program_id, "loop": "run",
                                          "step_index": idx, "outer_attempt": outer,
                                          "ok": result.ok})
                    if not result.ok:
                        run_error = result.error
                        break
                if run_error is not None:
                    return (ProgramOutcome(program_id, "triage", "never_ran",
                                           outer, total_run_repairs, run_error),
                            translated, diff)

                diff = validate()
                self._on_attempt({"program_id": program_id, "loop": "match",
                                  "step_index": -1, "outer_attempt": outer,
                                  "ok": diff.passed})
                if diff.passed:
                    return (ProgramOutcome(program_id, "parity_pass", None,
                                           outer, total_run_repairs, None),
                            translated, diff)
                if outer < self.config.max_match_repairs:
                    for idx in self._implicated(translated, diff):
                        translated[idx] = self.translator.repair_match(
                            translated[idx], diff.to_text(), program_id)

            return (ProgramOutcome(program_id, "triage", "diverged", outer,
                                   total_run_repairs,
                                   diff.to_text(max_chars=2000) if diff else None),
                    translated, diff)
        except TokenBudgetExceeded as e:
            return (ProgramOutcome(program_id, "triage", "budget", outer,
                                   total_run_repairs, str(e)),
                    translated, diff)

    @staticmethod
    def _implicated(translated: dict[int, TranslatedStep],
                    diff: DiffReport) -> list[int]:
        diverged = {t.table.lower() for t in diff.table_diffs if not t.passed}
        hits = [idx for idx, t in translated.items()
                if any(o.lower() in diverged for o in t.outputs)]
        return hits or [max(translated)]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_repair.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: nested repair loops with separate run/match budgets and triage routing"
```

---

### Task 13: Reporter — parity certificate + triage report

**Files:**
- Create: `sas_migrate/report.py`, `tests/test_report.py`

**Interfaces:**
- Consumes: `ProgramRecord` (Task 4), `ProgramOutcome` (Task 12), `DiffReport` (Task 9), `TranslatedStep` (Task 8), `StateStore` (Task 3), `MigrationConfig` (Task 1).
- Produces:
  - `parity_certificate(rec, outcome, diff, config, snapshot_hashes: dict[str, str]) -> str` — markdown containing: program id, owner, timestamp, outer attempts, per-table rows compared, tolerance actually used (`rec.float_rel_tol or config.float_rel_tol`), snapshot hashes, and the sentence "keyless comparisons normalize floats to 10 significant digits"
  - `triage_report(rec, outcome, translated: dict[int, TranslatedStep], diff) -> str` — markdown containing: failure mode, last error/diff text, and every translated step's closest-attempt code
  - `Reporter(store, config)`: `record(rec, outcome, diff, translated, snapshot_hashes) -> str` — writes a row to `parity_results` (append) with program_id/status/failure_mode/outer_attempts/report markdown, returns the markdown

- [ ] **Step 1: Write the failing tests**

`tests/test_report.py`:
```python
from sas_migrate.config import MigrationConfig
from sas_migrate.inventory import ProgramRecord
from sas_migrate.repair import ProgramOutcome
from sas_migrate.report import Reporter, parity_certificate, triage_report
from sas_migrate.statestore import LocalJsonStateStore
from sas_migrate.translate import TranslatedStep
from sas_migrate.validate import DiffReport, TableDiff


def _rec():
    return ProgramRecord("p1", "/sas/p1.sas", "phil",
                         ground_truth={"out": "ground_truth.p1_out"})


def _pass_outcome():
    return ProgramOutcome("p1", "parity_pass", None, 2, 1, None)


def _diff():
    return DiffReport("p1", [TableDiff("sandbox_p1.out", 1000, 1000, 0, 0)])


def test_certificate_records_what_was_compared():
    cert = parity_certificate(_rec(), _pass_outcome(), _diff(), MigrationConfig(),
                              {"staging_inputs.customers_ab12": "ab12ef34cd56"})
    assert "p1" in cert and "1000" in cert
    assert "1e-09" in cert                      # tolerance actually used
    assert "ab12ef34cd56" in cert               # snapshot hash
    assert "10 significant digits" in cert      # keyless proxy documented
    assert "attempts: 2" in cert.lower()


def test_certificate_uses_per_program_tolerance_override():
    rec = _rec()
    rec.float_rel_tol = 1e-6
    cert = parity_certificate(rec, _pass_outcome(), _diff(), MigrationConfig(), {})
    assert "1e-06" in cert


def test_triage_report_contains_mode_error_and_code():
    outcome = ProgramOutcome("p1", "triage", "never_ran", 1, 3,
                             "Traceback: AnalysisException")
    translated = {1: TranslatedStep(1, "sql", "SELECT broken", [], [])}
    rep = triage_report(_rec(), outcome, translated, None)
    assert "never_ran" in rep and "AnalysisException" in rep and "SELECT broken" in rep


def test_reporter_persists_result_row(tmp_path):
    store = LocalJsonStateStore(str(tmp_path))
    md = Reporter(store, MigrationConfig()).record(
        _rec(), _pass_outcome(), _diff(), {}, {})
    rows = store.scan("parity_results")
    assert rows[0]["program_id"] == "p1" and rows[0]["status"] == "parity_pass"
    assert "Parity Certificate" in md
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_report.py -v
```
Expected: collection error — no module `sas_migrate.report`.

- [ ] **Step 3: Implement `sas_migrate/report.py`**

```python
"""Audit artifacts. The parity certificate records exactly what was compared
and at what tolerance — no silent leniency (spec section 3)."""
from __future__ import annotations

from datetime import datetime, timezone

from sas_migrate.config import MigrationConfig
from sas_migrate.inventory import ProgramRecord
from sas_migrate.repair import ProgramOutcome
from sas_migrate.statestore import StateStore
from sas_migrate.translate import TranslatedStep
from sas_migrate.validate import DiffReport


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parity_certificate(rec: ProgramRecord, outcome: ProgramOutcome,
                       diff: DiffReport, config: MigrationConfig,
                       snapshot_hashes: dict[str, str]) -> str:
    tol = rec.float_rel_tol if rec.float_rel_tol is not None else config.float_rel_tol
    tables = "\n".join(
        f"| {t.table} | {t.gt_rows} | {t.out_rows} | PASS |"
        for t in diff.table_diffs)
    snaps = "\n".join(f"- `{t}`: `{h}`" for t, h in sorted(snapshot_hashes.items())) \
            or "- (none recorded)"
    return f"""# Parity Certificate — {rec.program_id}

- **Program:** `{rec.sas_path}` (owner: {rec.owner})
- **Certified at:** {_now()}
- **Outer attempts: {outcome.outer_attempts}** (run repairs: {outcome.total_run_repairs})
- **Float relative tolerance used:** {tol}
  (keyless comparisons normalize floats to 10 significant digits before hashing)

## Tables compared (order-insensitive)

| output table | ground-truth rows | output rows | result |
|---|---|---|---|
{tables}

## Input snapshots (content hashes)
{snaps}
"""


def triage_report(rec: ProgramRecord, outcome: ProgramOutcome,
                  translated: dict[int, TranslatedStep],
                  diff: DiffReport | None) -> str:
    evidence = outcome.last_error or "(no error text)"
    if diff is not None and outcome.failure_mode == "diverged":
        evidence = diff.to_text()
    code_sections = "\n".join(
        f"### Step {idx} ({t.language})\n```{t.language}\n{t.code}\n```"
        for idx, t in sorted(translated.items()))
    return f"""# Triage Report — {rec.program_id}

- **Program:** `{rec.sas_path}` (owner: {rec.owner})
- **Failure mode:** {outcome.failure_mode}
- **Outer attempts:** {outcome.outer_attempts} (run repairs: {outcome.total_run_repairs})
- **Filed at:** {_now()}

## Evidence
```
{evidence}
```

## Closest attempt (generated code)
{code_sections or '(no steps were translated)'}
"""


class Reporter:
    def __init__(self, store: StateStore, config: MigrationConfig):
        self.store = store
        self.config = config

    def record(self, rec: ProgramRecord, outcome: ProgramOutcome,
               diff: DiffReport | None, translated: dict[int, TranslatedStep],
               snapshot_hashes: dict[str, str]) -> str:
        if outcome.status == "parity_pass":
            md = parity_certificate(rec, outcome, diff, self.config, snapshot_hashes)
        else:
            md = triage_report(rec, outcome, translated, diff)
        self.store.append("parity_results", {
            "program_id": rec.program_id, "status": outcome.status,
            "failure_mode": outcome.failure_mode,
            "outer_attempts": outcome.outer_attempts, "ts": _now(), "report": md})
        return md
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_report.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: parity certificate and triage report with persisted results"
```

---

### Task 14: Pipeline + notebooks + README

**Files:**
- Create: `sas_migrate/pipeline.py`, `tests/test_pipeline.py`, `notebooks/Migrate_One.py`, `notebooks/Migrate_Batch.py`, `README.md`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `build_table_schemas(spark, tables: list[str]) -> dict[str, str]` — `"name TYPE, ..."` strings for prompts; silently skips tables that don't exist yet
  - `migrate_program(spark, rec: ProgramRecord, deps: PipelineDeps) -> ProgramOutcome` — full orchestration for one program, updating inventory status at each stage transition
  - `PipelineDeps(config, inventory, translator, reporter, store)` dataclass — wiring bundle so notebooks build it once
  - `migrate_batch(spark, deps, program_ids: list[str] | None = None) -> dict` — iterates `inventory.pending()` (or the given ids), enforces the per-batch token cap by constructing one shared `TokenBudget` checked between programs, returns `{"parity_pass": [...], "triage": [...]}`

- [ ] **Step 1: Write the failing test** (integration: fake gateway, real local Spark end-to-end)

`tests/test_pipeline.py`:
```python
import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import MockGateway, TokenBudget
from sas_migrate.inventory import Inventory, ProgramRecord
from sas_migrate.pipeline import PipelineDeps, build_table_schemas, migrate_program
from sas_migrate.report import Reporter
from sas_migrate.statestore import LocalJsonStateStore
from sas_migrate.translate import Translator


SAS_PROGRAM = """\
data work.filtered;
  set staging.customers;
  where region = 'east';
run;
"""

GOOD_RESPONSE = """\
```json
{"language": "sql", "inputs": ["staging_pl.customers"], "outputs": ["sandbox_pl1.filtered"]}
```
```sql
CREATE TABLE sandbox_pl1.filtered AS
SELECT * FROM staging_pl.customers WHERE region = 'east';
```"""

BAD_THEN_FIXED = """\
```json
{"language": "sql", "inputs": ["staging_pl.customers"], "outputs": ["sandbox_pl1.filtered"]}
```
```sql
CREATE TABLE sandbox_pl1.filtered AS
SELECT * FROM staging_pl.customers WHERE region = 'west';
```"""


@pytest.fixture()
def env(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS staging_pl")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gt_pl")
    spark.sql("DROP TABLE IF EXISTS staging_pl.customers")
    spark.sql("DROP TABLE IF EXISTS gt_pl.filtered")
    spark.createDataFrame(
        [(1, "east", 10.0), (2, "west", 20.0), (3, "east", 30.0)],
        "id INT, region STRING, balance DOUBLE").write.saveAsTable("staging_pl.customers")
    spark.createDataFrame(
        [(1, "east", 10.0), (3, "east", 30.0)],
        "id INT, region STRING, balance DOUBLE").write.saveAsTable("gt_pl.filtered")
    sas = tmp_path / "pl1.sas"
    sas.write_text(SAS_PROGRAM)
    store = LocalJsonStateStore(str(tmp_path / "state"))
    rec = ProgramRecord("pl1", str(sas), "phil",
                        inputs={"staging.customers": "staging_pl.customers"},
                        ground_truth={"filtered": "gt_pl.filtered"})
    yield spark, store, rec
    spark.sql("DROP SCHEMA IF EXISTS sandbox_pl1 CASCADE")


def _deps(store, responses):
    cfg = MigrationConfig()
    translator = Translator(MockGateway(responses), cfg,
                            TokenBudget(cfg.per_program_token_cap))
    inv = Inventory(store)
    return PipelineDeps(config=cfg, inventory=inv, translator=translator,
                        reporter=Reporter(store, cfg), store=store)


def test_build_table_schemas(spark, env):
    schemas = build_table_schemas(spark, ["staging_pl.customers", "nope.missing"])
    assert "region" in schemas["staging_pl.customers"].lower()
    assert "nope.missing" not in schemas


def test_migrate_program_reaches_parity_first_try(env):
    spark, store, rec = env
    deps = _deps(store, [GOOD_RESPONSE])
    deps.inventory.register(rec)
    outcome = migrate_program(spark, rec, deps)
    assert outcome.status == "parity_pass"
    assert deps.inventory.get("pl1").status == "parity_pass"
    assert store.scan("parity_results")[0]["status"] == "parity_pass"


def test_migrate_program_repairs_divergence_then_passes(env):
    spark, store, rec = env
    deps = _deps(store, [BAD_THEN_FIXED, GOOD_RESPONSE])  # wrong filter, then fixed
    deps.inventory.register(rec)
    outcome = migrate_program(spark, rec, deps)
    assert outcome.status == "parity_pass"
    assert outcome.outer_attempts == 2
```

- [ ] **Step 2: Run test to verify it fails**

```bash
.venv/bin/pytest tests/test_pipeline.py -v
```
Expected: collection error — no module `sas_migrate.pipeline`.

- [ ] **Step 3: Implement `sas_migrate/pipeline.py`**

```python
"""End-to-end orchestration: the only module that wires everything together.
Notebooks call migrate_program / migrate_batch and nothing else."""
from __future__ import annotations

from dataclasses import dataclass

from sas_migrate.config import MigrationConfig
from sas_migrate.execute import Executor
from sas_migrate.inventory import Inventory, ProgramRecord
from sas_migrate.preprocess import preprocess
from sas_migrate.repair import ProgramOutcome, RepairLoop
from sas_migrate.report import Reporter
from sas_migrate.statestore import StateStore
from sas_migrate.translate import Translator
from sas_migrate.validate import validate_program


@dataclass
class PipelineDeps:
    config: MigrationConfig
    inventory: Inventory
    translator: Translator
    reporter: Reporter
    store: StateStore


def build_table_schemas(spark, tables: list[str]) -> dict[str, str]:
    out = {}
    for t in tables:
        try:
            fields = spark.table(t).schema.fields
        except Exception:  # noqa: BLE001 - table may not exist yet
            continue
        out[t] = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in fields)
    return out


def _table_pairs(rec: ProgramRecord, sandbox: str) -> list[tuple[str, str, None]]:
    """Ground-truth table -> expected sandbox output (same base name).
    Keyless by default; keyed comparison via per-program config is a
    notebook-level override passed straight to validate_program."""
    return [(gt, f"{sandbox}.{name.split('.')[-1]}", None)
            for name, gt in sorted(rec.ground_truth.items())]


def migrate_program(spark, rec: ProgramRecord, deps: PipelineDeps) -> ProgramOutcome:
    cfg = deps.config
    steps, full_program = preprocess(rec.sas_path)
    deps.inventory.set_status(rec.program_id, "landed")

    executor = Executor(spark, cfg, rec.program_id)
    sandbox = cfg.sandbox_schema(rec.program_id)
    tol = rec.float_rel_tol if rec.float_rel_tol is not None else cfg.float_rel_tol
    schemas = build_table_schemas(spark, list(rec.inputs.values()))

    def validate():
        deps.inventory.set_status(rec.program_id, "validating")
        return validate_program(spark, rec.program_id,
                                _table_pairs(rec, sandbox), rel_tol=tol)

    loop = RepairLoop(deps.translator, cfg,
                      on_attempt=lambda a: deps.store.append("attempts", a))
    outcome, translated, diff = loop.run(
        rec.program_id, steps, full_program, schemas, rec.inputs,
        executor, validate)

    deps.inventory.set_status(
        rec.program_id, outcome.status,
        error=outcome.last_error, failure_mode=outcome.failure_mode)
    deps.reporter.record(rec, outcome, diff, translated, snapshot_hashes={})
    return outcome


def migrate_batch(spark, deps: PipelineDeps,
                  program_ids: list[str] | None = None) -> dict:
    from sas_migrate.gateway import TokenBudget, TokenBudgetExceeded

    batch_budget = TokenBudget(deps.config.per_batch_token_cap)
    results: dict = {"parity_pass": [], "triage": []}
    records = ([deps.inventory.get(pid) for pid in program_ids] if program_ids
               else deps.inventory.pending())
    for rec in records:
        if rec is None:
            continue
        # Fresh per-program budget each iteration; charge its actual usage
        # against the batch cap afterwards.
        deps.translator.budget = TokenBudget(deps.config.per_program_token_cap)
        outcome = migrate_program(spark, rec, deps)
        results[outcome.status].append(rec.program_id)
        try:
            batch_budget.charge(deps.translator.budget.used)
        except TokenBudgetExceeded:
            break  # remaining programs stay pending; batch resumes next run
    return results
```

- [ ] **Step 4: Run tests to verify they pass, then run the whole suite**

```bash
.venv/bin/pytest tests/test_pipeline.py -v && .venv/bin/pytest -q
```
Expected: 3 passed, then full suite green (≈53 tests).

- [ ] **Step 5: Write the notebooks**

`notebooks/Migrate_One.py`:
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Migrate One SAS Program
# MAGIC Power-user notebook: converts one SAS program, validates parity against
# MAGIC ground truth, and prints a parity certificate or triage report.

# COMMAND ----------

dbutils.widgets.text("program_id", "", "Program ID")
dbutils.widgets.text("sas_path", "", "Path to .sas file (Workspace/Volumes)")
dbutils.widgets.text("owner", "", "Owner (your name)")
dbutils.widgets.text("inputs_json", "{}", 'Input map {"libref.table": "catalog.schema.table"}')
dbutils.widgets.text("ground_truth_json", "{}", 'GT map {"sas_out": "catalog.schema.table"}')
dbutils.widgets.text("float_rel_tol", "", "Tolerance override (blank = 1e-9)")

# COMMAND ----------

import json

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import RestGatewayClient, TokenBudget
from sas_migrate.inventory import Inventory, ProgramRecord
from sas_migrate.pipeline import PipelineDeps, migrate_program
from sas_migrate.report import Reporter
from sas_migrate.statestore import DeltaStateStore
from sas_migrate.translate import Translator

config = MigrationConfig(
    gateway_base_url=dbutils.secrets.get("sas2dbx", "gateway_url"))
store = DeltaStateStore(spark, config)
gateway = RestGatewayClient(
    config, auth_token=dbutils.secrets.get("sas2dbx", "gateway_token"),
    on_call=lambda rec: store.append("llm_calls", rec))
budget = TokenBudget(config.per_program_token_cap)
deps = PipelineDeps(config=config, inventory=Inventory(store),
                    translator=Translator(gateway, config, budget),
                    reporter=Reporter(store, config), store=store)

# COMMAND ----------

tol = dbutils.widgets.get("float_rel_tol")
rec = ProgramRecord(
    program_id=dbutils.widgets.get("program_id"),
    sas_path=dbutils.widgets.get("sas_path"),
    owner=dbutils.widgets.get("owner"),
    inputs=json.loads(dbutils.widgets.get("inputs_json")),
    ground_truth=json.loads(dbutils.widgets.get("ground_truth_json")),
    float_rel_tol=float(tol) if tol else None)
deps.inventory.register(rec)
outcome = migrate_program(spark, rec, deps)
print(f"RESULT: {outcome.status}  (mode={outcome.failure_mode}, "
      f"outer={outcome.outer_attempts}, run_repairs={outcome.total_run_repairs})")

# COMMAND ----------

# Show the certificate / triage report
latest = [r for r in store.scan("parity_results")
          if r["program_id"] == rec.program_id][-1]
displayHTML(f"<pre>{latest['report']}</pre>")
```

`notebooks/Migrate_Batch.py`:
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Migrate Batch
# MAGIC Central-team notebook: processes every pending program in the inventory
# MAGIC (register programs via `Inventory.register` first), resumable — programs
# MAGIC already at parity_pass are skipped automatically.

# COMMAND ----------

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import RestGatewayClient, TokenBudget
from sas_migrate.inventory import Inventory
from sas_migrate.pipeline import PipelineDeps, migrate_batch
from sas_migrate.report import Reporter
from sas_migrate.statestore import DeltaStateStore
from sas_migrate.translate import Translator

config = MigrationConfig(
    gateway_base_url=dbutils.secrets.get("sas2dbx", "gateway_url"))
store = DeltaStateStore(spark, config)
gateway = RestGatewayClient(
    config, auth_token=dbutils.secrets.get("sas2dbx", "gateway_token"),
    on_call=lambda rec: store.append("llm_calls", rec))
deps = PipelineDeps(config=config, inventory=Inventory(store),
                    translator=Translator(gateway, config,
                                          TokenBudget(config.per_program_token_cap)),
                    reporter=Reporter(store, config), store=store)

# COMMAND ----------

results = migrate_batch(spark, deps)
print(f"parity_pass: {len(results['parity_pass'])}  triage: {len(results['triage'])}")

# COMMAND ----------

# Status funnel
import pandas as pd
rows = store.scan("inventory")
display(spark.createDataFrame(pd.DataFrame(rows))
        .groupBy("status").count().orderBy("status"))
```

- [ ] **Step 6: Write `README.md`**

```markdown
# sas2dbx — SAS → Databricks Migration Agent

Converts SAS programs to Spark SQL/PySpark via the company AI gateway,
executes them in a sandbox, diffs outputs cell-by-cell against SAS ground
truth, self-repairs, and emits parity certificates (or triage reports).

Spec: `docs/superpowers/specs/2026-07-16-sas2dbx-migration-agent-design.md`

## Local development

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest
```

Runtime code uses ONLY Databricks Runtime built-ins (JFrog constraint).
Dev-only deps (pytest, local pyspark) never ship to the cluster.

## In-tenant setup (the only work done at the company)

1. Fill in `RestGatewayClient._build_request` / `._parse_response` in
   `sas_migrate/gateway.py` with the gateway's REST contract.
2. Create secret scope `sas2dbx` with `gateway_url` and `gateway_token`.
3. Sync this repo into Databricks Repos; create catalog `sas_migration`.
4. Land ground-truth outputs and inputs (CSV/Parquet exported from SAS —
   `sas_migrate/landing.py` normalizes SAS quirks on the way in).
5. Register the golden-set programs and run `notebooks/Migrate_Batch.py`.

## Notebooks

- `notebooks/Migrate_One.py` — single program (power users, widget-driven)
- `notebooks/Migrate_Batch.py` — walk the inventory (central team, resumable)
```

- [ ] **Step 7: Run the full suite one final time**

```bash
.venv/bin/pytest -q
```
Expected: all tests pass, 0 failures.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat: pipeline orchestration, Databricks notebooks, README"
```

---

## Self-Review Notes

- **Spec coverage:** gateway+budgets (T2), state/inventory/resumability (T3–4), landing+normalization+snapshots (T5), preprocess (T6), translate+cribsheet (T7–8), validator+self-test corruptions (T9–10), sandboxed executor+timeout (T11), nested repair loops+triage modes (T12), certificate/triage artifacts (T13), notebooks+batch funnel (T14). Landing's Spark write side is intentionally thin (pandas → `spark.createDataFrame(...).write.saveAsTable` happens in-tenant during golden-set landing); snapshot hashes recorded via `content_hash` at landing time and passed to `Reporter.record` (pipeline passes `{}` until landing runs in-tenant — the certificate prints "(none recorded)").
- **Known deliberate gaps (v1, in-tenant):** step timeout cancel path verified on golden set; `DeltaStateStore` smoke-tested in-tenant; keyed comparison per program is a notebook-level override.
- **Type consistency check:** `ProgramRecord.failure_mode` values match `ProgramOutcome.failure_mode`; `TranslatedStep` fields consistent across T8/T11/T12/T13; `StateStore` method names consistent across T3/T4/T13/T14.
