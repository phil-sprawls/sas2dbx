# sas2dbx — SAS → Databricks Migration Agent (Design)

**Date:** 2026-07-16
**Status:** Approved pending user review
**Owner:** Phil Sprawls

## 1. Problem & Goal

Migrate the SAS workloads of ~1000 enterprise users to Databricks with **provable
output data parity**. The system converts SAS programs to Spark SQL (PySpark where
necessary), executes the converted code against the same inputs SAS used, diffs the
results against SAS-produced ground-truth outputs, self-repairs on failure, and emits
either a parity certificate or a triage report for human review.

**The parity harness is the product.** The LLM translation is replaceable; the
empirical validation loop is what makes sign-off trustworthy.

## 2. Constraints

- **Runs in Databricks notebooks.** Core pipeline is a Python package synced via
  Databricks Repos; two thin driver notebooks on top.
- **LLM access only via the company AI gateway** — a custom REST API. Available
  models: Anthropic Claude Opus 4.6 and OpenAI GPT Sol 5.6. Model is configuration,
  not code; default Opus 4.6 for translation and repair.
- **Python dependencies are governed by JFrog.** The core pipeline uses only what
  Databricks Runtime already ships: `pyspark`, `pandas`, `requests`, stdlib.
  `pyreadstat` (direct `.sas7bdat` reads) is optional behind an interface; the
  zero-dependency fallback is SAS-side export to CSV/Parquet. `pytest` is dev-only,
  never required on the cluster.
- **Workload mix:** vast majority SQL-shaped (DATA steps, PROC SQL), plus macros,
  reporting PROCs, and a tail of statistical PROCs.
- **Ground truth:** SAS-produced output datasets are available and can be landed in
  Databricks.
- **Input data:** mixed — some sources already in Unity Catalog, some still in SAS
  libraries; landing/snapshotting is in scope.
- **Operators:** hybrid — central migration team runs batches; power users
  self-serve on single programs.

## 3. Parity Definition (sign-off standard)

A converted program is **at parity** when every output table matches its SAS
ground-truth counterpart:

- **Order-insensitive** comparison (join on business keys when known; sorted
  row-hash otherwise).
- **Exact match** on non-float columns.
- **Relative tolerance** on float columns, default `1e-9`, because SAS and Spark
  legitimately differ in floating-point summation order. Statistical PROCs may use a
  looser, documented per-program tolerance.
- **Normalization applied to both sides before diffing** so we compare meaning, not
  encoding: SAS `.` missings → NULL, SAS date epoch (1960-01-01) → ISO dates,
  trailing-blank char padding trimmed.
- Every parity certificate records exactly what was compared and at what tolerance —
  no silent leniency.

## 4. Architecture

Deterministic Python control flow; the LLM is invoked at exactly two points —
translate and repair.

```
inventory (Delta state table)
  → land inputs + ground truth (snapshot, normalize SAS quirks)
  → preprocess (resolve %include, expand %let, split at DATA/PROC boundaries)
  → translate (gateway LLM: step → Spark SQL, PySpark fallback)
  → execute in per-program sandbox schema
  → validate (order-insensitive diff vs ground truth)
      → pass: parity certificate
      → fail: DiffReport/traceback fed back to LLM, repair, retry ≤ 5
      → still failing: triage report → human queue
```

### 4.1 Unity Catalog layout

Dedicated catalog (e.g., `sas_migration`):

| Schema | Contents |
|---|---|
| `control` | `inventory`, `attempts`, `llm_calls`, `parity_results` Delta tables |
| `ground_truth` | Landed SAS output datasets |
| `staging_inputs` | Snapshotted inputs, content-hashed, point-in-time frozen |
| `sandbox_<program>` | Execution outputs — generated code may never write elsewhere |

### 4.2 Components

One file, one job, unit-testable off-cluster where feasible.

1. **GatewayClient** (`gateway.py`) — the only module that knows the REST contract.
   `complete(system, messages, *, model, max_tokens) -> str` with retry/backoff and
   a circuit breaker that halts the batch after 5 consecutive failures. Every call logged (model, tokens,
   latency, purpose, program id) to `control.llm_calls`. Exact HTTP contract filled
   in from a sample request/response Phil provides.
2. **Inventory** (`inventory.py`) — registers programs (SAS source, owner, input
   mappings, ground-truth mappings) and tracks status:
   `registered → landed → translated → validating → parity_pass | triage`.
   Idempotent and resumable; a killed batch run picks up where it left off.
3. **DataLander** (`landing.py`) — lands inputs and SAS ground-truth outputs to
   Delta with SAS-quirk normalization. Reader interface with two implementations:
   CSV/Parquet exports (zero deps, preferred) and `pyreadstat` (optional, if JFrog
   approves). Snapshots named with content hashes for point-in-time consistency.
4. **Preprocessor** (`preprocess.py`) — deterministic, no full SAS parser: resolves
   `%include`, expands simple `%let` macro variables, splits the program at
   DATA-step/PROC boundaries into an ordered step list. Complex `%macro` bodies pass
   through whole, flagged for the LLM.
5. **Translator** (`translate.py`) — per step: SAS code + live schemas of referenced
   tables + a SAS↔Spark gotcha cribsheet → Spark SQL preferred, PySpark when
   relational SQL can't express it (flagged). Structured output: JSON header
   (language, inputs, outputs) + code block. Cribsheet covers: date epochs,
   missing-value comparison semantics, implicit RETAIN, PROC MEANS default
   statistics, FIRST./LAST. processing, formats/informats.
6. **Executor** (`execute.py`) — runs generated steps in order against frozen input
   snapshots, writing only to the program's sandbox schema. Per-step timeout;
   exceptions captured with full traceback for the repair loop.
7. **Validator** (`validate.py`) — the trust anchor. Per output table: schema
   alignment (name-normalized) → row counts → order-insensitive full comparison →
   per-column diff. Produces a structured **DiffReport**: sample missing/extra rows,
   sample mismatched cells, per-column mismatch rates.
8. **RepairLoop** (`repair.py`) — orchestrates translate → execute → validate, max
   5 attempts per program. On failure the LLM receives the current code plus the
   traceback or DiffReport — it fixes against evidence. Every attempt logged to
   `control.attempts`.
9. **Reporter** (`report.py`) — on pass: **parity certificate** (program, attempts,
   tables/rows/cells compared, tolerances used, snapshot hashes) — the audit
   artifact for sign-off. On failure: **triage report** (closest attempt's code,
   diff summary, suspected cause) queued for the migration team.

### 4.3 Driver notebooks

- **`Migrate_Batch`** — central team: walks the inventory with checkpointing,
  parallelizable, renders a status funnel (registered/landed/…/parity_pass/triage).
- **`Migrate_One`** — power users: Databricks widgets (program path, target schema,
  tolerance override), one program end-to-end with readable progress output and
  guardrails (sandbox-only writes, token caps).

## 5. Error Handling & Cost Control

- Stage-level status and error stored per program in `control.inventory`; batch
  reruns skip `parity_pass`.
- Gateway: exponential backoff; circuit breaker protects the shared enterprise
  gateway.
- Runaway execution: per-step timeout, kill and record.
- Budget guards: per-program token cap and per-batch-run global cap; hitting a cap
  routes the program to triage rather than silently continuing.

## 6. Testing

- **Unit tests off-cluster** (pytest, pandas-only paths) for Preprocessor,
  Validator, and prompt assembly.
- **Golden set:** 5–10 representative SAS programs with known outputs as the
  regression suite; the pipeline must pass all before pointing at the full
  inventory.
- **Validator self-test:** inject known corruptions (dropped rows, perturbed
  floats beyond tolerance, swapped values) into a copy of ground truth and confirm
  the DiffReport catches every one. We test the tester because sign-off rests on it.

## 7. Out of Scope (v1)

- Full SAS grammar parser / deterministic transpiler.
- Scheduling & orchestration of converted code post-migration (Jobs/Workflows setup).
- Automatic BI/report layer migration (ODS output fidelity, styling).
- Multi-agent tool-calling architectures.

## 8. Delivery

Built and unit-tested in `~/dev/sas2dbx` (this repo), then pushed to a git remote
Phil syncs into Databricks Repos at work. Nothing here depends on the public
internet at runtime except the company gateway.
