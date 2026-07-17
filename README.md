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
6. Notebooks issue `USE CATALOG <catalog>`; ensure it exists and you have
   USE/CREATE on it.
7. Verify `DeltaStateStore.upsert` explicitly (MERGE with parameter markers,
   never spliced SQL text) on the golden set.
8. Confirm the job-group timeout cancel path (`Executor._with_timeout`) fires
   correctly on the golden set.

## Notebooks

- `notebooks/Migrate_One.py` — single program (power users, widget-driven)
- `notebooks/Migrate_Batch.py` — walk the inventory (central team, resumable)
