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
        f"| {t.table} | {t.gt_rows} | {t.out_rows} | "
        f"{'PASS' if t.passed else 'FAIL'} | {t.method} |"
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

| output table | ground-truth rows | output rows | result | method |
|---|---|---|---|---|
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
        f"### Step {idx} ({t.language})\n"
        f"```{'python' if t.language == 'pyspark' else t.language}\n{t.code}\n```"
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
