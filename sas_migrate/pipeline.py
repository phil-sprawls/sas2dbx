"""End-to-end orchestration: the only module that wires everything together.
Notebooks call migrate_program / migrate_batch and nothing else."""
from __future__ import annotations

import traceback
from dataclasses import dataclass

from sas_migrate.config import MigrationConfig
from sas_migrate.execute import Executor
from sas_migrate.gateway import CircuitOpenError
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


def _table_pairs(rec: ProgramRecord, sandbox: str) -> list[tuple[str, str, list[str] | None]]:
    """Ground-truth table -> expected sandbox output (same base name), paired
    with the per-output key columns from rec.keys (if any). Keyless when a
    given output name has no entry in rec.keys."""
    return [(gt, f"{sandbox}.{name.split('.')[-1]}", rec.keys.get(name))
            for name, gt in sorted(rec.ground_truth.items())]


def migrate_program(spark, rec: ProgramRecord, deps: PipelineDeps) -> ProgramOutcome:
    try:
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

        expected = [f"{sandbox}.{name.split('.')[-1]}" for name in sorted(rec.ground_truth)]

        loop = RepairLoop(deps.translator, cfg,
                          on_attempt=lambda a: deps.store.append("attempts", a))
        deps.inventory.set_status(rec.program_id, "translated")
        outcome, translated, diff = loop.run(
            rec.program_id, steps, full_program, schemas, rec.inputs,
            executor, validate, expected_outputs=expected)

        deps.inventory.set_status(
            rec.program_id, outcome.status,
            error=outcome.last_error, failure_mode=outcome.failure_mode)
        deps.reporter.record(rec, outcome, diff, translated, snapshot_hashes={})
        return outcome
    except CircuitOpenError:
        deps.inventory.set_status(rec.program_id, "triage",
                                  error="gateway circuit open", failure_mode="never_ran")
        raise  # halting the batch is the circuit breaker's job
    except Exception:
        err = traceback.format_exc()
        outcome = ProgramOutcome(rec.program_id, "triage", "never_ran", 0, 0, err)
        deps.inventory.set_status(rec.program_id, "triage", error=err,
                                  failure_mode="never_ran")
        deps.reporter.record(rec, outcome, None, {}, snapshot_hashes={})
        return outcome


def migrate_batch(spark, deps: PipelineDeps,
                  program_ids: list[str] | None = None) -> dict:
    from sas_migrate.gateway import TokenBudget, TokenBudgetExceeded

    batch_budget = TokenBudget(deps.config.per_batch_token_cap)
    results: dict = {"parity_pass": [], "triage": []}
    if program_ids:
        records = []
        for pid in program_ids:
            rec = deps.inventory.get(pid)
            if rec is None:
                print(f"skipping unknown program_id {pid!r}")
                continue
            records.append(rec)
    else:
        records = deps.inventory.pending()
    for rec in records:
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
