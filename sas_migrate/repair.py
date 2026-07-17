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
