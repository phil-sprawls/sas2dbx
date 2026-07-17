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
