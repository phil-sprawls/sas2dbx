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


def test_happy_path_first_try():
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


def test_no_translatable_steps_routes_to_triage():
    loop = _loop([])
    steps = [SasStep(0, "global", "options nodate;")]
    outcome, translated, diff = loop.run(
        "p1", steps, "prog", {}, {}, FakeExecutor([]), lambda: _diff(False))
    assert outcome.status == "triage"
    assert outcome.failure_mode == "never_ran"
    assert translated == {} and diff is None
