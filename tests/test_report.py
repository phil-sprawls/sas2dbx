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
