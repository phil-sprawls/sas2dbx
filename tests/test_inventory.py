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
