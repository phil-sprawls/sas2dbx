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
