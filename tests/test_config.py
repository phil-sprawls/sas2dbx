from sas_migrate.config import MigrationConfig


def test_defaults_match_spec():
    c = MigrationConfig()
    assert c.float_rel_tol == 1e-9
    assert c.max_run_repairs == 3
    assert c.max_match_repairs == 5
    assert c.per_program_token_cap == 500_000
    assert c.per_batch_token_cap == 20_000_000
    assert c.default_model == "claude-opus-4-6"
    assert c.gateway_circuit_breaker_threshold == 5


def test_sandbox_schema_is_per_program():
    c = MigrationConfig()
    assert c.sandbox_schema("prog_001") == "sandbox_prog_001"
