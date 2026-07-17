from dataclasses import dataclass


@dataclass
class MigrationConfig:
    # Unity Catalog layout (catalog prepended in-tenant; local tests use 2-part names)
    catalog: str = "sas_migration"
    control_schema: str = "control"
    ground_truth_schema: str = "ground_truth"
    staging_schema: str = "staging_inputs"

    # Gateway / models — names are config, never hardcoded in logic
    gateway_base_url: str = ""
    default_model: str = "claude-opus-4-6"
    alt_model: str = "gpt-sol-5-6"
    gateway_max_retries: int = 4
    gateway_circuit_breaker_threshold: int = 5
    max_tokens_per_call: int = 8192

    # Parity
    float_rel_tol: float = 1e-9

    # Repair budgets
    max_run_repairs: int = 3      # inner loop: make it run (per step)
    max_match_repairs: int = 5    # outer loop: make it match (per program)

    # Token budgets
    per_program_token_cap: int = 500_000
    per_batch_token_cap: int = 20_000_000

    # Execution
    step_timeout_seconds: int = 1800

    def sandbox_schema(self, program_id: str) -> str:
        return f"sandbox_{program_id}"
