# Databricks notebook source
# MAGIC %md
# MAGIC # Migrate One SAS Program
# MAGIC Power-user notebook: converts one SAS program, validates parity against
# MAGIC ground truth, and prints a parity certificate or triage report.

# COMMAND ----------

dbutils.widgets.text("program_id", "", "Program ID")
dbutils.widgets.text("sas_path", "", "Path to .sas file (Workspace/Volumes)")
dbutils.widgets.text("owner", "", "Owner (your name)")
dbutils.widgets.text("inputs_json", "{}", 'Input map {"libref.table": "catalog.schema.table"}')
dbutils.widgets.text("ground_truth_json", "{}", 'GT map {"sas_out": "catalog.schema.table"}')
dbutils.widgets.text("float_rel_tol", "", "Tolerance override (blank = 1e-9)")

# COMMAND ----------

import json

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import RestGatewayClient, TokenBudget
from sas_migrate.inventory import Inventory, ProgramRecord
from sas_migrate.pipeline import PipelineDeps, migrate_program
from sas_migrate.report import Reporter
from sas_migrate.statestore import DeltaStateStore
from sas_migrate.translate import Translator

config = MigrationConfig(
    gateway_base_url=dbutils.secrets.get("sas2dbx", "gateway_url"))
store = DeltaStateStore(spark, config)
gateway = RestGatewayClient(
    config, auth_token=dbutils.secrets.get("sas2dbx", "gateway_token"),
    on_call=lambda rec: store.append("llm_calls", rec))
budget = TokenBudget(config.per_program_token_cap)
deps = PipelineDeps(config=config, inventory=Inventory(store),
                    translator=Translator(gateway, config, budget),
                    reporter=Reporter(store, config), store=store)

# COMMAND ----------

tol = dbutils.widgets.get("float_rel_tol")
rec = ProgramRecord(
    program_id=dbutils.widgets.get("program_id"),
    sas_path=dbutils.widgets.get("sas_path"),
    owner=dbutils.widgets.get("owner"),
    inputs=json.loads(dbutils.widgets.get("inputs_json")),
    ground_truth=json.loads(dbutils.widgets.get("ground_truth_json")),
    float_rel_tol=float(tol) if tol else None)
deps.inventory.register(rec)
outcome = migrate_program(spark, rec, deps)
print(f"RESULT: {outcome.status}  (mode={outcome.failure_mode}, "
      f"outer={outcome.outer_attempts}, run_repairs={outcome.total_run_repairs})")

# COMMAND ----------

# Show the certificate / triage report
latest = [r for r in store.scan("parity_results")
          if r["program_id"] == rec.program_id][-1]
displayHTML(f"<pre>{latest['report']}</pre>")
