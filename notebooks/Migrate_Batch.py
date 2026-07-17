# Databricks notebook source
# MAGIC %md
# MAGIC # Migrate Batch
# MAGIC Central-team notebook: processes every pending program in the inventory
# MAGIC (register programs via `Inventory.register` first), resumable — programs
# MAGIC already at parity_pass are skipped automatically.

# COMMAND ----------

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import RestGatewayClient, TokenBudget
from sas_migrate.inventory import Inventory
from sas_migrate.pipeline import PipelineDeps, migrate_batch
from sas_migrate.report import Reporter
from sas_migrate.statestore import DeltaStateStore
from sas_migrate.translate import Translator

config = MigrationConfig(
    gateway_base_url=dbutils.secrets.get("sas2dbx", "gateway_url"))
store = DeltaStateStore(spark, config)
gateway = RestGatewayClient(
    config, auth_token=dbutils.secrets.get("sas2dbx", "gateway_token"),
    on_call=lambda rec: store.append("llm_calls", rec))
deps = PipelineDeps(config=config, inventory=Inventory(store),
                    translator=Translator(gateway, config,
                                          TokenBudget(config.per_program_token_cap)),
                    reporter=Reporter(store, config), store=store)

# COMMAND ----------

results = migrate_batch(spark, deps)
print(f"parity_pass: {len(results['parity_pass'])}  triage: {len(results['triage'])}")

# COMMAND ----------

# Status funnel
import pandas as pd
rows = store.scan("inventory")
display(spark.createDataFrame(pd.DataFrame(rows))
        .groupBy("status").count().orderBy("status"))
