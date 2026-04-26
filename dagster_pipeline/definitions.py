"""
Dagster Definitions — single entry point loaded by workspace.yaml.

Asset groups:
  extract    →  DLT pipelines (adventureworks_extract, exchange_rates_extract)
  transform  →  dbt models + tests (all staging and mart models)

Jobs:
  adventure_works_full_pipeline  →  extract + transform end-to-end
  adventure_works_dbt_only       →  transform only (skip re-extraction)

Schedule:
  adventure_works_daily  →  full pipeline at 02:00 UTC
"""

from dagster import Definitions

from dagster_pipeline.assets.dbt_assets import adventure_works_dbt_assets
from dagster_pipeline.assets.dlt_assets import adventureworks_extract, exchange_rates_extract
from dagster_pipeline.jobs import adventure_works_dbt_only, adventure_works_full_pipeline
from dagster_pipeline.resources import dbt_resource
from dagster_pipeline.schedules import adventure_works_daily

defs = Definitions(
    assets=[
        adventureworks_extract,
        exchange_rates_extract,
        adventure_works_dbt_assets,
    ],
    resources={"dbt": dbt_resource},
    jobs=[adventure_works_full_pipeline, adventure_works_dbt_only],
    schedules=[adventure_works_daily],
)
