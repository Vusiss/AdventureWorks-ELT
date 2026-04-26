"""
dbt assets — steps 3 & 4 of the pipeline.

`adventure_works_dbt_assets` uses `dbt build` which runs every model then
immediately tests it, equivalent to `dbt run` followed by `dbt test` but with
per-model granularity.  Individual staging and mart models are visible as
separate assets in the Dagster UI.

Source tables (dbt `extract.*`) are mapped to the extract asset keys produced
by dlt_assets.py via the custom _DbtTranslator so Dagster draws the full
end-to-end lineage: DLT → staging → marts.

First-run setup:
  cd adventure_works_dbt && dbt parse
  (or just start `dagster dev` — prepare_if_dev() handles it automatically)
"""

import warnings
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

_DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "adventure_works_dbt"

dbt_project = DbtProject(project_dir=_DBT_PROJECT_DIR)

try:
    dbt_project.prepare_if_dev()
except Exception as exc:
    warnings.warn(
        f"Could not auto-generate dbt manifest ({exc}). "
        "Run 'dbt parse' inside adventure_works_dbt/ before starting Dagster."
    )


class _DbtTranslator(DagsterDbtTranslator):
    """Map dbt source tables → the extract asset keys emitted by the DLT assets."""

    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            source_name = dbt_resource_props["source_name"]
            # `identifier` is the physical table name; fall back to `name` if absent
            identifier = dbt_resource_props.get("identifier") or dbt_resource_props["name"]
            return AssetKey([source_name, identifier])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=_DbtTranslator(),
    name="transform"
)
def adventure_works_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Build staging + star-schema models and run data-quality tests (dbt build)."""
    yield from dbt.cli(["build"], context=context).stream()
