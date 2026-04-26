from dagster import AssetSelection, define_asset_job

adventure_works_full_pipeline = define_asset_job(
    name="adventure_works_full_pipeline",
    selection=AssetSelection.groups("extract", "transform"),
    description=(
        "Full ETL pipeline: "
        "DLT extract (AdventureWorks + CSV ratings + exchange rates) "
        "→ dbt build (staging + star schema + data-quality tests)."
    ),
)

adventure_works_dbt_only = define_asset_job(
    name="adventure_works_dbt_only",
    selection=AssetSelection.groups("transform"),
    description="Re-run dbt build without re-extracting source data.",
)
