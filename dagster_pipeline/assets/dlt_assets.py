"""
DLT extract assets — step 1 & 2 of the pipeline.

adventureworks_extract  →  loads AdventureWorks2014 + CSV ratings into aw-db.Extract
exchange_rates_extract  →  loads NBP USD/PLN rates into aw-db.Extract

Each @multi_asset emits one Dagster output per destination table so that
downstream dbt source assets can be wired correctly in the lineage graph.
"""

from dagster import AssetExecutionContext, AssetKey, AssetOut, Output, multi_asset

# Tables produced by run_pipeline() + run_csv_pipeline()
_AW_TABLES = [
    "product",
    "product_subcategory",
    "product_category",
    "sales_person",
    "sales_territory",
    "sales_order_header",
    "sales_order_detail",
    "person",
    "country_region",
    "product_rating",
]


@multi_asset(
    outs={
        table: AssetOut(key=AssetKey(["extract", table]), is_required=True)
        for table in _AW_TABLES
    },
    group_name="extract",
    description=(
        "Loads AdventureWorks2014 OLTP tables (Production / Sales / Person schemas) "
        "and CSV product ratings into aw-db.Extract via DLT."
    ),
)
def adventureworks_extract(context: AssetExecutionContext):
    from data_extract import run_csv_pipeline, run_pipeline

    context.log.info("Running AdventureWorks DLT pipeline (Production / Sales / Person)…")
    run_pipeline()
    context.log.info("Running CSV product-ratings DLT pipeline…")
    run_csv_pipeline()

    for table in _AW_TABLES:
        yield Output(value=None, output_name=table)


@multi_asset(
    outs={
        "currency_rate_data": AssetOut(
            key=AssetKey(["extract", "currency_rate_data"]),
            is_required=True,
        )
    },
    group_name="extract",
    description="Fetches NBP USD/PLN exchange rates into aw-db.Extract via DLT.",
)
def exchange_rates_extract(context: AssetExecutionContext):
    from exchange_rates import run_exchange_rate_pipeline

    context.log.info("Fetching NBP USD/PLN exchange rates…")
    run_exchange_rate_pipeline()

    yield Output(value=None, output_name="currency_rate_data")
