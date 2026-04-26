from dagster import ScheduleDefinition

from dagster_pipeline.jobs import adventure_works_full_pipeline

# Run the full pipeline every day at 02:00 UTC.
# AdventureWorks data is static (2011-2014), so this is mainly useful when the
# exchange-rate date range is extended or the CSV ratings file is updated.
adventure_works_daily = ScheduleDefinition(
    job=adventure_works_full_pipeline,
    cron_schedule="0 2 * * *",
    name="adventure_works_daily",
    description="Full Adventure Works pipeline — daily at 02:00 UTC.",
)
