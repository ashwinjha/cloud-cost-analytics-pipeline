from ingest import create_spark_session, read_raw_events
from transform import deduplicate_events, aggregate_daily_cost
from quality_checks import run_quality_checks

spark = create_spark_session()

paths = [
    "../data/billing_events_day1.csv",
    "../data/billing_events_day2_late.csv",
    "../data/billing_events_day1_backfill.csv"
]

raw_df = read_raw_events(spark, paths)
dedup_df = deduplicate_events(raw_df)
daily_df = aggregate_daily_cost(dedup_df)

run_quality_checks(daily_df)

daily_df.show(truncate=False)

spark.stop()
