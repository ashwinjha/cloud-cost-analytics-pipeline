from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, sum as _sum
from pyspark.sql.types import DoubleType

def deduplicate_events(raw_df):
    """
    Deduplicate events using latest ingestion_date (latest wins).
    """
    window = Window.partitionBy("event_id").orderBy(col("ingestion_date").desc())

    return (
        raw_df
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def aggregate_daily_cost(dedup_df):
    """
    Aggregate daily cost per service per project.
    """
    dedup_df = dedup_df.withColumn(
        "cost_usd", col("cost_usd").cast(DoubleType())
    )

    return (
        dedup_df
        .groupBy("event_date", "service_name", "project_id")
        .agg(_sum("cost_usd").alias("total_cost_usd"))
    )
