from pyspark.sql.functions import col

def run_quality_checks(df):
    """
    Basic data quality checks.
    """
    null_count = df.filter(
        col("event_date").isNull() |
        col("service_name").isNull() |
        col("project_id").isNull()
    ).count()

    if null_count > 0:
        raise Exception("Data quality check failed: null values found")

    if df.count() == 0:
        raise Exception("Data quality check failed: empty output")

    negative_costs = df.filter(col("total_cost_usd") < 0).count()
    if negative_costs > 0:
        raise Exception("Data quality check failed: negative costs detected")
