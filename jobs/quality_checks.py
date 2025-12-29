print("Running data quality checks...")

# 1. Null check on critical columns
null_count = daily_df.filter(
    col("event_date").isNull() |
    col("service_name").isNull() |
    col("project_id").isNull()
).count()

if null_count > 0:
    raise Exception("DATA QUALITY FAILURE: Null values found in critical columns")

# 2. Row count sanity
row_count = daily_df.count()
if row_count == 0:
    raise Exception("DATA QUALITY FAILURE: No rows in aggregated output")

# 3. Cost sanity check
negative_costs = daily_df.filter(col("total_cost_usd") < 0).count()
if negative_costs > 0:
    raise Exception("DATA QUALITY FAILURE: Negative costs detected")

print("All data quality checks passed")


spark.stop()
