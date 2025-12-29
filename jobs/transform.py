
# 3. Append raw data (immutable)
raw_df = day1_df.unionByName(day2_df).unionByName(backfill_df)


print("Combine done")

# 4. Deduplicate (latest ingestion_date wins)
window_spec = Window.partitionBy("event_id").orderBy(col("ingestion_date").desc())

dedup_df = (
    raw_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print("Deduplication done")

from pyspark.sql.types import DoubleType

dedup_df = dedup_df.withColumn(
    "cost_usd",
    col("cost_usd").cast(DoubleType())
)

# 5. Daily aggregation
daily_df = (
    dedup_df
    .groupBy("event_date", "service_name", "project_id")
    .sum("cost_usd")
    .withColumnRenamed("sum(cost_usd)", "total_cost_usd")
)

print("Aggregation done")

# 6. SHOW RESULT 
daily_df.show(truncate=False)
