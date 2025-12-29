from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# 1. Start Spark
spark = SparkSession.builder \
    .appName("CloudCostAnalytics") \
    .master("local[*]") \
    .getOrCreate()

# 2. Read raw CSVs
day1_df = spark.read.option("header", True).csv("../data/billing_events_day1.csv")
day2_df = spark.read.option("header", True).csv("../data/billing_events_day2_late.csv")
backfill_df = spark.read.option("header", True).csv("../data/billing_events_day1_backfill.csv")


print("Reading done")

# 3. Append raw data 
raw_df = day1_df.unionByName(day2_df).unionByName(backfill_df)


print("Combine done")

# 4. Deduplicate
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

print("Running data quality checks...")

# 7. Null check on critical columns
null_count = daily_df.filter(
    col("event_date").isNull() |
    col("service_name").isNull() |
    col("project_id").isNull()
).count()

if null_count > 0:
    raise Exception("DATA QUALITY FAILURE: Null values found in critical columns")

# 8. Row count sanity
row_count = daily_df.count()
if row_count == 0:
    raise Exception("DATA QUALITY FAILURE: No rows in aggregated output")

# 9. Cost sanity check
negative_costs = daily_df.filter(col("total_cost_usd") < 0).count()
if negative_costs > 0:
    raise Exception("DATA QUALITY FAILURE: Negative costs detected")

print("All data quality checks passed")


spark.stop()


