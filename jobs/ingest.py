spark = SparkSession.builder \
    .appName("CloudCostAnalytics") \
    .master("local[*]") \
    .getOrCreate()

# 2. Read raw CSVs
    """
    Reads raw billing event CSV files.
    Raw data is treated as immutable.
    """
day1_df = spark.read.option("header", True).csv("../data/billing_events_day1.csv")
day2_df = spark.read.option("header", True).csv("../data/billing_events_day2_late.csv")
backfill_df = spark.read.option("header", True).csv("../data/billing_events_day1_backfill.csv")


print("Reading done")
