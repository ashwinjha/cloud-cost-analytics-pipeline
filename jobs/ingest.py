from pyspark.sql import SparkSession

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CloudCostAnalytics")
        .master("local[*]")
        .getOrCreate()
    )


def read_raw_events(spark, paths):
    """
    Reads raw billing event CSV files.
    Raw data is treated as immutable.
    """
    dfs = []
    for path in paths:
        df = spark.read.option("header", True).csv(path)
        dfs.append(df)

    raw_df = dfs[0]
    for df in dfs[1:]:
        raw_df = raw_df.unionByName(df)

    return raw_df
