# tests/__init__.py

from pyspark.sql import SparkSession

# Create one shared SparkSession for all tests
spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("AddressToolkitTests")
    .getOrCreate()
)

def make_df(data, columns):
    """
    Convenience helper for creating test DataFrames consistently.
    """
    return spark.createDataFrame(data, columns)
