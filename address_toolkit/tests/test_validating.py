from pyspark.sql import SparkSession

from address_toolkit.resources import town_list
from address_toolkit.validating import (
    validate_from_list,
    validate_from_regex,
    validate_postcodes,
)

spark = SparkSession.builder.master("local").appName("PreProcessingTest").getOrCreate()

def test_validate_from_list():

    data = [("ID1", "123 Main Street, bournemouth, SM1 1AA"), ("ID2", "456 High Street, United Kingdom")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    validated_df = validate_from_list(df, "address", component_name = 'town', component_list = town_list, similarity_threshold = 95)
    validated_df_values = list(tuple(row) for row in validated_df.collect())
    validated_flags = [tup[2] for tup in validated_df_values]

    expected_flags = [1, 0]

    assert validated_flags == expected_flags

def test_validate_postcodes():

    data = [("ID1", "123 Main Street, London, SM1 1AA"), ("ID2", "456 High Str, Manchester, MA1 2AB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    validated_df = validate_postcodes(df, "address")
    validated_df_values = list(tuple(row) for row in validated_df.collect())
    validated_flags = [tup[2] for tup in validated_df_values]

    expected_flags = [1, 1]

    assert validated_flags == expected_flags

def test_validate_from_regex():

    data = [("ID1", "123 Main Street, London, SM1 1AA"), ("ID2", "Flat 3, 456 High Str, Manchester, MA1 2AB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    from address_toolkit.resources import flat_regex

    validated_df = validate_from_regex(df, "address", component_name = 'flat', regex_pattern = flat_regex)
    validated_df_values = list(tuple(row) for row in validated_df.collect())
    validated_flags = [tup[2] for tup in validated_df_values]

    expected_flags = [0, 1]

    assert validated_flags == expected_flags
