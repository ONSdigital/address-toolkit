from pyspark.sql import SparkSession

from address_toolkit.extracting import (
    extract_components_from_list,
    extract_components_from_regex,
    extract_postcodes,
)
from address_toolkit.resources import floor_regex, town_list

spark = SparkSession.builder.master("local").appName("ExtractTest").getOrCreate()

def test_extract_postcodes():

    data = [("ID1", "123 Main Street, London, SM1 1AA"), ("ID2", "456 High Str, Manchester, MA1 2AB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    extracted_df = extract_postcodes(df, "address", replace = False)
    extracted_df_values = list(tuple(row) for row in extracted_df.collect())
    extracted_postcodes = [tup[2] for tup in extracted_df_values]

    expected_postcodes = ["SM1 1AA", "MA1 2AB"]

    assert extracted_postcodes == expected_postcodes

def test_extract_from_list():

    data = [("ID1", "123 Main Street, bournemouth, SM1 1AA"), ("ID2", "456 High Street, Barmouth")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    extracted_df = extract_components_from_list(df, "address", component_name = 'town', component_list = town_list, similarity_threshold=95)
    extracted_df_values = list(tuple(row) for row in extracted_df.collect())
    extracted_towns = [tup[2] for tup in extracted_df_values]

    expected_towns = ["BOURNEMOUTH", ""]

    assert extracted_towns == expected_towns

def test_extract_components_from_regex():

    data = [("ID1", "123 Main Street, Basement, SM1 1AA"), ("ID2", "456 High Street, First Floor")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    extracted_df = extract_components_from_regex(df, "address", "floor_extracted", floor_regex)
    extracted_df_values = list(tuple(row) for row in extracted_df.collect())
    extracted_floors = [tup[2] for tup in extracted_df_values]

    expected_floors = ["Basement", "First Floor"]

    assert extracted_floors == expected_floors