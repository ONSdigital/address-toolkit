from pyspark.sql import SparkSession

from address_toolkit.contextualising import contextualise_from_lookup
from address_toolkit.resources import town_lookup

spark = SparkSession.builder.master("local").appName("ExtractTest").getOrCreate()

def test_contextualise_from_lookup_processing():
    data = [("ID1", "123 Main Street, Ashford, TN23 1AA"), ("ID2", "456 High Street, Manchester, MAN1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    component_lookup = town_lookup

    contextualised_df = contextualise_from_lookup(df, "address", "town", component_lookup, create_flag = False, overwrite = True)
    contextualised_values = list(tuple(row) for row in contextualised_df.collect())
    contextualised_values = [tup[1] for tup in contextualised_values]

    expected_values = ["123 Main Street,Ashford,Kent,TN23 1AA","456 High Street,Manchester,MAN1 2BB"]

    assert contextualised_values == expected_values

def test_contextualise_from_lookup_flagging():
    data = [("ID1", "123 Main Street, Ashford, TN23 1AA"), ("ID2", "456 High Street, Manchester, MAN1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    component_lookup = town_lookup

    contextualised_df = contextualise_from_lookup(df, "address", "town", component_lookup, create_flag = True, overwrite = True)
    contextualised_values = list(tuple(row) for row in contextualised_df.collect())
    contextualised_values = [tup[2] for tup in contextualised_values]

    expected_flags = [1, 0]

    assert contextualised_values == expected_flags
