from pyspark.sql import SparkSession

from address_toolkit.cleaning import (
    clean_punctuation,
    deduplicate_addresses,
    deduplicate_postcodes,
    denoise_addresses,
    rectify_postcodes,
    standardise_street_types,
)

spark = SparkSession.builder.master("local").appName("PreProcessingTest").getOrCreate()

def test_clean_punctuation_processing():

    data = [("ID1", "123  Main Street,, London, SM1 1AA"), ("ID2", "456 High Street, Manchester, MAN1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag = False, overwrite=True)

    expected_data = [("ID1","123 Main Street,London,SM1 1AA"), ("ID2", "456 High Street,Manchester,MAN1 2BB")]
    expected_df = spark.createDataFrame(expected_data, columns)

    cleaned_values = set(tuple(row) for row in cleaned_df.collect())
    expected_values = set(tuple(row) for row in expected_df.collect())

    assert cleaned_values == expected_values

def test_clean_punctuation_flags():

    data = [("ID1", "123  Main Street,, London, SM1 1AA"), ("ID2", "456 High Street,Manchester,MAN1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag = True, overwrite=True)
    cleaned_values = list(tuple(row) for row in cleaned_df.collect())
    cleaned_flags = [tup[2] for tup in cleaned_values]

    expected_flags = [1, 0]

    assert cleaned_flags == expected_flags

#Currently not passing tests, need to figure out how function behaves or if test needs some working on...
def test_denoise_processing():

    data = [("ID1", "123 Main Street, AAAA, London BBBB, SM1 1AA"), ("ID2", "456 High Street,Manchester,MAN1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = denoise_addresses(df, "address", create_flag = False, overwrite = True)
    cleaned_df_values = list(tuple(row) for row in cleaned_df.collect())
    cleaned_values = [tup[1] for tup in cleaned_df_values]

    expected_values = ["123 Main Street,London,SM1 1AA", "456 High Street,Manchester,MAN1 2BB"]

    assert cleaned_values == expected_values

def test_denoise_flags():

    data = [("ID1", "123 Main Street, AAAA, London BBBB, SM1 1AA"), ("ID2", "456 High Street,Manchester,MAN1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = denoise_addresses(df, "address", create_flag = True, overwrite = True)
    cleaned_df_values = list(tuple(row) for row in cleaned_df.collect())
    cleaned_flags = [tup[2] for tup in cleaned_df_values]

    expected_flags = [1, 0]

    assert cleaned_flags == expected_flags

def test_deduplicate_address_processing():

    data = [("ID1", "123 Main Street Street,Main Street,London London,SM1 1AA"), ("ID2", "456 High Street,Manchester,MA1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    deduplicated_df = deduplicate_addresses(df, "address", intracomponents = True, intercomponents = True, tolerance = 0, create_flag = False, overwrite = True)
    deduplicated_df_values = list(tuple(row) for row in deduplicated_df.collect())
    deduplicated_values = [tup[1] for tup in deduplicated_df_values]

    expected_values = ["123 Main Street,London,SM1 1AA", "456 High Street,Manchester,MA1 2BB"]

    assert deduplicated_values == expected_values

def test_deduplicate_address_flags():
    data = [("ID1", "123 Main Street Street,Main Street,London London,SM1 1AA"), ("ID2", "456 High Street,Manchester,MA1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    deduplicated_df = deduplicate_addresses(df, "address", intercomponents = True, intracomponents = True, tolerance = 0, create_flag = True, overwrite = True)
    deduplicated_df_values = list(tuple(row) for row in deduplicated_df.collect())
    deduplicated_flags = [tup[2] for tup in deduplicated_df_values]

    expected_flags = [1, 0]

    assert deduplicated_flags == expected_flags

def test_deduplicate_postcodes_processing():

    data = [("ID1", "123 Main Street, London, SM1 1AA, SM1 1AA"), ("ID2", "456 High Street, Manchester, MA12bb, MA1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    deduplicated_df = deduplicate_postcodes(df, "address", create_flag = False, overwrite = True)
    deduplicated_df_values = list(tuple(row) for row in deduplicated_df.collect())
    deduplicated_values = [tup[1] for tup in deduplicated_df_values]

    expected_values = ["123 Main Street,London,SM1 1AA", "456 High Street,Manchester,MA1 2BB"]

    assert deduplicated_values == expected_values

def test_deduplicate_postcodes_flags():

    data = [("ID1", "123 Main Street, London, SM1 1AA, SM1 1AA"), ("ID2", "456 High Street, Manchester, MA12BB, MA1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    deduplicated_df = deduplicate_postcodes(df, "address", create_flag = True, overwrite = True)
    deduplicated_df_values = list(tuple(row) for row in deduplicated_df.collect())
    deduplicated_flags = [tup[2] for tup in deduplicated_df_values]

    expected_flags = [1, 1]

    assert deduplicated_flags == expected_flags

def test_rectify_postcodes_processing():

    data = [("ID1", "123 Main Street,London,S M1 1AA"), ("ID2", "456 High Street,Manchester,MA1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    rectified_df = rectify_postcodes(df, "address", create_flag = False, overwrite = True)
    rectified_df_values = list(tuple(row) for row in rectified_df.collect())
    rectified_values = [tup[1] for tup in rectified_df_values]

    expected_values = ["123 Main Street,London,SM1 1AA", "456 High Street,Manchester,MA1 2BB"]
    print(rectified_values)

    assert rectified_values == expected_values

def test_rectify_postcodes_flags():

    data = [("ID1", "123 Main Street, London, S M1 1AA"), ("ID2", "456 High Street, Manchester, MA1 2BB")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    rectified_df = rectify_postcodes(df, "address", create_flag = True, overwrite = True)
    rectified_df_values = list(tuple(row) for row in rectified_df.collect())
    rectified_flags = [tup[2] for tup in rectified_df_values]

    expected_flags = [1, 0]

    assert rectified_flags == expected_flags

def test_standardise_street_types_processing():

    data = [("ID1", "123 Main Str, London"), ("ID2", "456 High Rd, Manchester")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    standardised_df = standardise_street_types(df, "address", create_flag = False, overwrite = True)
    standardised_df_values = list(tuple(row) for row in standardised_df.collect())
    standardised_values = [tup[1] for tup in standardised_df_values]

    expected_values = ["123 Main Street, London", "456 High Road, Manchester"]

    assert standardised_values == expected_values

def test_standardise_street_types_flags():

    data = [("ID1", "123 Main Str, London"), ("ID2", "456 High Rd, Manchester")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    standardised_df = standardise_street_types(df, "address", create_flag = True, overwrite = True)
    standardised_df_values = list(tuple(row) for row in standardised_df.collect())
    standardised_flags = [tup[2] for tup in standardised_df_values]

    expected_flags = [1, 1]

    assert standardised_flags == expected_flags
