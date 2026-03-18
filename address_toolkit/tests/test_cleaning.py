from pyspark.sql import SparkSession
import pytest
from tests import spark

from address_toolkit.cleaning import (
    clean_punctuation,
    deduplicate_addresses,
    deduplicate_postcodes,
    denoise_addresses,
    rectify_postcodes,
    standardise_street_types,
)


# Testing clean_punctuation
# ---------------------------
# 1. LEADING / TRAILING PUNCTUATION
# ---------------------------
def test_clean_punctuation_leading_and_trailing_characters():
    data = [
        ("ID1", ",,,,...123 Main Street, London,,,"),
        ("ID2", "...,,,45 High Road, Bristol,,")
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag=False, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "123 Main Street,London",
        "45 High Road,Bristol"
    ]

    assert cleaned == expected


# ---------------------------
# 2. MULTIPLE COMMAS + COMMA NORMALISATION
# ---------------------------
def test_clean_punctuation_multiple_commas_normalisation():
    data = [
        ("ID1", "123 Main Street,, London,,, SM1 1AA"),
        ("ID2", "BLOCK A,, 14,, SOME ROAD")
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag=False, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "123 Main Street,London,SM1 1AA",
        "BLOCK A,14,SOME ROAD"
    ]

    assert cleaned == expected


# ---------------------------
# 3. SPACING AROUND PERIODS AND HYPHENS
# ---------------------------
def test_clean_punctuation_spacing_around_periods_and_hyphens():
    data = [
        ("ID1", "FLAT 4 . 2, 67 HIGH STREET - 123, CITY - 45"),
        ("ID2", "APT 8 - 3 , 10 WEST ROAD . 4")
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag=False, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "FLAT 4.2,67 HIGH STREET-123,CITY-45",
        "APT 8-3,10 WEST ROAD.4"
    ]

    assert cleaned == expected


# ---------------------------
# 4. HYPHEN PRESERVATION BETWEEN NUMBERS
# ---------------------------
def test_clean_punctuation_preserve_hyphens_between_numbers():
    data = [
        ("ID1", "BLOCK A-1-2, 14 - 16, SOMEWHERE ROAD"),
        ("ID2", "UNIT 7-9, FLOOR 3-4")
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag=False, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "BLOCK A-1-2,14-16,SOMEWHERE ROAD",
        "UNIT 7-9,FLOOR 3-4"
    ]

    assert cleaned == expected


# ---------------------------
# 5. REMOVAL OF NOISE PUNCTUATION
# ---------------------------
def test_clean_punctuation_noise_removal():
    data = [
        ("ID1", ",,,, UNIT 9,, BLOCK-5..,, RANDOM ROAD,,"),
        ("ID2", "!!!???,, 22A,, TEST ROAD..,,")
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag=False, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "UNIT 9,BLOCK-5,RANDOM ROAD",
        "22A,TEST ROAD"
    ]

    assert cleaned == expected


# ---------------------------
# 6. OVERWRITE = FALSE MODE (NEW COLUMN ADDED)
# ---------------------------
def test_clean_punctuation_new_column_mode():
    data = [("ID1", "123,, Main Street,,, London")]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag=False, overwrite=False)

    assert "cleaned_punctuation" in cleaned_df.columns
    assert cleaned_df.collect()[0]["cleaned_punctuation"] == "123 Main Street,London"
    assert cleaned_df.collect()[0]["address"] == "123,, Main Street,,, London"  # original preserved


# ---------------------------
# 7. FLAG CREATION
# ---------------------------
def test_clean_punctuation_flag_creation():
    data = [
        ("ID1", "123  Main Street,, London, SM1 1AA"),  # needs cleaning → flag 1
        ("ID2", "456 High Street,Manchester,MAN1 2BB")  # no change → flag 0
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag=True, overwrite=True)
    flags = [row["punctuation_cleaned_flag"] for row in cleaned_df.collect()]

    expected = [1, 0]

    assert flags == expected


# ---------------------------
# 8. COMPLEX MULTI-ISSUE ADDRESSES
# ---------------------------
def test_clean_punctuation_complex_cases():
    data = [
        ("ID1", "..., 12- 13,, HIGH . ROAD -- LONDON,,, SM1 . 1AA"),
        ("ID2", "FLAT . 3 - 2,, BLOCK- 9,,     CITY,,"),
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = clean_punctuation(df, "address", create_flag=False, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "12-13,HIGH ROAD-LONDON,SM1.1AA",
        "FLAT 3-2,BLOCK-9,CITY"
    ]

    assert cleaned == expected

# Testing Denoise addresses
# ----------------------------------------------------------
# 1. BASIC NOISE REMOVAL 
# ----------------------------------------------------------
def test_denoise_basic_processing():
    data = [
        ("ID1", "123 Main Street, AAAA, London BBBB, SM1 1AA"),
        ("ID2", "456 High Street,Manchester,MAN1 2BB")
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = denoise_addresses(df, "address", create_flag=False, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "123 Main Street,London,SM1 1AA",
        "456 High Street,Manchester,MAN1 2BB"
    ]

    assert cleaned == expected


# ----------------------------------------------------------
# 2. FLAG CREATION (matches your existing tests)
# ----------------------------------------------------------
def test_denoise_flag_basic():
    data = [
        ("ID1", "123 Main Street, AAAA, London BBBB, SM1 1AA"),  # noise
        ("ID2", "456 High Street,Manchester,MAN1 2BB")           # no noise
    ]
    columns = ["id", "address"]
    df = spark.createDataFrame(data, columns)

    cleaned_df = denoise_addresses(df, "address", create_flag=True, overwrite=True)
    flags = [row["noise_removed_flag"] for row in cleaned_df.collect()]

    assert flags == [1, 0]


# ----------------------------------------------------------
# 3. MULTIPLE NOISE TOKENS IN SINGLE ADDRESS
# ----------------------------------------------------------
def test_denoise_multiple_noise_words():
    data = [
        ("ID1", "BLOCK AAAA AREA BBBBB SECTION CCCC ROAD"),
    ]
    columns = ["id", "address"]

    df = spark.createDataFrame(data, columns)
    cleaned_df = denoise_addresses(df, "address", create_flag=False, overwrite=True)

    cleaned = cleaned_df.collect()[0]["address"]

    expected = "BLOCK AREA SECTION ROAD"

    assert cleaned == expected


# ----------------------------------------------------------
# 4. NO NOISE PRESENT (string should be unchanged)
# ----------------------------------------------------------
def test_denoise_no_noise_present():
    data = [
        ("ID1", "123 Main Street, London"),
    ]
    columns = ["id", "address"]

    df = spark.createDataFrame(data, columns)
    cleaned_df = denoise_addresses(df, "address", create_flag=True, overwrite=True)

    cleaned = cleaned_df.collect()[0]["address"]
    flag = cleaned_df.collect()[0]["noise_removed_flag"]

    assert cleaned == "123 Main Street, London"
    assert flag == 0


# ----------------------------------------------------------
# 5. CUSTOM NOISE PATTERN (e.g., remove ANY numeric noise)
# ----------------------------------------------------------
def test_denoise_custom_pattern():
    data = [
        ("ID1", "123 Main Street, 9999, LONDON"),
        ("ID2", "22A ROAD, 444, SOUTH CITY"),
    ]
    columns = ["id", "address"]

    df = spark.createDataFrame(data, columns)

    # Remove numbers of length 3+
    custom_pattern = r"\b\d{3,}\b"

    cleaned_df = denoise_addresses(df, "address", noise_pattern=custom_pattern, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "123 Main Street,LONDON",
        "22A ROAD,SOUTH CITY"
    ]

    assert cleaned == expected


# ----------------------------------------------------------
# 6. NON-OVERWRITE MODE (should create new column)
# ----------------------------------------------------------
def test_denoise_new_column_mode():
    data = [
        ("ID1", "123 Main Street, AAAA, London")
    ]
    columns = ["id", "address"]

    df = spark.createDataFrame(data, columns)
    cleaned_df = denoise_addresses(df, "address", create_flag=False, overwrite=False)

    assert "cleaned_noise_words" in cleaned_df.columns
    assert cleaned_df.collect()[0]["cleaned_noise_words"] == "123 Main Street,London"
    assert cleaned_df.collect()[0]["address"] == "123 Main Street, AAAA, London"


# ----------------------------------------------------------
# 7. FLAGGING COMPLEX SCENARIOS
# ----------------------------------------------------------
def test_denoise_flag_multiple_cases():
    data = [
        ("ID1", "AAAA BLOCK BBBB ROAD"),    # noise → 1
        ("ID2", "BLOCK ROAD"),              # no noise → 0
        ("ID3", "123 BBBB STREET"),         # noise in middle → 1
    ]
    columns = ["id", "address"]

    df = spark.createDataFrame(data, columns)
    cleaned_df = denoise_addresses(df, "address", create_flag=True, overwrite=True)

    flags = [row["noise_removed_flag"] for row in cleaned_df.collect()]

    assert flags == [1, 0, 1]


# ----------------------------------------------------------
# 8. CHECK INTERACTION WITH clean_punctuation()
# ----------------------------------------------------------
def test_denoise_interaction_with_clean_punctuation():
    data = [
        ("ID1", "123 Main Street ,,, AAAA ,,, London"),
    ]
    columns = ["id", "address"]

    df = spark.createDataFrame(data, columns)
    cleaned_df = denoise_addresses(df, "address", create_flag=False, overwrite=True)

    cleaned = cleaned_df.collect()[0]["address"]

    expected = "123 Main Street,London"  # both noise removed *and* punctuation cleaned

    assert cleaned == expected


# ----------------------------------------------------------
# 9. LOWERCASE OR MIXED-CASE NOISE SHOULD NOT MATCH
# ----------------------------------------------------------
def test_denoise_mixed_case_not_removed():
    data = [
        ("ID1", "123 Main Street, aAaA, London"),
        ("ID2", "456 Road, bBbbB, CITY"),
    ]
    columns = ["id", "address"]

    df = spark.createDataFrame(data, columns)

    cleaned_df = denoise_addresses(df, "address", create_flag=True, overwrite=True)
    cleaned = [row["address"] for row in cleaned_df.collect()]
    flags = [row["noise_removed_flag"] for row in cleaned_df.collect()]

    # Pattern only matches AAAA, BBBB (same uppercase repeated)
    assert cleaned == [
        "123 Main Street,aAaA,London",     # unchanged
        "456 Road,bBbbB,CITY"              # unchanged
    ]
    assert flags == [0, 0]


# ----------------------------------------------------------
# 10. NOISE AT START / END / WITHOUT COMMAS
# ----------------------------------------------------------
def test_denoise_noise_in_various_positions():
    data = [
        ("ID1", "AAAA 123 MAIN STREET"),        # beginning
        ("ID2", "123 MAIN STREET BBBBB"),       # end
        ("ID3", "BLOCK CCCC AREA DDDDD ROAD")   # middle
    ]
    columns = ["id", "address"]

    df = spark.createDataFrame(data, columns)
    cleaned_df = denoise_addresses(df, "address", create_flag=False, overwrite=True)

    cleaned = [row["address"] for row in cleaned_df.collect()]

    expected = [
        "123 MAIN STREET",
        "123 MAIN STREET",
        "BLOCK AREA ROAD"
    ]

    assert cleaned == expected


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
