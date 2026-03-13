from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    initcap,
    lit,
    regexp_extract,
    regexp_replace,
    split,
    udf,
    upper,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from address_toolkit.resources import postcode_regex
from address_toolkit.utilities import (
    clean_part,
    deduplicate_intercomponents,
    deduplicate_intracomponents,
    deduplicate_postcode,
    rectify_postcode,
)

####################################################################################

def clean_punctuation(df: DataFrame, input_col: str, create_flag: bool =True, overwrite: bool=False) -> DataFrame:
    """
    Cleans up punctuation from address strings by removing or fixing unwanted characters, while preserving hyphens
    and periods where necessary (e.g., between numbers or block names).

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The name of the column containing the address strings to be cleaned.
    - create_flag (bool): Whether to create a flag indicating if punctuation was cleaned (default is True).
    - overwrite (bool): Whether to overwrite the input column with cleaned addresses (default is False).

    Returns:
    - df (DataFrame): A DataFrame with cleaned address strings in 'final_cleaned_address', and an optional flag
                      ('punctuation_cleaned_flag') indicating if changes were made.

    Examples:
    - Input: ",.123- MAIN STREET, LONDON,"
      Output: "123 MAIN STREET, LONDON"
      (Leading commas and periods are removed, and the hyphen is preserved between numbers.)

    - Input: "BLOCK A-1-2, 14 - 16, SOMEWHERE ROAD"
      Output: "BLOCK A-1-2, 14-16, SOMEWHERE ROAD"
      (Hyphens between alphanumeric chars are preserved, while hyphens between numbers are standardised.)

    - Input: "FLAT 4 . 2, 67 HIGH STREET - 123, CITY - 45, TOWN"
      Output: "FLAT 4.2, 67 HIGH STREET-123, CITY-45, TOWN"
      (Unnecessary spaces around periods and hyphens are removed, standardising punctuation between numbers and alphanumeric strings.)

    - Input: ",,,, UNIT 9,, BLOCK-5,, RANDOM ROAD,,"
      Output: "UNIT 9, BLOCK-5, RANDOM ROAD"
      (Multiple commas and unnecessary punctuation are cleaned up to create a clearer string.)

    """
    # Apply cleaning logic to each address part
    @udf(ArrayType(StringType()))
    def clean_parts_udf(parts):
        return [clean_part(part) for part in parts]

    # Step 1: Temporarily preserve hyphens between numbers or in special cases
    df = df.withColumn("cleaned_address", regexp_replace(col(input_col), r'(?<=\d)-(?=\d)', ' TEMP_HYPHEN '))
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r'(?<=\d)\.\s*(?=\d)', ' TEMP_DOT '))
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r'(?<=\w)-(?=\d\w)', ' TEMP_HYPHEN '))

    # Step 2: Handle punctuation marks and spaces, excluding preserved hyphens and periods
    df = df.withColumn("cleaned_address",
                       regexp_replace(col("cleaned_address"),
                                      r"[\s,.-]*\.,[\s,.-]*|[\s,.-]+\,|,\s*[\s,.-]+",
                                      ","))
    df = df.withColumn("cleaned_address",
                       regexp_replace(col("cleaned_address"),
                                      r",\s*,|(^[\s,.-]+)|([\s,.-]+$)",
                                      ","))

    # Step 3: Remove leading punctuation or commas at the start of the string (again after transformations)
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r"^[,.\s]+", ""))

    # Step 4: Split the address into parts to handle each part separately
    df = df.withColumn("address_parts", split(col("cleaned_address"), ",\\s*"))

    # Step 5: Clean each part and join back into a single address string
    df = df.withColumn("cleaned_parts", clean_parts_udf(col("address_parts")))
    df = df.withColumn("final_cleaned_address", concat_ws(",", col("cleaned_parts")))

    # Step 6: Restore preserved hyphens and periods in the final cleaned address
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), ' TEMP_HYPHEN ', '-'))
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), ' TEMP_DOT ', '.'))

    # Step 7: Remove any trailing commas and spaces in the final cleaned address
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), r",\s*$", ""))

    # Step 8: If user toggled create_flag, create a flag indicating whether punctuation was cleaned
    if create_flag:
        df = df.withColumn("punctuation_cleaned_flag",
                           when(col(input_col) == col("final_cleaned_address"), 0).otherwise(1))

    # Step 9: Overwrite the input column if specified, otherwise write to new column "cleaned_addresses"
    if overwrite:
        df = df.withColumn(input_col, col("final_cleaned_address"))
    else:
        df = df.withColumnRenamed("final_cleaned_address", "cleaned_punctuation")

    # Drop intermediate columns
    df = df.drop("cleaned_address", "address_parts", "cleaned_parts", "final_cleaned_address")

    return df

##############################################################################

def denoise_addresses(df: DataFrame, input_col: str, noise_pattern: str = r"\b([A-Z])\1{3,}\b", create_flag: bool =True, overwrite: bool =False) -> DataFrame:
    """
    Removes noise words from the input address column and flags any rows where noise words were removed.
    Noise words are any of those matching the noise pattern provided. By default, noise words will be defined as sequences of the same uppercase letter
    repeated three or more times (e.g., "AAAA", "BBBBB").

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The name of the column containing the address strings to be cleaned (default is "final_cleaned_address").
    - noise_pattern (str): A regex pattern defining noise words to be removed (default matches sequences of the same uppercase letter repeated 3 or more times).
    - create_flag (bool): Whether to create a flag indicating if noise words were removed (default is True).
    - overwrite (bool): Whether to overwrite the input column with cleaned addresses (default is False).

    Returns:
    - df (DataFrame): The updated DataFrame with noise words removed and a flag ('noise_removed_flag') indicating
                      whether any noise words were removed.

    Example:
    - Input: "123 MAIN ROAD, AAAA, LONDON"
    - Output: "123 MAIN ROAD, LONDON"
      (The noise word "AAAA" is removed, and the 'noise_removed_flag' is set to 1 for this row.)

    """
    # Replace noise words in the input address column with an empty string
    df = df.withColumn("cleaned_address", regexp_replace(col(input_col), noise_pattern, ""))

    # Create a flag that indicates whether noise words have been removed
    if create_flag:
        df = df.withColumn("noise_removed_flag", when(col("cleaned_address") != col(input_col), 1).otherwise(0))

    # Update the original address column with the cleaned address and drop the column created for this function
    if overwrite:
        df = df.drop(input_col)
        df = df.withColumnRenamed("cleaned_address", input_col)
        df = clean_punctuation(df, input_col, create_flag = False, overwrite = True)
    else:
        df = df.withColumnRenamed("cleaned_address", "cleaned_noise_words")
        df = clean_punctuation(df, "cleaned_noise_words", create_flag = False, overwrite = True)

    return df

  ###################################################################################
def deduplicate_addresses(df: DataFrame, input_col: str, intracomponents: bool = True, intercomponents: bool = True, tolerance: int = 3, create_flag: bool =True, overwrite: bool =False) -> DataFrame:
    """
    Deduplicates addresses intercomponently and intracomponently based on a similarity threshold and tolerance.

    Parameters:
    - df (DataFrame): The input spark DataFrame containing address data.
    - input_col (str): The name of the address column to process.
    - intracomponents (bool): Whether to perform intracomponent deduplication (default is True).
    - intercomponents (bool): Whether to perform intercomponent deduplication (default is True).
    - tolerance (int): The minimum length of parts to consider for intracomponentdeduplication (default is 3).
    - create_flag (bool): Whether to create a flag indicating if deduplication was performed (default is True).
    - overwrite (bool): Whether to overwrite the input column with deduplicated addresses (default is False).

    Returns:
    - df (DataFrame): The updated DataFrame with deduplicated addresses resolved and a flag ('words_deduplicated_flag') indicating
                      which addresses were resolved.

    Example:
    - Input: "123 MAIN ROAD, MAIN ROAD, LONDON"
    - Output: ("123 MAIN ROAD, LONDON", 1)
      (The repeated part "MAIN ROAD" is removed, and the 'deduplicated_addresses_flag' is set to 1.)

    """
    def _deduplicate_address(address, tolerance = tolerance, intracomponents = intracomponents, intercomponents = intercomponents):
        """
        Deduplicates an address string both intracomponently and intercomponently.

        Parameters:
        - address (str): The address string to be deduplicated.
        - tolerance (int): The minimum length of parts to consider for intracomponent deduplication.
        - intracomponents (bool): Whether to perform intracomponent deduplication.
        - intercomponents (bool): Whether to perform intercomponent deduplication.

        Returns:
        - deduplicated_address_string (str): The deduplicated address string.
        - changes_flag (int): Flag indicating whether any deduplication changes were made (1 if changes were made, 0 otherwise).

        """
        # Drop instances of duplicated intracomponent parts
        components = [part for part in address.split(',')]

        if intracomponents:
            intracomponents_parts = [part.split(' ') for part in components]
            deduplicated_address_string = deduplicate_intracomponents(intracomponents_parts, tolerance)
        else:
            deduplicated_address_string = address

        if intercomponents:
            components = [part for part in deduplicated_address_string.split(',')]
            deduplicated_address_string = deduplicate_intercomponents(components, tolerance)

        changes_flag = 1 if deduplicated_address_string != address else 0

        return deduplicated_address_string, changes_flag

    process_and_deduplicate_address_udf = udf(_deduplicate_address, StructType([
        StructField("deduplicated_addresses", StringType()),
        StructField("deduplicated_address_flag", IntegerType())
    ]))

    results_struct = process_and_deduplicate_address_udf(df[input_col])
    df = df.withColumn("deduplicated_addresses", results_struct["deduplicated_addresses"])
    df = clean_punctuation(df, "deduplicated_addresses", create_flag = False, overwrite = True)

    # If user toggled create_flag, create a flag indicating whether cleaning was done
    if create_flag:
        df = df.withColumn("deduplicated_address_flag", results_struct["deduplicated_address_flag"])
    # Overwrite the input column if specified
    if overwrite:
        df = df.drop(input_col)
        df = df.withColumnRenamed("deduplicated_addresses", input_col)

    return df

###################################################################################

def deduplicate_postcodes(df: DataFrame, input_col: str, create_flag: bool=True, overwrite: bool=False) -> DataFrame:

    """
    Deduplicates postcodes from a given address string.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - create_flag (bool): Whether to create a flag indicating if postcode deduplication was performed (default is True).
    - overwrite (bool): Whether to overwrite the input column with deduplicated addresses (default is False).

    Returns:
    - DataFrame: The updated DataFrame.

    """
    # Register UDF and apply to DataFrame
    deduplicate_postcodes_udf = udf(deduplicate_postcode, StructType([
        StructField("cleaned_duplicated_postcodes", StringType(), True),
        StructField("postcode_deduplicated_flag", IntegerType(), True)
    ]))

    result_struct = deduplicate_postcodes_udf(df[input_col])
    df = df.withColumn("cleaned_duplicated_postcodes", result_struct["cleaned_duplicated_postcodes"])
    df = clean_punctuation(df, "cleaned_duplicated_postcodes", create_flag = False, overwrite = True)

    # If user toggled create_flag, create a flag indicating whether deduplication was done
    if create_flag:
        df = df.withColumn("postcode_deduplicated_flag", result_struct["postcode_deduplicated_flag"])

    # Overwrite the input column if specified
    if overwrite:
        df = df.drop(input_col)
        df = df.withColumnRenamed("cleaned_duplicated_postcodes", input_col)

    return df

#############################################################################################
def rectify_postcodes(df: DataFrame, input_col: str, create_flag: bool=True, overwrite: bool=False) -> DataFrame:

    """
    Rectifies postcodes which are in unconventional formats in a given address string.
    For example 'S E1 8E N' would be rectified to 'SE1 8EN'.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - create_flag (bool): Whether to create a flag indicating if postcode rectification was performed
                          (default is True).
    - overwrite (bool): Whether to overwrite the input column with rectified addresses (default is False).

    Returns:
    - DataFrame: The updated DataFrame.

    Caveats:
    - The function will only rectify those postcodes for which are determinable within an isolated address component
    (between the commas).
    """

    rectify_postcodes_udf = udf(rectify_postcode, StructType([
        StructField("rectified_address", StringType()),
        StructField("rectified_postcode_flag", IntegerType())
    ]))

    result_struct = rectify_postcodes_udf(df[input_col])
    df = df.withColumn("rectified_address", result_struct["rectified_address"])

    # If user toggled create_flag, create a flag indicating whether cleaning was done
    if create_flag:
        df = df.withColumn("rectified_postcode_flag", result_struct["rectified_postcode_flag"])

    # Overwrite the input column if specified
    if overwrite:
        df = df.drop(input_col)
        df = df.withColumnRenamed("rectified_address", input_col)

    return df

#############################################################################################

def standardise_street_types(df: DataFrame, input_col: str, create_flag: bool =True, overwrite: bool=False) -> DataFrame:
    """
    Standardises street type abbreviations and common misspellings within an address column,
    applying a set of predefined rules to replace short forms like 'ST' with 'STREET' and
    fix common typos. Adds a flag to indicate if any standardisation occurred.

    The function performs the following steps:
    1. Identifies and replaces abbreviations or misspellings of street types (e.g., 'ST' becomes 'STREET').
    2. Applies regex-based transformations for common street type abbreviations such as:
       - 'STR' or 'STRT' -> 'STREET'
       - 'ST' (not followed by 'REET') -> 'STREET'
       - 'RD' or 'RAOD' -> 'ROAD'
       - 'AVE', 'AVE.' or 'AVENEU' -> 'AVENUE'
       - 'CRT', 'CRT.', or 'CT' -> 'COURT'
       - 'CRESENT' or 'CRSNT' -> 'CRESCENT'
       - 'DRV' or 'DR' -> 'DRIVE'
       - 'GRDN' or 'GDN' -> 'GARDEN'
       - 'PK' -> 'PARK'
       - 'CL' -> 'CLOSE'
    3. Compares the original and modified address columns to determine if any changes were made.
    4. Adds a flag (`street_type_standardised_flag`) indicating whether any street type standardisation occurred.

    Parameters:
    ----------
    df (pyspark.sql.DataFrame): The input DataFrame containing address data.
    input_col (str): The name of the column containing the address strings to be processed.
    create_flag (bool): Whether to create a flag indicating if standardisation was performed (default is True).
    overwrite (bool): Whether to overwrite the input column with the standardised addresses (default is False).

    Returns:
    --------
    df (pyspark.sql.DataFrame): The updated DataFrame containing:
        - The standardised address column with street type abbreviations expanded and misspellings corrected.
        - 'street_type_standardised_flag' (IntegerType): A flag (1 if changes were made, 0 otherwise) indicating whether any standardisation occurred.

    Example:
    -------
    Input: "123 MAIN ST, LONDON"
    Output: "123 MAIN STREET, LONDON", 1  # 'ST' expanded to 'STREET'

    Input: "456 PARK AVE., CITY"
    Output: "456 PARK AVENUE, CITY", 1  # 'AVE.' corrected to 'AVENUE'

    Input: "789 GARDEN CRT, TOWN"
    Output: "789 GARDEN COURT, TOWN", 1  # 'CRT' corrected to 'COURT'

    Input: "321 DRIVE LANE"
    Output: "321 DRIVE LANE", 0  # No changes, street type already standardised
    """
    original_column = col(input_col)
    destination_col = "standardised_street_addresses"

    # Apply standardisation rules for street types
    df = df.withColumn(destination_col, regexp_replace(original_column, r'(?i)\bSTR\b|\bSTRT\b', 'Street'))
    df = df.withColumn(destination_col, regexp_replace(col(destination_col), r'(?i)\bRD\b|RAOD', 'Road'))
    df = df.withColumn(destination_col, regexp_replace(col(destination_col), r'(?i)\bAVE\b|\bAVE\.\b|\bAVENEU\b', 'Avenue'))
    df = df.withColumn(destination_col, regexp_replace(col(destination_col), r'(?i)\bCRT\b|\bCRT\.\b|\bCT\b', 'Court'))
    df = df.withColumn(destination_col, regexp_replace(col(destination_col), r'(?i)\bCRESENT\b|\bCRSNT\b', 'Crescent'))
    df = df.withColumn(destination_col, regexp_replace(col(destination_col), r'(?i)\bDRV\b|\bDR\b', 'Drive'))
    df = df.withColumn(destination_col, regexp_replace(col(destination_col), r'(?i)\bGRDN(?=S\b)?\b|\bGDN(?=S\b)?\b', 'Garden'))
    df = df.withColumn(destination_col, regexp_replace(col(destination_col), r'(?i)\bPK\b', 'Park'))
    df = df.withColumn(destination_col, regexp_replace(col(destination_col), r'(?i)\bCL\b', 'Close'))

    # Add a flag to indicate if any standardization has occurred by comparing the original column and the modified one
    if create_flag:
        df = df.withColumn(
            'street_type_standardised_flag',
            when(col(destination_col) != original_column, lit(1)).otherwise(lit(0))
        )

    if overwrite:
        df = df.drop(input_col)
        df = df.withColumnRenamed(destination_col, input_col)

    return df

#############################################################################################

def prettify_addresses(df: DataFrame, input_col: str, overwrite: bool = True) -> DataFrame:

    """
    Formats addresses by applying title case to the main address components while ensuring that postcodes are in uppercase.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The name of the column containing the address strings to be formatted.
    - overwrite (bool): Whether to overwrite the input column with the prettified addresses (default is True).
    """

    # Create spacing between commas
    df = df.withColumn(input_col, regexp_replace(col(input_col), r",\s*", ", "))

    # Extract Postcode
    df = df.withColumn("extracted_postcode", regexp_extract(col(input_col), postcode_regex, 0))

    # Replace extracted postcode in the address with an empty string
    df = df.withColumn("prettified_address", regexp_replace(col(input_col), postcode_regex, ""))

    # Perform title method on remaining address string and concatenate with extracted postcode
    df = df.withColumn("prettified_address", concat_ws(",", initcap(col("prettified_address")), upper(col("extracted_postcode"))))

    # Overwrite the input column if specified
    if overwrite:
        df = df.drop(input_col)
        df = df.withColumnRenamed("prettified_address", input_col)
        df = clean_punctuation(df, input_col, create_flag = False, overwrite = True)

    else:
        df = df.withColumnRenamed("prettified_address", "prettified_addresses")
        df = clean_punctuation(df, "prettified_addresses", create_flag = False, overwrite = True)

    df = df.drop("extracted_postcode")

    return df
