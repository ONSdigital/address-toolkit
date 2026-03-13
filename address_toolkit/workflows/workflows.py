from pyspark.sql import DataFrame

from address_toolkit.cleaning import (
    clean_punctuation,
    deduplicate_addresses,
    deduplicate_postcodes,
    denoise_addresses,
    prettify_addresses,
    rectify_postcodes,
    standardise_street_types,
)
from address_toolkit.extracting import (
    extract_components_from_list,
    extract_postcodes,
)
from address_toolkit.resources import (
    city_list,
    county_list,
    town_list,
)
from address_toolkit.validating import validate_from_list, validate_postcodes

#################################################################################

def clean_addresses(df: DataFrame, input_col: str, create_flag: bool = False) -> DataFrame:
    """
    Cleans address data by applying a series of cleaning functions including punctuation cleaning,
    denoising, deduplication of addresses and postcodes, postcode rectification, and street type standardization.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - create_flag (bool, optional): Whether to create flag columns for each cleaning step. Defaults to False.

    Returns:
    - DataFrame: The cleaned DataFrame with updated address information and associated flags.

    Notes:
    For denoising addresses, the default pattern will be used which removes noise words defined as 3 or more consecutive letters e.g. "AAAA".
    For deduplicating addresses, intercomponent duplicates will be dropped and intracomponent duplicates preserved.

    """
    # Step 1: Clean punctuation
    df = clean_punctuation(df, input_col, create_flag=create_flag, overwrite=True)

    # Step 2: Standardise street types
    df = standardise_street_types(df, input_col, create_flag=create_flag, overwrite=True)

    # Step 2: Denoise addresses
    df = denoise_addresses(df, input_col, create_flag=create_flag, overwrite=True)

    # Step 3: Deduplicate addresses
    df = deduplicate_addresses(df, input_col, intercomponents = True, intracomponents = False, tolerance = 0, create_flag=create_flag, overwrite=True)

    # Step 4: Deduplicate postcodes
    df = deduplicate_postcodes(df, input_col, create_flag=create_flag, overwrite=True)

    # Step 5: Rectify postcodes
    df = rectify_postcodes(df, input_col, create_flag=create_flag, overwrite=True)

    # Step 6: Prettify Addresses
    df = prettify_addresses(df, input_col, overwrite = True)

    return df

#################################################################################

def validate_addresses(df: DataFrame, input_col: str, similarity_threshold: int = 95, scores: bool = False) -> DataFrame:
    """
    Validates address data by applying a series of validation functions including address component validation
    and postcode validation.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - similarity_threshold (int, optional): The threshold for similarity score when validating components. Defaults to 95.
    - scores (bool, optional): Whether to include similarity scores in the output. Defaults to False.

    Returns:
    - DataFrame: The validated DataFrame with associated validation flags.

    Notes:
    Address component validation will be run for towns, cities and counties by default.

    Postcode validation will be performed using UK postcode rules, correct formatting and without disallowed characters.
    Only one valid UK postcode is required to set the flag to 1.
    """
    # Step 1: Validate town components
    df = validate_from_list(df, input_col, component_name='town', component_list=town_list, similarity_threshold=similarity_threshold, scores = scores)

    # Step 2: Validate city components
    df = validate_from_list(df, input_col, component_name='city', component_list=city_list, similarity_threshold=similarity_threshold, scores = scores)

    # Step 3: Validate county components
    df = validate_from_list(df, input_col, component_name='county', component_list=county_list, similarity_threshold=similarity_threshold, scores = scores)

    # Step 4: Validate postcodes
    df = validate_postcodes(df, input_col)

    return df

#################################################################################

def extract_address_components(df: DataFrame, input_col: str, similarity_threshold: int = 95, scores: bool = False, replace: bool = True) -> DataFrame:
    """
    Extracts address components such as postcodes, towns, cities, and counties from the given address column.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - similarity_threshold (int, optional): The threshold for similarity score when extracting components. Defaults to 95.
    - scores (bool, optional): Whether to include similarity scores in the output. Defaults to False.
    - replace (bool, optional): Whether to replace the existing columns or create new ones. Defaults to True.

    Returns:
    - DataFrame: The DataFrame with extracted address components.

    Notes:
    Extraction will be performed for postcodes, towns, cities, and counties by default.

    """
    # Step 1: Extract postcodes
    df = extract_postcodes(df, input_col, replace=replace)

    # Step 2: Extract towns
    df = extract_components_from_list(df, input_col, component_name='town', component_list=town_list, similarity_threshold=similarity_threshold, scores = scores, replace=replace)

    # Step 3: Extract cities
    df = extract_components_from_list(df, input_col, component_name='city', component_list=city_list, similarity_threshold=similarity_threshold, scores = scores, replace=replace)

    # Step 4: Extract counties
    df = extract_components_from_list(df, input_col, component_name='county', component_list=county_list, similarity_threshold=similarity_threshold, scores = scores, replace=replace)

    return df

#################################################################################
