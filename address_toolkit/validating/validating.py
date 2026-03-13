import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from rapidfuzz import fuzz

from address_toolkit.resources import town_list
from address_toolkit.utilities import check_valid_postcode

#################################################################################

def validate_from_list(df: DataFrame, input_col: str, component_name: str, component_list: list = town_list, similarity_threshold: int = 95, scores: bool = False,
                                search_space: int = 7) -> DataFrame:
    """
    Validates address components by checking presence against a comparator list using fuzzy matching.
    A range of comparator lists can be imported from address_cleaning.resources (e.g., town_list, village_list...).

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - component_name (str): The name of the address component being validated.
    - component_list (list): The list of valid names (e.g., towns) to compare against.
    - similarity_threshold (int, optional): The threshold above which a town name match is considered valid. Defaults to 95.
    - scores (bool, optional): Whether to include match scores in the output. Defaults to False.
    - search_space (int, optional): The range of character length difference allowed when comparing components. Defaults to 7.

    Returns:
    - DataFrame: The updated DataFrame with a new flag column indicating validated address components.

    Notes:
    - The caveat for this function is that it may not accurately identify valid comparator components if they are not clearly separated by commas.
      This is an intentional design choice to balance performance and accuracy.

    """
    component_dict_by_letter = {}
    for item in component_list:
        key = item[0]
        component_dict_by_letter.setdefault(key, set()).add(item)

    def _extract_address_component(address,
                                   similarity_threshold=similarity_threshold,
                                   component_dict_by_letter=component_dict_by_letter,
                                   search_space=search_space):
        """
        Extracts address component from a given address string using fuzzy matching.

        Parameters:
        - address (str): The address string to be processed.
        - similarity_threshold (int): The similarity threshold for matching.
        - component_dict_by_letter (dict): A dictionary categorizing components by their starting letter.
        - search_space (int): The range of character length difference allowed when comparing components.

        Returns:
        - tuple: A tuple containing the modified address string and the extracted component.

        """
        parts = [part.strip() for part in address.split(",")]

        best_part = ""
        best_score = 0

        for part in parts:

            try:
                min_length = max(len(part) - search_space, 0)
                max_length = len(part) + search_space

                potential_components_by_letter = component_dict_by_letter[part[0].upper()]
                potential_components = set([component for component in potential_components_by_letter if min_length <= len(component) <= max_length])

                if part.upper() in potential_components:
                    best_part = part
                    best_score = 100
                    return best_part, best_score

            except Exception as error:
                continue

            if similarity_threshold != 100:
                for comp in potential_components:
                    similarity_score = fuzz.ratio(part.upper(), comp)
                    if (similarity_score >= similarity_threshold) and (similarity_score > best_score):
                        best_score = similarity_score
                        best_part = part

        return best_part, int(best_score)

    score_colname = f"{component_name}_match_score"
    validated_component_name = f"validated_{component_name}_flag"
    strongest_match_udf = udf(_extract_address_component, StructType([
        StructField(validated_component_name, StringType(), True),
        StructField(score_colname, IntegerType(), True)
    ]))

    match_results = strongest_match_udf(df[input_col])
    df = df.withColumn(validated_component_name, match_results[validated_component_name])
    df = df.withColumn(validated_component_name, when(col(validated_component_name) != "", 1).otherwise(0))

    if scores:
        df = df.withColumn(score_colname, match_results[score_colname])

    return df

##################################################################################

def validate_postcodes(df: DataFrame, input_col: str) -> DataFrame:

    """
    Validates postcodes in the given address column using UK postcode rules, correct formatting and without disallowed characters.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.

    Returns:
    - DataFrame: The updated DataFrame with a new flag column indicating validated postcode.

    Notes:
    - Only one valid UK postcode is required to set the flag to 1.

    """
    valid_postcode_udf = udf(check_valid_postcode, IntegerType())
    df = df.withColumn("validated_postcode_flag", valid_postcode_udf(col(input_col)))

    return df

##################################################################################

def validate_from_regex(df: DataFrame, input_col: str, component_name: str, regex_pattern: str) -> DataFrame:
    """
    Validates address components by checking presence against a regex pattern.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - component_name (str): The name of the address component being validated.
    - regex_pattern (str): The regex pattern to validate against.

    Returns:
    - DataFrame: The updated DataFrame with a new flag column indicating validated address components.

    Notes:
    - Only one valid match is required to set the flag to 1.

    """
    def _validate_component_with_regex(address, regex_pattern=regex_pattern):
        matches = re.findall(regex_pattern, address)
        return 1 if matches else 0

    validate_component_udf = udf(_validate_component_with_regex, IntegerType())
    validated_component_name = f"validated_{component_name}_flag"
    df = df.withColumn(validated_component_name, validate_component_udf(col(input_col)))

    return df