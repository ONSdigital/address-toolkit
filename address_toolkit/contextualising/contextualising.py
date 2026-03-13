import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, regexp_extract, split, udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from rapidfuzz import fuzz

from address_toolkit.cleaning.cleaning import clean_punctuation
from address_toolkit.resources import postcode_regex
from address_toolkit.utilities import ensure_postcode_format

#################################################################

def contextualise_from_lookup(df: DataFrame, input_col: str, component_name: str, component_lookup: dict,
                                 similarity_threshold: int = 95, search_space: int = 7, create_flag: bool = True, overwrite: bool = True):

    """
    Contextualises address components by checking presence against a comparator lookup using fuzzy matching and validating against the postcode district.
    Validation against postcode district is an intentional design choice to mitigate the risk of false positives when contextualising from comparator components.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - component_name (str): The name of the address component being contextualised.
    - component_lookup (dict): A dictionary where keys are component names and values are lists of associated address components including postcode district.
    - similarity_threshold (int, optional): The threshold above which a component match is considered valid. Defaults to 95.
    - search_space (int, optional): The range of character length difference allowed when comparing components. Defaults to 7.
    - create_flag (bool, optional): Whether to create a flag column indicating whether contextualisation occurred. Defaults to True.
    - overwrite (bool, optional): Whether to overwrite the input column with the contextualised address. Defaults to True.

    Returns:
    - DataFrame: The updated DataFrame with contextualised addresses and associated flags if create_flag is True."""

    components = list(component_lookup.keys())
    component_dict_by_letter = {}
    for item in components:
        key = item[0]
        component_dict_by_letter.setdefault(key, set()).add(item.upper())

    def _contextualise_address_component(address, component_lookup = component_lookup, component_dict_by_letter = component_dict_by_letter,
                                         similarity_threshold = similarity_threshold, search_space = search_space):
        """
        Helper function to contextualise address components by checking presence against a comparator lookup.

        Parameters:
        - address (str): The input address string to be contextualised.
        - component_lookup (dict): A dictionary where keys are component names and values are lists of associated address components including postcode district.
        - component_dict_by_letter (dict): A dictionary where keys are the first letter of components and values are sets of components starting with that letter.
        - similarity_threshold (int): The threshold above which a component match is considered valid.
        - search_space (int): The range of character length difference allowed when comparing components.

        Returns:
        - str: The contextualised address if a match is found, otherwise the original address.
        - int: A flag indicating whether contextualisation occurred (1) or not (0).
        """


        best_part = ""
        best_score = 0
        changes_flag = 0

        postcode = re.search(postcode_regex, address)
        postcode = postcode.group() if postcode else None

        if not postcode:
            return address, changes_flag

        parts = [part.strip() for part in address.split(",")]
        for part in parts:

            try:
                min_length = max(len(part) - search_space, 0)
                max_length = len(part) + search_space

                potential_components_by_letter = component_dict_by_letter[part[0].upper()]
                potential_components = set([component for component in potential_components_by_letter if min_length <= len(component) <= max_length])

                # Extract first part from postcode and validate against that from the component lookup
                if part.upper() in potential_components:
                    context = component_lookup[part.upper()]
                    if postcode.split(" ")[0] == context[-1].upper():
                        best_part = part
                        best_comp_part = part.upper()
                        best_score = 100

            except Exception as error:
                continue

            if similarity_threshold != 100:
                for comp in potential_components:
                    similarity_score = fuzz.ratio(part.upper(), comp)
                    if (similarity_score >= similarity_threshold) and (similarity_score > best_score):
                        context = component_lookup[comp]
                        if postcode.split(" ")[0] == context[-1].upper():
                            best_part = part
                            best_comp_part = comp
                            best_score = int(similarity_score)

        if best_part:
            context = component_lookup[best_comp_part]
            indexed_part = parts.index(best_part)
            contextualised_components = parts[:indexed_part] + context[:-1] + [postcode]
            address = ','.join(contextualised_components)
            changes_flag = 1

        return address, changes_flag

    contextualised_component_flag_colname = f"contextualised_from_{component_name}_flag"
    contextualised_address_colname = f"contextualised_from_{component_name}_address"
    strongest_match_udf = udf(_contextualise_address_component, StructType([
        StructField(contextualised_address_colname, StringType(), True),
        StructField(contextualised_component_flag_colname, IntegerType(), True)
    ]))

    match_results = strongest_match_udf(df[input_col])
    df = df.withColumn(contextualised_address_colname, match_results[contextualised_address_colname])
    df = clean_punctuation(df, contextualised_address_colname, create_flag = False, overwrite = True)


    if create_flag:
        df = df.withColumn(contextualised_component_flag_colname, match_results[contextualised_component_flag_colname])

    if overwrite:
        df = df.drop(input_col)
        df = df.withColumnRenamed(contextualised_address_colname, input_col)

    return df