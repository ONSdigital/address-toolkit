import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, regexp_extract, split, udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from rapidfuzz import fuzz

from address_toolkit.cleaning.cleaning import clean_punctuation
from address_toolkit.resources import postcode_regex
from address_toolkit.utilities import ensure_postcode_format

#################################################################


def extract_postcodes(df: DataFrame, input_col: str, replace: bool = False) -> DataFrame:
    """
    Extracts UK valid postcodes from a given address column.
    The UK postcodes will be cleaned and formatted correctly.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The name of the column containing the address strings to be processed
    - replace (bool): Whether to remove the extracted postcodes from the address column (default is False).

    Returns:
    - DataFrame: The updated DataFrame with extracted postcodes in a new 'postcode' column.

    """

    def _extract_postcode(address, replace=replace):
        """
        Extracts postcode from a given address string, replacing it in the address if specified.

        Parameters:
        - address (str): The address string to be processed.
        - replace (bool): Whether to remove the extracted postcode from the address string.

        Returns:
        - tuple: A tuple containing the modified address string and the extracted postcode(s) as a comma-separated string.

        """
        processed_address = re.sub(r"[^\w\s,]", " ", address)
        postcode_spans = [match.span() for match in re.finditer(postcode_regex, processed_address)]

        postcodes = [processed_address[start:end] for start, end in postcode_spans]
        postcodes_formatted = [ensure_postcode_format(pc.replace(" ", "").upper()) for pc in postcodes]

        if replace:
            address = "".join(
                address[end:start_next]
                for (end, start_next) in zip(
                    [0] + [e for _, e in postcode_spans],
                    [s for s, _ in postcode_spans] + [len(address)],
                )
            )

        return address, ",".join(list(set(postcodes_formatted)))

    extract_postcode_udf = udf(_extract_postcode, StructType(
            [StructField("address", StringType(), True),
            StructField("postcode", StringType(), True)]))

    postcode_results = extract_postcode_udf(df[input_col])
    df = df.withColumn("postcode", postcode_results["postcode"])
    df = df.withColumn(input_col, postcode_results["address"])

    df = clean_punctuation(df, input_col, create_flag=False, overwrite=True)

    return df


#################################################################


def extract_components_from_list(df: DataFrame, input_col: str, component_name: str, component_list: list,
                                 similarity_threshold: int = 95, search_space: int = 7, scores: bool = False, replace: bool = False) -> DataFrame:
    """
    Extracts components (e.g., town names) from an address column based on fuzzy matching against a provided component list.
    The extracted component is added as a new column in the DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The name of the column containing the address strings to be processed.
    - component_name (str): The name of the new column to store the extracted component.
    - component_list (list): The list of valid names (e.g., towns) to compare against.
    - similarity_threshold (int, optional): The threshold above which a town name match is considered valid. Defaults to 95.
    - search_space (int, optional): The range of length difference allowed between the address component and the comparator components. Defaults to 7.
    - scores (bool, optional): Whether to include similarity scores in the output. Defaults to False.
    - replace (bool): Whether to remove the extracted component from the address column (default is False).

    Returns:
    - DataFrame: The updated DataFrame with the extracted component in a new column.

    Notes:
    The caveat for this function is that it may not accurately identify valid comparator components if they are not clearly separated by commas.
    This is an intentional design choise to balance performance and accuracy.

    """
    component_dict_by_letter = {}
    for item in component_list:
        key = item[0]
        component_dict_by_letter.setdefault(key, set()).add(item)

    def _extract_address_component(address, similarity_threshold=similarity_threshold, component_dict_by_letter=component_dict_by_letter,
                                   search_space=search_space):
        """
        Extracts address component from a given address string using fuzzy matching.

        Parameters:
        - address (str): The address string to be processed.
        - similarity_threshold (int): The similarity threshold for matching.
        - component_dict_by_letter (dict): A dictionary categorizing components by their starting letter.

        Returns:
        - tuple: A tuple containing the modified address string and the extracted component.

        """
        parts = [part.strip() for part in address.split(",")]

        best_part = ""
        best_comp = ""
        best_score = 0

        for part in parts:

            try:
                min_length = max(len(part) - search_space, 0)
                max_length = len(part) + search_space

                potential_components_by_letter = component_dict_by_letter[part[0].upper()]
                potential_components = set([component for component in potential_components_by_letter if min_length <= len(component) <= max_length])

                if part.upper() in potential_components:
                    best_part = part
                    best_comp = part.upper()
                    best_score = 100
                    new_parts = ",".join([p for p in parts if p != best_part])
                    return new_parts, best_comp, best_score

            except Exception as error:
                continue

            if similarity_threshold != 100:
                for comp in potential_components:
                    similarity_score = fuzz.ratio(part.upper(), comp)
                    if (similarity_score >= similarity_threshold) and (similarity_score > best_score):
                        best_score = similarity_score
                        best_part = part
                        best_comp = comp

        new_parts = ",".join([p for p in parts if p != best_part])

        return new_parts, best_comp, int(best_score)

    score_colname = f"{component_name}_match_score"
    strongest_match_udf = udf(_extract_address_component,StructType(
            [StructField("remaining_address", StringType(), True),
            StructField(component_name, StringType(), True),
            StructField(score_colname, IntegerType(), True)]))

    match_results = strongest_match_udf(df[input_col])
    df = df.withColumn(component_name, match_results[component_name])

    if scores:
        df = df.withColumn(score_colname, match_results[score_colname])

    if replace:
        df = df.withColumn(input_col, match_results["remaining_address"])

    return df


#################################################################


def extract_components_from_regex(df: DataFrame, input_col: str, component_name: str, regex_pattern: str, replace: bool = False) -> DataFrame:
    """
    Extracts address components from a given address column based on a provided regex pattern.
    The extracted component is added as a new column in the DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The name of the column containing the address strings to be processed.
    - component_name (str): The name of the new column to store the extracted component.
    - regex_pattern (str): The regex pattern used to identify the component.
    - replace (bool): Whether to remove the extracted component from the address column (default is False).

    Returns:
    - DataFrame: The updated DataFrame with the extracted component in a new column.

    Notes:
        The regex pattern should be designed to capture the desired component, with the component expected to be in the first capturing group.

    """
    df = df.withColumn("extracted_regex", regexp_extract(col(input_col), regex_pattern, 0))
    df = df.withColumn(component_name, split(col("extracted_regex"), ",").getItem(0))
    df = df.drop("extracted_regex")

    if replace:
        expression = f"regexp_replace({input_col}, {component_name}, '')"
        df = df.withColumn(input_col, expr(expression))
        df = clean_punctuation(df, input_col, create_flag=False, overwrite=True)

    return df
