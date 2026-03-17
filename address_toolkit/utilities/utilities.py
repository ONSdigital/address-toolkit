import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, when

from address_toolkit.resources import postcode_regex

###################################################################################
# GENERIC UTILITY FUNCTIONS
###################################################################################

def add_length_flag(df, input_col: str, min_length: int = 10, max_length: int = 150) -> DataFrame:
    """
    Adds a flag to an input dataframe indicating whether the length of the address in the specified column
    falls outside the defined minimum and maximum length boundaries.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The column name in df that contains the address information.
    - min_length (int): The minimum acceptable length for the address. Defaults to 10.
    - max_length (int): The maximum acceptable length for the address. Defaults to 150.

    Returns:
    - DataFrame: The updated DataFrame with an additional length_flag column.

    """
    df = df.withColumn("length_flag",
                       when((length(col(input_col)) < min_length) |
                            (length(col(input_col)) > max_length), 1).otherwise(0))
    return df

###################################################################################
# CLEAN PUNCTUATION UTILITY FUNCTIONS
###################################################################################

def clean_part(part):
    """
    Cleans a part of an address by removing unwanted punctuation while preserving meaningful characters.

    Parameters:
    - part (str): A segment of the address to be cleaned.

    Returns:
    - str: The cleaned address part.
    """
    if part:
        # Replace hyphen between room numbers with a comma (e.g., "Room 7 - 1"), account for varying spaces
        part = re.sub(r'(Room\s+\d+)\s*-\s*(\d+)', r'\1, \2', part)

        # Preserve hyphens between numbers, even if there are spaces around the hyphen (e.g., "14 - 16")
        part = re.sub(r'(?<=\d)\s*-\s*(?=\d)', ' TEMP_HYPHEN ', part)

        # Preserve periods (.) between numbers (e.g., "14.16")
        part = re.sub(r'(?<=\d)\s*\.\s*(?=\d)', ' TEMP_DOT ', part)

        # Preserve hyphens in cases like "II-2" or "Gp2-4-B-7"
        part = re.sub(r'(?<=\w)-(?=\w)', ' TEMP_HYPHEN ', part)
        part = re.sub(r'(?<=BLOCK\s\w)-(?=\d)', ' TEMP_HYPHEN ', part)  # Preserve hyphen in block names
        part = re.sub(r'(?<=\w)-(?=\d\w)', ' TEMP_HYPHEN ', part)  # Preserve hyphens like "C-11E"

        # Remove leading hyphens before numbers, except in preserved cases
        part = re.sub(r'-\s*(?=\d)', '', part)

        # Remove punctuation at the beginning and end
        part = re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "", part)

        # Normalise whitespace
        part = re.sub(r"\s+", " ", part)

        # Restore preserved hyphens and periods
        part = part.replace(' TEMP_HYPHEN ', '-')
        part = part.replace(' TEMP_DOT ', '.')

    return part.strip()

#################################################################################
# DEDUPLICATE ADDRESS UTILITY FUNCTIONS
#################################################################################

def deduplicate_intracomponents(intracomponents, tolerance):
    """
    Deduplicates similar parts within each component of an address using fuzzy matching.

    Parameters:
    - intracomponents (list of list of str): A list where each element is a list of parts within a component of the address.
    - tolerance (int): The minimum length a part must have to be considered for deduplication.

    Returns:
    - str: The deduplicated address as a single string.
    """

    deduplicated_address_string = ""

    for parts in intracomponents:
        for index, part in enumerate(parts):
            comparator_intraparts = [p for i, p in enumerate(parts) if i != index]
            for part_comp in comparator_intraparts:
                if (len(part) >= tolerance and len(part_comp) >= tolerance and part.upper()==part_comp.upper()):
                    parts.remove(part)
        processed_parts = " ".join(parts)

        if not deduplicated_address_string:
            deduplicated_address_string = processed_parts
        else:
            deduplicated_address_string = deduplicated_address_string + "," + processed_parts

    return deduplicated_address_string

def deduplicate_intercomponents(components, tolerance):
    """
    Deduplicates similar components of an address using fuzzy matching.

    Parameters:
    - components (list of str): A list of components of the address.
    - tolerance (int): The minimum length a component must have to be considered for deduplication.

    Returns:
    - str: The deduplicated address as a single string.
    """

    for index, part in enumerate(components):
        if bool(re.search(r'\d', part)):
            non_numeric_part = " ".join([c for c in part.split(' ') if not re.search(r'\d', c)])
            comparator_parts = [c for i, c in enumerate(components) if i != index]
            for comp_part in comparator_parts:
                if (len(non_numeric_part) >= tolerance and len(comp_part) >= tolerance and comp_part.upper()==non_numeric_part.upper()):
                    components.remove(comp_part)
        else:
            comparator_parts = [c for i, c in enumerate(components) if i != index]
            for comp_part in comparator_parts:
                if (len(part) >= tolerance and len(comp_part) >= tolerance and comp_part.upper()==part.upper()):
                    components.remove(part)

    # Introducing a final pass here for robustness as some cases weren't being detected by the above
    for index, part in enumerate(components):
        comparator_parts = [c for i, c in enumerate(components) if i != index]
        for comp_part in comparator_parts:
            if (len(part) >= tolerance and len(comp_part) >= tolerance and comp_part.upper()==part.upper()):
                components.remove(part)

    return ",".join(components)


#################################################################################
# DEDUPLICATE POSTCODES UTILITY FUNCTIONS
#################################################################################

def ensure_postcode_format(postcode):
    """
    Ensures that a UK postcode has the correct format with a space before the last three characters.

    Parameters:
    - postcode (str): The postcode to be formatted.

    Returns:
    - str: The formatted postcode.

    """
    if len(postcode) <= 3:
        return postcode
    else:
        return postcode[:-3] + " " + postcode[-3:]

def deduplicate_postcode(address):
    """
    Deduplicates postcodes found within an address string.

    Parameters:
    - address (str): The address string potentially containing multiple postcodes.

    Returns:
    - str: The address string with deduplicated postcodes.
    - int: A flag indicating whether duplicates were detected (1) or not (0).

    """
    processed_address = re.sub(r"[^\w\s,]", " ", address)
    postcode_spans = [match.span() for match in re.finditer(postcode_regex, processed_address)]

    postcodes = [processed_address[start:end] for start, end in postcode_spans]
    postcodes_formatted = [ensure_postcode_format(pc.replace(" ","").upper()) for pc in postcodes]

    duplicate_detected = 1 if len(postcodes_formatted) != len(set(postcodes_formatted)) else 0

    postcodes_finalised = ",".join(list(set(postcodes_formatted)))

    address = ''.join(
        address[end: start_next]
        for (end, start_next) in zip([0] + [e for _, e in postcode_spans], [s for s, _ in postcode_spans] + [len(address)])
    )

    address = address + "," + postcodes_finalised if postcodes_finalised else address

    return address.strip(), duplicate_detected

#################################################################################
# RECTIFY POSTCODES UTILITY FUNCTIONS
#################################################################################

def rectify_postcode(address):
    """
    Rectifies misread postcodes within an address which are separated by multiple spaces.

    Parameters:
    - address (str): The address string potentially containing misread postcodes.

    Returns:
    - str: The address string with rectified postcodes.
    - int: A flag indicating whether any changes were made (1) or not (0).

    """
    changes_flag = 0
    for component in address.split(","):
        cleaned_component = re.sub(r"[^\w\s]", " ", component)
        cleaned_component = cleaned_component.replace(" ", "")
        match = re.search(postcode_regex, cleaned_component)
        if match:
            # Including length filter reduces likelihood of matching to inappropriate address parts
            if match.group() == cleaned_component and len(cleaned_component) >= 5 and len(cleaned_component) <= 7:
                correct_postcode = ensure_postcode_format(cleaned_component.upper())
                if correct_postcode not in address:
                    address = address.replace(component, correct_postcode)
                    changes_flag = 1

    return address, changes_flag
#################################################################################
# VALIDATE POSTCODES UTILITY FUNCTIONS
#################################################################################

def check_valid_postcode(address):
    """
    Checks if there is at least one valid UK postcode in the given address string.

    Parameters:
    - address (str): The address string to be checked.

    Returns:
    - int: 1 if a valid postcode is found, otherwise 0.

    """
    # Ref: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/611951/Appendix_C_ILR_2017_to_2018_v1_Published_28April17.pdf
    disallowed_incode_characters = ["C", "I", "K", "M", "O", "V"]

    processed_address = re.sub(r"[^\w\s,]", " ", address)
    postcode_spans = [match.span() for match in re.finditer(postcode_regex, processed_address)]
    postcodes = [processed_address[start:end].replace(" ", "") for start, end in postcode_spans]

    is_valid_postcode = 0

    for postcode in postcodes:
        if ("ZZ99" not in postcode.upper()) and not any(char in postcode.upper()[-3:] for char in disallowed_incode_characters) and len(postcode) >= 5 and len(postcode) <= 7:
            is_valid_postcode = 1

    return is_valid_postcode
