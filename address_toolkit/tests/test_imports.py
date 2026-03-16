def test_address_functions_cleaning_imports():

    try:
        from address_toolkit.cleaning import (
            clean_punctuation,
            deduplicate_addresses,
            deduplicate_postcodes,
            denoise_addresses,
            rectify_postcodes,
            standardise_street_types,
        )
        success = 1

    except Exception as error:
        success = 0

    assert success == 1

def test_address_validation_import():

    try:
        from address_toolkit.validating import (
            validate_from_list,
            validate_from_regex,
            validate_postcodes,
        )
        success = 1
    except Exception as error:
        success = 0

    assert success == 1

def test_address_extracting_import():

    try:
        from address_toolkit.extracting import (
            extract_components_from_list,
            extract_components_from_regex,
            extract_postcodes,
        )
        success = 1
    except Exception as error:
        success = 0

    assert success == 1

def test_address_contextualising_import():
    try:
        from address_toolkit.contextualising import contextualise_from_lookup
        success = 1
    except Exception as error:
        success = 0

    assert success == 1

def test_workflows_import():

    try:
        from address_toolkit.workflows import (
            clean_addresses,
            extract_address_components,
            validate_addresses,
        )
        success = 1
    except Exception as error:
        success = 0

    assert success == 1

def test_resource_list_imports():

    try:
        from address_toolkit.resources import (
            town_list,
            city_list,
            bay_list,
            place_list,
            hamlet_list,
            suburb_list,
            district_list,
            county_list,
            village_list
        )

        success = 1
    except Exception as error:
        success = 0
    
    assert success == 1

def test_resource_lookup_imports():

    try:
        from address_toolkit.resources import (
            town_lookup,
            bay_lookup,
            village_lookup,
            suburb_lookup,
            hamlet_lookup
        )
        success = 1
    except Exception as error:
        success = 0
    
    assert success == 1