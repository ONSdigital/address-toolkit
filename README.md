# Address Toolkit (In Development)

![Python](https://img.shields.io/badge/Python-3.10-blue)
![License](https://img.shields.io/badge/License-MIT-orange)

## Introduction
The address toolkit package is a lightweight package and has been designed to assist with working with address data registered as a `pyspark.sql.DataFrame`.
The package includes functions for cleaning, validating and extracting addresses and address components.
Additionally, `workflows` have been created as an 'out-of-the-box' application combining the functions across `cleaning`, `validating` and `extracting`.

To install the package, run:
```
pip install address-toolkit
```

To register a spark dataframe from a CSV, the following code can be run.

```
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("test").getOrCreate()
df = spark.read.csv("test.csv")
```

A recommended first step in using this package is to apply the `clean_punctuation` function from `address_toolkit.cleaning`.

```
from address_toolkit.cleaning import clean_punctuation

df = clean_punctuation(df, 'address_column', create_flag = True, overwrite = True)
```

## Main Package Contents:

| Folder Name | Description | Includes |
| ------------- | ------------- | ------------- |
| `cleaning` | Contains functions to clean addresses | `clean_punctuation`, `denoise_addresses`, `deduplicate_addresses`, `deduplicate_postcodes`, `rectify_postcodes`, `standardise_street_types`, `prettify_addresses` |
| `validating` | Contains functions to validate addresses | `validate_from_list`, `validate_from_regex`, `validate_postcodes` |
| `extracting` | Contains functions to extract address components | `extract_from_list`, `extract_from_regex`, `extract_postcodes` |
| `contextualising` | Contains functions to contextualise addresses | `contextualise_from_component` |
| `workflows` | Contains functions to streamline processing | `clean_addresses`, `validate_addresses`, `extract_address_components` |

## Supplementary Package Contents (Resources):

| Resource | Includes |
| ------------- | ------------- |
| UK Postcode Regex | `postcode_regex` |
| Unit Address Level Regex | `flat_regex`, `room_regex`, `unit_regex`, `block_regex`, `apartment_regex`, `floor_regex` |
| Noise Regex | `consecutive_letters_regex` |
| Miscellaneous Regex | `misc_numbers_regex`, `standalone_numbers_regex`, `txt_before_numbers_regex`, `end_address_numbers_regex`, `start_address_numbers_regex`, `end_address_identifier_regex` |
| Address Component Lists | `town_list`, `city_list`, `village_list`, `hamlet_list`, `suburb_list`, `bay_list`, `place_list`, `district_list`, `county_list`, `allowed_country_list`, `disallowed_country_list` |
| Keyword Component Lists | `contextual_keywords`, `misc_keywords_list`, `misc_special_keywords` |
| Address Component Lookups | `town_lookup`, `village_lookup`, `bay_lookup`, `hamlet_lookup`, `suburb_lookup` |

Note:
For unit address level RegEx patterns, alternatives i.e. `flat_regex_alternative` are available which are less 'strict' in their matching.
Address Component lists and lookups have been created from Open Names dataset from Ordnance Survey Data Hub [https://osdatahub.os.uk/data/downloads/open/OpenNames]

## Example Usage
See `tutorial.ipynb` for full use of all the functions within `cleaning`, `validating`, `extracting`, `contextualising` and `workflows`.




