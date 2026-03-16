# Maintenance Notes

## Package Release Versions
When a new package release is required, developers/maintainers will need to create a new release with GitHub.
A GitHub actions workflow has been configured such that when a new release is published, this will trigger the workflows to publish the package to PyPI.

## Python Release Version
The most stable Python release from testing in version 3.10. 
GitHub workflows have been put together such that any changes pushed to the `main` branch will perform the tests detailed in `address_toolkit/tests`.
This will also test the packaging to ensure compatibility.

## Security (Dependabot)
Dependabot has been configured in GitHub actions to provide recommendations and resolve issues on security issues. Some may not be able to be implemented due to 
causing compatibility issues with the python version and package dependencies. Such cases may require manual resolution.

## Package Development
If looking to extend the functionality provided by the package, ensure tests are written within `address_toolkit/tests`.
Developers and maintainers will also need to ensure the latest guidance and correct processing of postcodes is being followed.
The latest release on this guidance is [Annex C - Valid post code format](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/611951/Appendix_C_ILR_2017_to_2018_v1_Published_28April17.pdf)

## Data Sources
The [Ordnance Survey Open Names](https://osdatahub.os.uk/data/downloads/open/OpenNames) dataset is released on a quarterly basis in January, April, July and October.
The resource lists and lookups will need to be updated within `address_toolkit.resources` to ensure latest releases are being used in the package.
A Jupyter Notebook workflow has been created in `package_maintenance/` which will process the OS Open Names release. Notepad++ will also be required as part of this maintenance.
The notebook is not adaptable to schema changes, therefore some manual review and maintenance per release may be required.

The resources currently being derived from Ordnance Survey Open Names are:
| Resource Type | Variables |
| --- | --- |
| Lists | `town_list`, `city_list`, `village_list`, `place_list`, `district_list`, `bay_list`, `suburb_list`, `county_list`, `hamlet_list` |
| Lookups | `town_lookup`, `village_lookup`, `bay_lookup`, `suburb_lookup`, `hamlet_lookup` |
