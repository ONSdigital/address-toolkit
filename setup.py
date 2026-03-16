from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name = 'address_toolkit',
    version = '1.0.0',
    author = 'Dan Harris, Ben Moscrop, Stephen Rowlands',
    description = 'A toolkit for cleaning, validating, extracting and contextualising GB Address Data.',
    packages = find_packages(exclude = ["tests"]),
    requirements = requirements,
    python_requires = '3.10',
    url = "https://github.com/ONSdigital/address-toolkit")

