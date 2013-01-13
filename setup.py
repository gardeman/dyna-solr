#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name             = 'dyna_solr',
    version          = '0.0.2',
    packages         = find_packages(exclude=['_*']),
    install_requires = [
        'pysolr==2.1.0-beta',
        'python-dateutil==2.1'
    ],
)
