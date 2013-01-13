#!/usr/bin/env python
from setuptools import setup, find_packages

name = 'dyna_solr'

setup(
    name             = name,
    version          = __import__(name).__version__,
    packages         = find_packages(exclude=['_*']),
    install_requires = [
        'pysolr==2.1.0-beta',
        'python-dateutil==2.1'
    ],
)
