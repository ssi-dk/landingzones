#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup configuration for Landing Zone"""

import os
from setuptools import setup, find_packages

# Read version from __init__.py
here = os.path.abspath(os.path.dirname(__file__))
about = {}
with open(os.path.join(here, 'src', 'landingzones', '__init__.py')) as f:
    exec(f.read(), about)

# Read long description from README
with open(os.path.join(here, 'README.md'), 'r') as f:
    long_description = f.read()

# Requirements
install_requires = [
    'pandas>=1.0.0',
    'pyyaml>=5.0.0',  # For config.yaml support
]

tests_require = [
    'pytest>=7.0.0',
    'pytest-cov>=4.0.0',
]

setup(
    name='landingzones',
    version=about['__version__'],
    description=about['__description__'],
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=about['__author__'],
    url='https://github.com/ssi-dk/landingzones',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    include_package_data=True,
    package_data={
        'landingzones': ['config/*.tsv', 'config/*.yaml.example'],
    },
    python_requires='>=3.8',
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
        'dev': tests_require + [
            'flake8',
            'black',
        ],
    },
    entry_points={
        'console_scripts': [
            'lz-generate-cron=landingzones.generate_cron_files:main',
            'lz-check-deployment=landingzones.check_deployment_readiness:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Systems Administration',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    keywords='data-transfer rsync cron automation',
)
