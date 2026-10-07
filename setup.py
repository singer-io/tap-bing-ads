#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-bing-ads',
    version="3.1.0",
    description=(
        'Singer.io tap for extracting data from Microsoft Advertising (Bing Ads). '
        'REST is the default/recommended transport; legacy SOAP remains available '
        'via api_type=soap until its 2027-01-31 retirement.'
    ),
    author='Stitch',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_bing_ads'],
    install_requires=[
        'arrow>=1.4.0',
        'requests>=2.34.2',
        'setuptools==83.0.0',
        'singer-python==6.8.0',
        'backoff==2.2.1',
    ],
    extras_require={
        'soap': [
            # Required only when a connection sets `api_type: soap`.
            # Supported until the Bing Ads SOAP API sunset (2027-01-31).
            'bingads==13.0.28',
        ],
        'test': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'parameterized>=0.9.0',
            'freezegun>=1.2.0',
            'responses>=0.23.0',
        ],
        'dev': [
            'ipdb',
        ]
    },
    entry_points='''
      [console_scripts]
      tap-bing-ads=tap_bing_ads:main
    ''',
    packages=find_packages(),
    package_data={
        'tap_bing_ads': ['schemas/*.json'],
    },
    include_package_data=True,
)

