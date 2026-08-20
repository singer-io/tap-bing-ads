#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-bing-ads',
    version="3.0.0",
    description='Singer.io tap for extracting data from the Microsoft Advertising (Bing Ads) REST API',
    author='Stitch',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_bing_ads'],
    install_requires=[
        'arrow>=1.4.0',
        'requests>=2.34.2',
        'singer-python==6.8.0',
        'backoff==2.2.1',
    ],
    extras_require={
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

