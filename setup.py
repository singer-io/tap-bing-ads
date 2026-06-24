#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-bing-ads',
    version="2.3.4",
    description='Singer.io tap for extracting data from the Bing Ads API',
    author='Stitch',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_bingads'],
    install_requires=[
        'arrow==0.17.0',
        # Seems that suds-community is now the reference for 13.0.11.1 so we can install it now with the removal of use_2to3
        # https://github.com/BingAds/BingAds-Python-SDK/pull/192
        'bingads==13.0.11.1',
        # bingads 13.0.11.1 imports pkg_resources at module import time.
        # pkg_resources was removed in setuptools 82, so keep setuptools below 82.
        'setuptools<82',
        'requests==2.33.0',
        'singer-python==6.0.1',
        'backoff==2.2.1',
    ],
    extras_require={
        'test': [
            'pylint==3.0.3'
        ],
        'dev': [
            'ipdb'
        ]
    },
    entry_points='''
      [console_scripts]
      tap-bing-ads=tap_bing_ads:main
    ''',
    packages=find_packages()
)
