#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-bing-ads',
    version="0.3.0",
    description='Singer.io tap for extracting data from the Bing Ads API',
    author='Stitch',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_bingads'],
    install_requires=[
        'arrow==0.12.0',
        'bingads==11.5.6',
        'requests==2.18.4',
        'singer-python==5.0.4',
        'stringcase==1.2.0'
    ],
    entry_points='''
      [console_scripts]
      tap-bing-ads=tap_bing_ads:main
    ''',
    packages=find_packages()
)
