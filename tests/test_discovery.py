"""
Test tap discovery
"""
import unittest
#import re
#from tap_tester import menagerie, connections, runner

from tap_tester.base_suite_tests.discovery_test import DiscoveryTest
from base_new_framework import BingAdsBaseTest


class DiscoveryTest(DiscoveryTest,BingAdsBaseTest):
    """ Test the tap discovery """

    @staticmethod
    def name():
        return "tap_tester_bing_ads_discovery_test"
    def streams_to_test(self):
        return self.expected_stream_names()
