import unittest
from tap_bing_ads import snakecase, pascalcase

class TestStringCase(unittest.TestCase):

    def test_snake_case(self):
        self.assertEqual("ad_group_performance_report", snakecase("AdGroupPerformanceReport"))
        self.assertEqual("clicks", snakecase("Clicks"))
        self.assertEqual("account_id", snakecase("accountId"))
        self.assertEqual("", snakecase(""))
        self.assertEqual("none", snakecase(None))

    def test_pascal_case(self):
        self.assertEqual("AdGroupPerformanceReport", pascalcase("ad_group_performance_report"))
        self.assertEqual("Clicks", pascalcase("clicks"))
        self.assertEqual("AccountId", pascalcase("account_id"))
        self.assertEqual("", pascalcase(""))
        self.assertEqual("None", pascalcase(None))
