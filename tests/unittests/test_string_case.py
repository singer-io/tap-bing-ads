import unittest
import stringcase
from tap_bing_ads import snakecase, snakecase_to_pascalcase

class TestStringCase(unittest.TestCase):

    def test_snake_case(self):
        test_weird_string = "_weirdString this-is a we.ird st32134ng !"
        expected_weird_result = "_weird_string_this_is_a_we_ird_st32134ng_!"
        self.assertEqual(expected_weird_result, snakecase(test_weird_string))
        self.assertEqual(expected_weird_result, stringcase.snakecase(test_weird_string))

        test_camel_string = "iAmACamelCaseStringHello"
        expected_camel_result = "i_am_a_camel_case_string_hello"
        self.assertEqual(expected_camel_result, snakecase(test_camel_string))
        self.assertEqual(expected_camel_result, stringcase.snakecase(test_camel_string))

        self.assertEqual("", snakecase(""))
        self.assertEqual("none", snakecase(None))

    def test_pascal_case(self):
        self.assertEqual("SnakeString", snakecase_to_pascalcase("snake_string"))
        self.assertEqual("SnakeString", snakecase_to_pascalcase("_snake_string"))

        self.assertEqual("", snakecase_to_pascalcase(""))
        self.assertEqual("None", snakecase_to_pascalcase(None))
