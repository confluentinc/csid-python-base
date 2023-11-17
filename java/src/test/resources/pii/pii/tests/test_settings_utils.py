from unittest import TestCase

from pii.settings_utils import string_to_list, list_or_string_to_list

class Test(TestCase):
    def test_string_to_list(self):
        self.assertEqual(string_to_list("aa, bb, cc"), ["aa", "bb", "cc"])
        self.assertEqual(string_to_list(""), [""])
        self.assertEqual(string_to_list("aa"), ["aa"])
        self.assertEqual(string_to_list("aa/bb"), ["aa/bb"])

    def test_list_or_string_to_list(self):
        self.assertEqual(list_or_string_to_list("aa, bb, cc"), ["aa", "bb", "cc"])
        self.assertEqual(list_or_string_to_list(""), [""])
        self.assertEqual(list_or_string_to_list("aa"), ["aa"])
        self.assertEqual(list_or_string_to_list(["aa", "bb", "cc"]), ["aa", "bb", "cc"])
        self.assertEqual(list_or_string_to_list([]), [])
