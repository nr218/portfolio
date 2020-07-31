from unittest import TestCase
from data_analysis.utils.location_regex import get_state, get_country


class GetStateTest(TestCase):
    """Test the get_state function to check if it correctly returns the state"""

    def test_location_usa(self):
        """Check when the location is in USA the state is correctly returned"""
        location = "Boston, MA"
        self.assertEqual(str.strip(get_state(location)), "MA")

    def test_location_other(self):
        """Test when the location is outside USA empty string is returned"""
        self.assertEqual(get_state("Toronto, ON (Canada)"), "")
        self.assertEqual(get_state("Dubai (UAE)"), "")

    def test_location_none(self):
        """Test when empty location is passed empty string is returned"""
        self.assertEqual(get_state(""), "")


class GetCountry(TestCase):
    """Test the get_country function correctly returns the country from the location column"""

    def test_location_usa(self):
        """Test when no country in brackets is present, USA is returned"""
        location = "Boston, MA"
        self.assertEqual(get_country(location), "USA")

    def test_location_other(self):
        """Test when country in brackets is present, the country extracted from the brackets is returned"""
        self.assertEqual(get_country("Toronto, ON (Canada)"), "Canada")
        self.assertEqual(get_country("Dubai (UAE)"), "UAE")
