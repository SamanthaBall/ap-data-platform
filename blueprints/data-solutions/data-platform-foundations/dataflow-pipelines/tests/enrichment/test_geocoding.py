import unittest
from unittest.mock import patch, MagicMock
from utils.enrichment.geocode import FuzzyMatchLocation, GeocodeLocation


class TestLocationProcessing(unittest.TestCase):
    def setUp(self):
        # Setting up sample data
        self.location = "New York"
        self.non_matching_location = "NonExistentPlace"
        self.matched_country = "United States"
        
        # Create instances of the classes
        self.geocoder = GeocodeLocation()
        self.fuzzy_matcher = FuzzyMatchLocation()

    def test_fuzzy_match_found(self):
        # Test that a correct match is found by fuzzy matcher
        result = self.fuzzy_matcher.match_location(self.location)
        self.assertEqual(result, self.matched_country)
    
    @patch('mypackage.utils.geocoding.geolocator.geocode')
    def test_geocode_location_success(self, mock_geocode):
        # Mock successful geocoding response
        mock_geocode.return_value = MagicMock(raw={'components': {'country': 'United States'}})
        
        result = self.geocoder.geocode_location(self.non_matching_location)
        self.assertEqual(result, 'United States')

    @patch('mypackage.utils.geocoding.geolocator.geocode')
    def test_geocode_location_fail(self, mock_geocode):
        # Mock geocode failure (None result)
        mock_geocode.return_value = None
        
        result = self.geocoder.geocode_location(self.non_matching_location)
        self.assertIsNone(result)

    def test_fuzzy_and_geocode_combination(self):
        # Simulate fuzzy matching and geocoding together
        fuzzy_result = self.fuzzy_matcher.match_location(self.non_matching_location)
        if fuzzy_result is None:
            geocode_result = self.geocoder.geocode_location(self.non_matching_location)
            self.assertEqual(geocode_result, 'United States')
        else:
            self.assertEqual(fuzzy_result, 'United States')
