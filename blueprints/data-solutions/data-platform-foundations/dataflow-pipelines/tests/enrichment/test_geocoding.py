import pytest
from unittest.mock import patch, MagicMock
from utils.enrichment.geocode import FuzzyMatchLocation, GeocodeLocation

# Sample data for testing
location = "New York"
non_matching_location = "NonExistentPlace"
matched_country = "United States"

@pytest.fixture
def setup_geocoder_and_fuzzy_matcher():
    # Create instances of the classes
    geocoder = GeocodeLocation()
    fuzzy_matcher = FuzzyMatchLocation()
    return geocoder, fuzzy_matcher


def test_fuzzy_match_found(setup_geocoder_and_fuzzy_matcher):
    geocoder, fuzzy_matcher = setup_geocoder_and_fuzzy_matcher
    # Test that a correct match is found by fuzzy matcher
    result = fuzzy_matcher.match_location(location)
    assert result == matched_country


@patch('utils.enrichment.geocode.geolocator.geocode')
def test_geocode_location_success(mock_geocode, setup_geocoder_and_fuzzy_matcher):
    geocoder, _ = setup_geocoder_and_fuzzy_matcher
    # Mock successful geocoding response
    mock_geocode.return_value = MagicMock(raw={'components': {'country': 'United States'}})
    
    result = geocoder.geocode_location(non_matching_location)
    assert result == 'United States'


@patch('utils.enrichment.geocode.geolocator.geocode')
def test_geocode_location_fail(mock_geocode, setup_geocoder_and_fuzzy_matcher):
    geocoder, _ = setup_geocoder_and_fuzzy_matcher
    # Mock geocode failure (None result)
    mock_geocode.return_value = None
    
    result = geocoder.geocode_location(non_matching_location)
    assert result is None


def test_fuzzy_and_geocode_combination(setup_geocoder_and_fuzzy_matcher):
    geocoder, fuzzy_matcher = setup_geocoder_and_fuzzy_matcher
    # Simulate fuzzy matching and geocoding together
    fuzzy_result = fuzzy_matcher.match_location(non_matching_location)
    if fuzzy_result is None:
        geocode_result = geocoder.geocode_location(non_matching_location)
        assert geocode_result == 'United States'
    else:
        assert fuzzy_result == 'United States'
