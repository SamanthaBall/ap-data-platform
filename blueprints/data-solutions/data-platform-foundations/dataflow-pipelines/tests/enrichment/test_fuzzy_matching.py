import apache_beam as beam
import pytest
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that, equal_to
from unittest.mock import patch
from utils.enrichment.geocode import FuzzyMatchLocation 

# Sample COUNTRY_NAMES for testing
COUNTRY_NAMES = ['United States', 'Canada', 'Mexico']

@pytest.fixture
def setup_data():
    return [
        {'location': 'New York'},
        {'location': 'Toronto'},
        {'location': 'NonExistentPlace'}
    ]

def test_fuzzy_match_location(setup_data):

    with patch('utils.enrichment.geocode.fuzzy_process.extractOne') as mock_extract:

        # Setting up mock return values
        mock_extract.side_effect = [
            ('United States', 90),  # Match for New York
            ('Canada', 95),         # Match for Toronto
            (None, 0)               # No match for NonExistentPlace
        ]
        
        with test_pipeline.TestPipeline() as p:
            input_data = p | beam.Create(setup_data)
            output = input_data | beam.ParDo(FuzzyMatchLocation()) | beam.Map(lambda x: {**x}) 

            # Assert the expected output
            expected_output = [
                {'location': 'New York', 'country': 'United States', 'matched': True},
                {'location': 'Toronto', 'country': 'Canada', 'matched': True},
                {'location': 'NonExistentPlace', 'country': None, 'matched': False}
            ]

            assert_that(output, equal_to(expected_output))

