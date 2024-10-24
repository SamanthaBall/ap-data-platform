import apache_beam as beam
import pytest
from apache_beam.testing import test_pipeline
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
    # Mocking fuzzy_process.extractOne
    with patch('utils.enrichment.geocode.fuzzy_process.extractOne') as mock_extract:
        # Setting up mock return values
        mock_extract.side_effect = [
            ('United States', 90),  # Match for New York
            ('Canada', 95),         # Match for Toronto
            (None, 0)               # No match for NonExistentPlace
        ]
        
        # Creating a Beam pipeline to test the DoFn
        with test_pipeline.TestPipeline() as p:
            input_collection = p | beam.Create(setup_data)
            output_collection = input_collection | beam.ParDo(FuzzyMatchLocation())

            # Collect output
            output_collection | beam.Map(lambda x: print(x))  # Optionally print output for inspection

            # Assert the expected output
            expected_output = [
                {'location': 'New York', 'country': 'United States', 'matched': True},
                {'location': 'Toronto', 'country': 'Canada', 'matched': True},
                {'location': 'NonExistentPlace', 'country': None, 'matched': False}
            ]
            assert output_collection | beam.combiners.ToList() == expected_output

