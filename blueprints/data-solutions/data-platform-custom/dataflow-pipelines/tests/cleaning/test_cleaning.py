import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from utils.cleaning.clean import DropIrrelevantFields


@pytest.fixture
def test_data():
    # Sample test data
    return [
        {'id': 1, 'name': 'Alice', 'age': 25, 'unnecessary_field': 100},
        {'id': 2, 'name': 'Bob', 'age': 30, 'unnecessary_field': 200},
        {'id': 3, 'name': 'Charlie', 'age': 35, 'unnecessary_field': 300},
    ]


def test_drop_irrelevant_fields(test_data):
    fields_to_drop = ['unnecessary_field']

    # Expected output after dropping fields
    expected_data = [
        {'id': 1, 'name': 'Alice', 'age': 25},
        {'id': 2, 'name': 'Bob', 'age': 30},
        {'id': 3, 'name': 'Charlie', 'age': 35},
    ]

    # Create a TestPipeline
    with TestPipeline() as p:
        input_data = p | beam.Create(test_data)

        # Apply the DropIrrelevantFields DoFn
        output_data = input_data | beam.ParDo(DropIrrelevantFields(fields_to_drop))

        # Assert that the output is as expected
        assert_that(output_data, equal_to(expected_data))



