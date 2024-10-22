import pandas as pd
import pytest
from utils.cleaning.clean import drop_irrelevant_fields

def test_drop_irrelevant_fields():
    # Setup
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'unnecessary_field': [100, 200, 300]
    })
    
    fields_to_drop = ['unnecessary_field']
    
    # Execute
    cleaned_data = drop_irrelevant_fields(data, fields_to_drop)
    
    # Verify
    expected_data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    pd.testing.assert_frame_equal(cleaned_data, expected_data)



def test_drop_non_existent_fields():
    # Setup
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    fields_to_drop = ['unnecessary_field']  # Field does not exist
    
    # Execute
    cleaned_data = drop_irrelevant_fields(data, fields_to_drop)
    
    # Verify
    pd.testing.assert_frame_equal(cleaned_data, data)  # Should be unchanged



def test_performance_with_large_data():
    # Setup large dataset
    large_data = pd.DataFrame({
        'id': range(1, 1000000),
        'name': ['Name'] * 1000000,
        'unnecessary_field': [100] * 1000000
    })
    
    # Execute and measure time
    import time
    start_time = time.time()
    cleaned_data = drop_irrelevant_fields(large_data, ['unnecessary_field'])
    duration = time.time() - start_time
    
    # Verify performance (ensure it runs within a reasonable time)
    assert duration < 1.0  # Adjust this threshold based on your environment
