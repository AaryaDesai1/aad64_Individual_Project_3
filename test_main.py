import pytest
from mylib.extract import extract
from mylib.load import load
from mylib.query import execute_queries
from mylib.transform import transform_data
from mylib.visualize import visualize


# Test for the extract function
def test_extract():
    # Call the extract function
    file_path = extract()

    # Check if the file_path is not empty (indicating successful extraction)
    assert file_path is not None


# Test for the load function
def test_load():
    # Call the load function
    loaded_df = load()

    # Check if the loaded DataFrame is not empty
    assert not loaded_df.isEmpty()


# Test for the query execution
def test_execute_queries():
    # Call the execute_queries function
    query_results = execute_queries()

    # Check if the query_results dictionary is not empty
    assert query_results
