from mylib.extract import extract
from mylib.load import load
from mylib.transform import transform_data
from mylib.query import execute_queries
from mylib.visualize import visualize
import os


if __name__ == "__main__":
    current_directory = os.getcwd()
    print(current_directory)
    extract()
    df = load()
    execute_queries()
    transform_data(df)
    visualize(df)
