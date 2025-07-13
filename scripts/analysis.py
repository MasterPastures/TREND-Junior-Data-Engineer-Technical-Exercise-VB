import pandas as pd
import argparse
import gc
from typing import Generator
import sqlite3

def read_large_file(file_path: str, limit: int, chunk_size: int) -> Generator:
    """
    Reads a large file, preferably in csv format,
    in chunks using a generator.
      
    Args:
        file_path (str): Path to the dataset on the website. 
        
        Example: `https://data.org/specific-dataset`

        limit (int): Number of rows to be read from the dataset.

        chunk_size (int): The size, in rows, of the chunks.

    Yields:
        str: The path to the directory containing
                the dataset.
    """
    path_with_limit =  file_path + '?$limit=' + str(limit)
    for chunk in pd.read_csv(path_with_limit, chunksize=chunk_size):
        yield chunk

def dataset_into_sql(dataset_generator: Generator) -> sqlite3:
    """
    Defines SQL tables and loads the pandas DataFrames
    into them.
    
    Args:
        dataset_generator (Generator): The dataset parsed from the given URL.
        
    Yields:
        sqlite3: The sqlite3 table representation of the dataset."""
    
    


def main():
    total_size = 0
    for data_chunk in read_large_file('https://data.cityofnewyork.us/resource/erm2-nwe9.csv', 50000000, 10000):
        print(data_chunk.head())

    gc.collect()

if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description="Download a Kaggle dataset and analyze it using SQLite and Pandas.")
    #parser.add_argument("kaggle_path", help="Path to file on Kaggle's website (username + '/' + dataset name)")
    #parser.add_argument("dataset_name", help="Name of locally-downloaded dataset to analyze within directory located at `kaggle_path`")
    
    #args = parser.parse_args()
    #main(args)
    main()

    """import sqlite3
import pandas as pd

db = sqlite3.connect("my_large_data.sqlite")
# Load your CSV into SQLite in chunks
for chunk in pd.read_csv("large_dataset.csv", chunksize=10000):
    chunk.to_sql("my_table", db, if_exists="append")
db.execute("CREATE INDEX my_index ON my_table(relevant_column)") # Create an index for faster queries
db.close()

# Later, query the database
conn = sqlite3.connect("my_large_data.sqlite")
df = pd.read_sql_query("SELECT * FROM my_table WHERE relevant_column = 'some_value'", conn)
conn.close()
"""

"""NOTES TO SELF:
SELECT A SUBSET OF COLUMNS ONCE THE DATA IS LOADED
COMMUNITY BOARD AND BOROUGH ARE DUPLICATIVE
CITY CAN GO IN AN INCIDENT-SPECIFIC TABLE AND BOROUGH CAN GO IN ANOTHER FOR JOINS
COLUMN B IS A PARCEL ID

"""