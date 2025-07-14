import pandas as pd
import argparse
import gc
from typing import Generator
import sqlite3
import datetime
import numpy as np

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
    
    incident_columns = ['unique_key', 'agency', 'complaint_type',
                        'descriptor', 'status', 'created_date', 
                        'closed_date', 'location_type'] 
                        # rename 'unique_key' to 'incident_id' and 'status' to 'incident_status'
    
    locations_columns = ['city', 'incident_zip', 
                        'borough'] 
                        # rename 'incident_zip' to 'zipcode'
                        # rename 'location_id' to 'id'

    # Add this column to DataFrame for joins:
    # 'location_id': 'incident_zip' + 'borough'

    for data_chunk in dataset_generator:
        incident_df = data_chunk[incident_columns]
        incident_df = incident_df.rename(columns={'unique_key': 'incident_id', 'status': 'incident_status'})
        incident_df = incident_df.astype({'incident_id': str, 'agency': str, 'complaint_type': str,
                                          'descriptor': str, 'incident_status': str, 'location_type': str})
        date_cols = ['created_date', 'closed_date']
        incident_df[date_cols] = incident_df[date_cols].apply(pd.to_datetime, errors='coerce', format='%Y-%m-%d')

        incident_df['location_id'] = data_chunk['incident_zip'].astype(str) + '_' + data_chunk['borough'].astype(str)

        locations_df = data_chunk[locations_columns]
        locations_df = locations_df.rename(columns={'incident_zip': 'zipcode'})
        locations_df = locations_df.astype({'city': str, 'zipcode': str,
                                          'borough': str})
        locations_df['id'] = data_chunk['incident_zip'].astype(str) + '_' + data_chunk['borough'].astype(str)
        
        try:
            conn = sqlite3.connect('nyc311.db') # Connect to or create the database file
            with open('../.sql_files/schema.sql', 'r') as sql_file: # Open and read the SQL file
                sql_script = sql_file.read()
            cur = conn.cursor()
            cur.executescript(sql_script) # Execute the SQL script
            try:
                incident_df.to_sql('incident', conn, if_exists='append', index=False) # populate incident table
                locations_df.to_sql('locations', conn, if_exists='append', index=False) # populate locations table
            except sqlite3.IntegrityError as e:
                print(f"Integrity Error: {e}")
                print("Duplicate rows were not inserted due to unique constraint.")
            print("Database and tables created successfully!")
            conn.commit()
            conn.close()

        except sqlite3.Error as e:
            print(e)

        print(incident_df)
        print(locations_df)

        gc.collect()
    

def main():
    gen = read_large_file('https://data.cityofnewyork.us/resource/erm2-nwe9.csv', 5, 5)
    dataset_into_sql(gen)        

    gc.collect()

if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description="Download a Kaggle dataset and analyze it using SQLite and Pandas.")
    #parser.add_argument("kaggle_path", help="Path to file on Kaggle's website (username + '/' + dataset name)")
    #parser.add_argument("dataset_name", help="Name of locally-downloaded dataset to analyze within directory located at `kaggle_path`")
    
    #args = parser.parse_args()
    #main(args)
    main()
