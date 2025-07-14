import pandas as pd
import argparse
import gc
from typing import Generator
import sqlite3
import os

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
    date_cols = ['created_date', 'closed_date']
    cols_needed = ['unique_key', 'agency', 'complaint_type',
                        'descriptor', 'status', 'created_date', 
                        'closed_date', 'location_type', 'city', 
                        'incident_zip', 'borough']
    dtype_dict = {'unique_key': str, 'agency': str, 'complaint_type': str,
                  'descriptor': str, 'incident_status': str, 'location_type': str,
                  'city': str, 'incident_zip': str, 'borough': str}
    print("Starting CSV read...", flush=True)
    for chunk in pd.read_csv(path_with_limit, chunksize=chunk_size, 
                                          dtype=dtype_dict, usecols=cols_needed, parse_dates=date_cols):
        yield chunk    

def dataset_into_sql(dataset_generator: Generator):
    """
    Defines SQL tables and loads the pandas DataFrames
    into them.
    
    Args:
        dataset_generator (Generator): The dataset parsed from the given URL.
    """
    
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
        incident_df['location_id'] = data_chunk['incident_zip'].astype(str) + '_' + data_chunk['borough'].astype(str)
        incident_df.dropna(subset=['incident_id'], inplace=True)
        incident_df.drop_duplicates(subset=['incident_id'], inplace=True)


        locations_df = data_chunk[locations_columns]
        locations_df = locations_df.rename(columns={'incident_zip': 'zipcode'})
        locations_df['id'] = data_chunk['incident_zip'].astype(str) + '_' + data_chunk['borough'].astype(str)
        locations_df.dropna(subset=['zipcode', 'borough'], inplace=True)
        locations_df.drop_duplicates(subset=['id'], inplace=True)
                
        try:
            conn = sqlite3.connect('nyc311.db') # Connect to or create the database file
            with open('../.sql_files/schema.sql', 'r') as sql_file: # Open and read the SQL file
                sql_script = sql_file.read()
            cur = conn.cursor()
            cur.executescript(sql_script) # Execute the SQL script
            try:
                incident_df.to_sql('incident', conn, if_exists='append', index=False) # populate incident table
                conn.commit()
            except sqlite3.IntegrityError as e:
                print("Duplicate rows have been detected in the `incident` table.")
            try:
                locations_df.to_sql('locations', conn, if_exists='append', index=False) # populate locations table
                conn.commit()
            except sqlite3.IntegrityError as e:
                print("Duplicate rows have been detected in the `locations` table.")
            conn.commit()
            conn.close()

        except sqlite3.Error as e:
            print(e)

        gc.collect()
    

def main():

    gen = read_large_file('https://data.cityofnewyork.us/resource/erm2-nwe9.csv', 50, 10)
    dataset_into_sql(gen)        
    
    connection = sqlite3.connect("nyc311.db")
    
    # Answer 3 interesting questions about the dataset

    # [SQL] 1. We typically think of NYC as a single city -- how many distinct 'city' values are there?
    # Could this column be misleading folks -- city employees and researchers alike -- when
    # something like 'neighborhood' might be a better fit? Let's see based on how many values are
    # returned.
    
    crsr_cities = connection.cursor()

    how_many_cities = """SELECT 
                        COUNT(DISTINCT city) AS unique_cities_in_nyc
                        FROM locations;"""

    crsr_cities.execute(how_many_cities)

    ans_city = crsr_cities.fetchall()

    output_cities = '../visualizations/unique_cities_in_nyc.txt'
    with open(output_cities, 'w') as f:
        # Write header (optional)
        f.write(f"Number of unique cities: {ans_city}")
        
    print(f"Number of unique cities in NYC has been written to {output_cities}")  
    
    # [SQL] 2. Which borough typically has the quickest time of resolution for a ticket?
    # I would assume this would be Manhattan since it's the most storied of the boroughs,
    # and it's also the smallest. I would expect Queens and Brooklyn, two large boroughs
    # that are less densely populated, to have less resources committed to resolving these issues. 

    crsr_resolution = connection.cursor()

    how_quick_resolution = """SELECT l.borough, AVG(JULIANDAY(i.closed_date) - JULIANDAY(i.created_date)) AS resolution_time_in_days
                              FROM incident i
                              JOIN locations l ON i.location_id = l.id
                              WHERE i.closed_date IS NOT NULL AND i.created_date IS NOT NULL
                              GROUP BY l.borough
                              ORDER BY resolution_time_in_days DESC;"""

    crsr_resolution.execute(how_quick_resolution)

    ans_res = crsr_resolution.fetchall()

    output_resolution = '../visualizations/resolution_time_by_borough.txt'
    with open(output_resolution, 'w') as f:
        # Write header (optional)
        f.write(f"Number of unique cities: {ans_res}") 

    print(f"Average time to resolution by borough has been written to {output_resolution}")  

    # [Pandas] 3. What's the most common incident type in each borough?

    common_incident_df = pd.read_sql_query("SELECT * from incident", connection)



    connection.close()
    os.remove("nyc311.db")

    gc.collect()

if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description="Download a Kaggle dataset and analyze it using SQLite and Pandas.")
    #parser.add_argument("kaggle_path", help="Path to file on Kaggle's website (username + '/' + dataset name)")
    #parser.add_argument("dataset_name", help="Name of locally-downloaded dataset to analyze within directory located at `kaggle_path`")
    
    #args = parser.parse_args()
    #main(args)
    main()
