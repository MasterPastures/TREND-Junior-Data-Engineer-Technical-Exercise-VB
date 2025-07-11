import os
import pandas as pd
import kagglehub
import argparse

def find_path_to_directory(kp: str) -> str:
    """
    Downloads the Kaggle dataset of interest
    and constructs the absolute path to its 
    parent directory on the user's local machine.
      
    Args:
        kp (str): Path to the dataset on Kaggle's
                    website. 

                 Example: `username/userdataset`

    Returns:
        str: The path to the directory containing
                the dataset.
    """
    directory_path =  kagglehub.dataset_download(kp)
    print("Path to dataset files:", directory_path)
    return directory_path

def create_df(fp: str) -> pd.DataFrame:
    """
    Constructs the data.
      
    Args:
        fp (str): Path to the locally-downloaded file. 

                 Example: `.../file.xslx` or `.../file.csv`

    Returns:
        pd.DataFrame: The DataFrame representation of the file.
    """
  

def main(args):
    kaggle_path = args.kaggle_path
    dataset_name = args.dataset_name

    find_path_to_directory(kaggle_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download a Kaggle dataset and analyze it using SQLite and Pandas.")
    parser.add_argument("kaggle_path", help="Path to file on Kaggle's website (username + '/' + dataset name)")
    parser.add_argument("dataset_name", help="Name of locally-downloaded dataset to analyze within directory located at `kaggle_path`")
    
    args = parser.parse_args()
    main()