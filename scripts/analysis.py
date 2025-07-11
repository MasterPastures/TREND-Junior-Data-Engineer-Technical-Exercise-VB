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

def create_df(dp: str, fn: str) -> pd.DataFrame:
    """
    Constructs the DataFrame to be analyzed.
      
    Args:
        dp (str): Path to the locally-downloaded directory. 

                Example: `.../file_directory`

        fn (str): Name of the locally-downloaded file.

                Example: `file.xslx` or `file.csv`

    Returns:
        pd.DataFrame: The DataFrame representation of the file.
    """
    full_path = dp + os.sep + fn
    final_df = pd.DataFrame()

    if full_path.endswith('.csv'):
        final_df = pd.read_csv(full_path)
    elif full_path.endswith('.xlsx'):
        final_df = pd.read_excel(full_path)
    else:
        print("Your data's file format is not supported. Please convert it to either a csv or xlsx (Excel) file.")

    print(len(final_df))
    return final_df

def main(args):
    kaggle_path = args.kaggle_path
    dataset_name = args.dataset_name

    directory_path = find_path_to_directory(kaggle_path)
    create_df(directory_path, dataset_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download a Kaggle dataset and analyze it using SQLite and Pandas.")
    parser.add_argument("kaggle_path", help="Path to file on Kaggle's website (username + '/' + dataset name)")
    parser.add_argument("dataset_name", help="Name of locally-downloaded dataset to analyze within directory located at `kaggle_path`")
    
    args = parser.parse_args()
    main(args)