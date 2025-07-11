import os
import pandas as pd
import kagglehub

def find_path_to_directory(kaggle_path: str) -> str:
    """
    Download the Kaggle dataset of interest
    and construct the absolute path to its 
    parent directory on the user's local machine.
      
    Parameters
    __________
    kaggle_path: string
                 Path to the dataset on Kaggle's
                 website. Contains the username of the
                 dataset's uploader and the name of 
                 the dataset.

                 Example: `username/userdataset`

    Returns
    _______
    str
                The path to the directory containing
                the dataset.
    """
    directory_path =  kagglehub.dataset_download("baraazaid/superhero-battles")
    print("Path to dataset files:", directory_path)
    return directory_path

def main():
    # Download latest version
    path = kagglehub.dataset_download("baraazaid/superhero-battles")

    print("Path to dataset files:", path)

if __name__ == "__main__":
    main()