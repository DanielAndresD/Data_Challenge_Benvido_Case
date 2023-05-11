import pandas as pd
from csv import Error

def extract_data(csv_path: str):
    """Extract the data from a csv file and preprocess data, returns a Pandas dataframe."""
    try:
        #Create dataset from csv path
        df= pd.read_csv(csv_path)
        #Preprocessing data
        df = df.dropna()  # Drop rows with null values
        df = df.drop_duplicates()  # Drop duplicated values
        return df 
    except Error as ex:
        print("Error during file extract: {}".format(ex))