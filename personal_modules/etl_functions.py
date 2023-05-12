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

def list_outer_values(df_origin,column_pk,df_join,column_fk):
    """Extract unique values from 2 dataframes and returns outer values from df_join"""
    l1= list(df_origin[column_pk].unique()) #Extract list of unique values from pk table
    l2=list(df_join[column_fk].unique()) # Extract listo of unique values from fk table
    return list(set(l2) ^ set(l1))