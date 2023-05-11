""" 
Database functions module
___________________________________________________________________________________
Autor: Daniel Dávila Lesmes
Contact: danielandresd998@gmail.com - https://www.linkedin.com/in/danielandresd/
Created at: 11-05-2023
Version:01
Last Change: 11-05-2023
___________________________________________________________________________________
"""

#import modules & functions
import json
import pymysql
from pandas import DataFrame

from pymysql import Error


# Get credentials file
def get_db_credentials_from_json_file_path(path: str):
    """Read a json file from path and returns a json var with the SQL connection credentials"""
    with open(path) as credentials_json:
        return json.load(credentials_json)

# Connect to database 
def connect_to_mysql_db(credentials_json):
    """Create a database connection to the MySQL server, from a json"""
    connection = None
    try:
        connection = pymysql.connect(
            host=credentials_json['HOST'],
            port=credentials_json['PORT'],
            user=credentials_json['USERNAME'],
            password=credentials_json['PASSWORD'],
            db=credentials_json['DATABASE'],
        )
        print("MySQL Database connection successful")
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    return connection


# Create a insert query from dataframe
def create_insert_query_from_df(df: DataFrame,table_name: str):
    """Get the headers from a Pandas Dataframe and return str with insert query statement"""
    col_names=list(df.columns)
    rows= f"{', '.join(['%s']*len(col_names))}"
    return f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({rows})"

#Insert data to database
def insert_data_into_db(connection,cursor,query, row):
    """Insert data into database."""
    try:
        cursor.execute(query, row)
        connection.commit()
        
    except Error as e:
        print (f"Error in query {query} {e} {row}")
        connection.rollback()
    finally:
        connection.close()