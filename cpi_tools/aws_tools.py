import pandas as pd
import boto3
import s3fs
from io import StringIO

from dotenv import load_dotenv, find_dotenv

def read_from_s3(s3_bucket: str, s3_file_path:str, file_name: str, file_type = 'csv', sheet_name=None, skip_rows=0) -> pd.DataFrame:
    """
    This function reads data from an S3 bucket and returns it as a pandas DataFrame.

    Inputs:

    s3_bucket (str): The name of the S3 bucket where the file is located.
    s3_file_path (str): The file path within the S3 bucket.
    file_name (str): The name of the file to be read.
    file_type (str, optional): The file type of the file to be read. Defaults to 'csv'.
    sheet_name (str, optional): The sheet name to be read from the excel file. Only used if file_type is 'xlsx'.
    skip_rows (int, optional): The number of rows to skip when reading the file. Defaults to 0.
    Returns:

    df (pd.DataFrame): The data read from the file.
    """

    if load_dotenv(find_dotenv()):

        if file_type == 'csv':
            df = pd.read_csv(f's3://{s3_bucket}/{s3_file_path}/{file_name}.csv',skiprows=skip_rows, encoding='utf-8')

        elif file_type == 'xlsx':
            if sheet_name:
                df = pd.read_excel(f's3://{s3_bucket}/{s3_file_path}/{file_name}.xlsx', sheet_name=sheet_name, skiprows=skip_rows)
            else:
                df = pd.read_excel(f's3://{s3_bucket}/{s3_file_path}/{file_name}.xlsx', skiprows=skip_rows)
    else:
        print('No AWS account variables found. Please add account variables to the .env file in your local directory')

    return df


def write_to_s3(df: pd.DataFrame, s3_bucket: str, s3_file_path:str, file_name: str):

    '''
    This function takes a processed reference dataframe and the filename for it to be saved as within S3 
    and uploads the dataframe as a CSV

    Inputs:

    df (pd.DataFrame): The dataframe to be written to s3
    s3_bucket (str): The name of the S3 bucket where the file will ve saved.
    s3_file_path (str): The file path within the S3 bucket.
    file_name (str): The name of the file to be saved.

    Returns:
    None
    '''
    
    #Filepath to processed cpi reference folder
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8")
    s3_resource = boto3.resource('s3')
    s3_resource.Object(s3_bucket, f'{s3_file_path}/{file_name}.csv').put(Body=csv_buffer.getvalue())
    

def write_to_s3_parquet(df: pd.DataFrame, numeric_cols: [str], s3_bucket: str, s3_file_path: str, file_name: str):
    """
    This function takes a pandas df and writes to parquet format in S3
    df (pd.DataFrame):  The dataframe to be written to s3
    numeric_columns([str]): List of columns in df that are numeric
    s3_bucket (str): The name of the S3 bucket where the file will ve saved.
    s3_file_path (str): The file path within the S3 bucket.
    file_name (str): The name of the file to be saved.
    
    Returns:
    None
    
    """
    df = df.astype('string')
    for col in numeric_cols: 
        df[col] = df[col].astype('Float64')
    
    s3_url = f's3://{s3_bucket}/{s3_file_path}/{file_name}.parquet'
    df.to_parquet(s3_url, index = False)
    
    print('Data written to S3')
    
    print('Copy below when adding bulk schema in Athena')
    for col in df.columns:

        col_type = df[col].dtype
        if col_type == 'Float64':
            col_type = 'double'
        print(f'{col} {col_type}, ')
    