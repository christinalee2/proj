import dropbox
from dropbox.exceptions import AuthError
from dropbox import DropboxOAuth2FlowNoRedirect
import os
import pandas as pd
import numpy as np
import io
import unicodedata
from datetime import date
import uuid
from dotenv import load_dotenv, find_dotenv
import s3fs
import boto3
load_dotenv(find_dotenv())
from cpi_tools import aws_tools

S3_REFERENCE_BUCKET = 'cpi-reference-data'

"""
######################################################################################################
######################## Dropbox and reference table processing functions ############################
######################################################################################################

"""


def dropbox_authorisation(app_key:str, secret_key:str):
    """
    This function initiates an authorization process for a Dropbox App. 

    Parameters:
    app_key (str): The app key for your Dropbox app. You get this when you create an app on the Dropbox developers' site.
    secret_key (str): The secret key for your Dropbox app. This is also provided when you create an app.

    Returns:
    oauth_result: A DropboxOAuth2FlowNoRedirect result object which contains access token and other information.
    """

    # Creating an instance of DropboxOAuth2FlowNoRedirect using app_key and secret_key.
    # This class performs the second half of the OAuth2 'code' flow, where a code is exchanged for a token.
    auth_flow = DropboxOAuth2FlowNoRedirect(app_key, secret_key)

    # Starting the OAuth2 flow and getting the authorization URL.
    authorize_url = auth_flow.start()

    # Printing instructions for the user.
    print ("1. Go to: " + authorize_url)
    print ("2. Click 'Allow' (you might have to log in first).")
    print ("3. Copy the authorization code.")

    # Taking the authorization code input from the user.
    auth_code = input("Enter the authorization code here: ").strip()

    # Trying to finish the OAuth2 flow using the authorization code and obtaining the result.
    try:
        oauth_result = auth_flow.finish(auth_code)
    except Exception as e:
        print("Error: %s" % (e,))

    # Return the result which can be used for making Dropbox API calls.
    return oauth_result

def change_dropbox_path_to_teamspace(current_dbx): 
    """
    This function changes the path for the inputted Dropbox Client (dbx) to incorporate the user's unique root id. 
    This function is now needed in order to access files in our teamspace path, after the May 2024 Dropbox migration to using an organization teamspace. 
    Reference:
        Using the Dropbox-API-Path-Root Header: https://developers.dropbox.com/dbx-team-files-guide#using-the-dropbox-api-path-root-header

    Parameters:
    dbx: (Dropbox Client): the dbx client returned after performing your dropbox authorization. 

    Returns:
    updated_dbx (Dropbox Client): the updated dbx client that incorporates your root namespace id, in order to appropriately access teamspace files. 
    """
    updated_dbx = None
    try: 
        root_namespace_id = current_dbx.users_get_current_account().root_info.root_namespace_id
        updated_dbx = current_dbx.with_path_root(dropbox.common.PathRoot.root(root_namespace_id))
    except Exception as e:
        print("Error changing your dropbox client to access our teamspace: " + str(e))
    return updated_dbx

def export_dropbox_file_to_s3(dropbox_file_path:str, s3_bucket:str, s3_path:str, s3_file_name:str):
    """
    This function exports a file from Dropbox to an Amazon S3 bucket.

    Parameters:
    dropbox_file_path (str): The path of the file in Dropbox.
    s3_bucket (str): The name of the S3 bucket to which the file is to be uploaded.
    s3_path (str): The path in the S3 bucket where the file should be placed.
    s3_file_name (str): The name with which the file should be stored in the S3 bucket.

    Returns:
    None
    """
    
    # Attempt to load environment variables for Dropbox app key and secret key
    try:
        app_key = os.getenv('DROPBOX_APP_KEY')
        secret_key = os.getenv('DROPBOX_SECRET_KEY')
    except Exception as e:
        print('Error Loading Dropbox app and secret key. Ensure variables are in your .env file: ' + str(e))

    # Authorize Dropbox API usage with the app key and secret key
    oauth = dropbox_authorisation(app_key=app_key, secret_key=secret_key)
    
    # Attempt to download the file from Dropbox and upload to S3
    try:
        # Set up Dropbox client with OAuth credentials
        with dropbox.Dropbox(oauth2_access_token=oauth.access_token,
                             oauth2_access_token_expiration=oauth.expires_at,
                             oauth2_refresh_token=oauth.refresh_token,
                             app_key=app_key,
                             app_secret=secret_key) as dbx:
            
            # update dbx client to access our teamspace
            dbx = change_dropbox_path_to_teamspace(dbx)
                                 
            print("Successfully set up client!")

            # Download the file from Dropbox
            metadata, result = dbx.files_download(path=dropbox_file_path)

            try: 
                # Set up S3 client
                s3 = boto3.resource('s3')
                # Upload the file to S3
                s3.Bucket(s3_bucket).put_object(Key=f'{s3_path}/{s3_file_name}', Body=result.content)
            except Exception as e:
                print('Error uploading dropbox data to S3: ' + str(e))

    except Exception as e:
        print('Error downloading file from Dropbox: ' + str(e))

    return None


def process_reference_data(base_data, file_name=None, sql_field_name=None, col_name_row=0, start_row=3, project_type=False): 
    '''
    This function takes an unprocessed reference dataframe taken from the master reference table excel and converts it
    into a format useable within processing code
    
    If a filename is provided, it then writes this processed data to an s3 bucket 
    
    Parameters:
    base_data: Pandas Dataframe
    file_name: String
    sql_field_name: String - If the reference table has a SQL Field name column, this is dropped
    col_name_row: Int - selects the row that is used as the column names for the processed data
    start_row: Int - Selects the row to start from, drops the rows prior to this one
    project_type: Boolean - If project_type reference data, apply additional processing steps
    
    Returns
    df: Pandas Dataframe - processed reference data
    '''
    
    df = base_data.copy()

    #Set the column names to the correct rows
    df.columns = df.iloc[col_name_row]
    
    #If the ref table has a sql field, drop it
    if sql_field_name:

        df.drop(columns = sql_field_name, inplace=True)
    
    #Select the row to start from
    df = df.iloc[start_row:]
    
    #Drop columns called nan
    df = df.loc[:, df.columns.notna()]
    
    #If ref_project_type reference data, create a use column
    if project_type:
            df['use'] = df.apply(lambda x: generate_project_type_use(x['MI'], x['AD']), axis=1)
        
    
    #Drop rows where all values are nan
    df.dropna(axis = 0, how = 'all', inplace = True)

    #Remove whitespace from strings and column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    #Enforce unicode conversion
    df = df.map(lambda x: unicodedata.normalize("NFKD",x) if isinstance(x, str) else x)
    
    #Write to S3 if filename is provided
    if file_name:
            
        aws_tools.write_to_s3(df, S3_REFERENCE_BUCKET, 'cpi/processed', file_name)
        
def generate_project_type_use(mi, ad):
    '''
    This function generates a Use column in the project type reference data using the MI and AD columns
    
    mi: Boolean Column
    ad: Boolean Column
    
    Returns
    use: String Column
    
    '''

    if ((mi == 1) & (ad == 1)):
        use = 'Multiple Objectives'
    elif mi == 1:
        use = 'Mitigation'
    elif ad == 1:
        use = 'Adaptation'
    else:
        use = np.nan

    return use

def extract_reference_sheets():
    """
    This function extracts multiple reference sheets from an Excel file stored in an Amazon S3 bucket.
    It then processes each sheet using the 'process_reference_data' function.

    Parameters: None

    Returns: None
    """
    # Define the path to the file in the S3 bucket
    s3_file_path = 'cpi/raw'
    # Define the name of the file
    file_name = 'master_ref.xlsm'
    
    # For each sheet in the Excel file, read the data into a pandas DataFrame
    # and then process that data using the 'process_reference_data' function.
    # The arguments passed to 'process_reference_data' vary depending on the specific sheet being processed.

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='coeff'),
                           'ref_coef', 'SQL Field Name')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='double_counting'),
                           'ref_double_counting', 'SQL Field Name')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='fx'),
                           'ref_fx', 'SQL Field Name')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='gearing'),
                           'ref_gearing')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='gender'),
                           'ref_gender', 'SQL Field Name')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='geog_list_all'),
                           'ref_geog_list_all', 'SQL Field Name')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='geog_list_cpi'),
                           'ref_geog_list_cpi', 'SQL Field Name', 1,4)

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='institution_list_all'),
                           'ref_institution_list_all', 'SQL Field Name')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='institution_list_cpi'),
                           'ref_institution_list_cpi','SQL Field Name', start_row=4)

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='instr'),
                           'ref_instr', 'SQL Field Name')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='multiplier'),
                           'ref_multiplier')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='recipient'),
                           'ref_recipient')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='project_type'),
                           'ref_project_type', 'SQL Field Name', project_type=True)
        
    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='ownership'),
                           'ref_ownership')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='sector'),
                           'ref_sector', 'SQL Field Name')

    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='soe'),
                           'ref_soe', 'SQL Field Name')
    
    process_reference_data(pd.read_excel(f's3://{S3_REFERENCE_BUCKET}/{s3_file_path}/{file_name}', sheet_name='data_source'),
                           'ref_data_source')

    return None


def update_ref_tables():
    """
    This function updates reference tables by reading a master reference table from Dropbox,
    exporting it to an Amazon S3 bucket, and then extracting reference data from the table.

    Parameters: None

    Returns: None
    """

    # Output status update
    print('Reading Master Reference Table...')

    # Define the path to the file in Dropbox
    reference_metadata_dropbox_path = '/CF-Program/1.Workstreams/Tracking/GLCF/GLCF MASTER/3. Reference Tables Excel/Reference Tables_v1_MASTER_2022.xlsm'
    
    # Export the file from Dropbox to an S3 bucket
    export_dropbox_file_to_s3(reference_metadata_dropbox_path, s3_bucket='cpi-reference-data',
                              s3_path='cpi/raw', s3_file_name='master_ref.xlsm')
    
    # Output status update
    print('Extracting reference data')

    # Extract reference data from the master reference table
    extract_reference_sheets()

    return None



def get_reference_tables(update=False):
    """
    This function retrieves various reference tables stored in an Amazon S3 bucket as pandas DataFrames.
    If the 'update' parameter is set to True, it will first update the reference tables
    by calling the 'update_ref_tables' function.

    Parameters:
    update (bool): If True, the reference tables are updated before being retrieved.

    Returns:
    tuple: A tuple of pandas DataFrames, each one representing a reference table.
    """
    
    # If the 'update' parameter is set to True, update the reference tables
    if update:
        update_ref_tables()

    # Define the path to the folder in the S3 bucket where the reference tables are stored
    cpi_reference_folder = 'cpi/processed'

    # Retrieve each reference table from the S3 bucket and read it into a pandas DataFrame.
    # Note that each file is read with 'utf-8' encoding.
    # Each DataFrame is stored in a separate variable.

    # Data Source Geography Names
    ref_geog_list_all = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_geog_list_all.csv', encoding='utf-8')
    
    # Coefficient table
    ref_coeff =  pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_coef.csv', encoding='utf-8')
    
    # Foreign exchange rates
    ref_fx =  pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_fx.csv', encoding='utf-8')
    
    # Gender table
    ref_gender =  pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_gender.csv', encoding='utf-8')
    
    # Gearing table
    ref_gearing =  pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_gearing.csv', encoding='utf-8')
    
    # CPI Geography Names
    ref_geog_list_cpi =  pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_geog_list_cpi.csv', encoding='utf-8')
    
    # Data Source Institution names
    ref_institution_list_all = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_institution_list_all.csv', encoding='utf-8')
    
    # CPI Institution names
    ref_institution_list_cpi = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_institution_list_cpi.csv', encoding='utf-8')

    # Ownership table
    ref_ownership = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_ownership.csv', encoding='utf-8')
    
    # Project type table
    ref_project_type = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_project_type.csv')
    
    # Sector table
    ref_sector = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_sector.csv', encoding='utf-8')
    
    # Instrument table
    ref_instr = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_instr.csv', encoding='utf-8')
    
    # Capacity multiplier tables
    ref_multiplier = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_multiplier.csv', encoding='utf-8')
    
    # Recipient table
    ref_recipient = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_recipient.csv', encoding='utf-8')
    
    # Data source
    ref_data_source = pd.read_csv(f's3://{S3_REFERENCE_BUCKET}/{cpi_reference_folder}/ref_data_source.csv', encoding='utf-8')

    # Return all the DataFrames as a tuple
    return ref_geog_list_all, ref_coeff, ref_fx, ref_gender, ref_gearing, ref_geog_list_all, ref_geog_list_cpi, ref_institution_list_all, \
    ref_institution_list_cpi, ref_ownership, ref_project_type, ref_sector, ref_instr, ref_multiplier, ref_recipient, ref_data_source


def generate_glcf_id(df:pd.DataFrame, year:int):
    '''
    This function generates a unique ID for each entry in the dataset processed. 
    
    Parameters:
    df(pd.DataFrame): the dataset being processed. 
    Note that it is important that this dataset has already been cleaned and most
    importantly subset to only the relevant row entries, as the final data ID will
    include the row number of each observation. 
    year(int): the year of the GLCF publication (e.g., 2023 for data relating to
    2021 and 2022).
    
    Returns:
    df(pd.DataFrame): the inputted dataframe, with the addition of the column 'id_glcf'.
    '''
    
    df['id_glcf'] = df.data_source_code.map(lambda x: str(x).zfill(3)) + '-' + \
                           str(year) + '-' + \
                           df.index.map(lambda x: str(x+1).zfill(9))

    return df
    
    
    
    
