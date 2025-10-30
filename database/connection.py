import boto3
from pyathena import connect
from pyathena.cursor import Cursor
from typing import Optional, Dict, Any, List
import streamlit as st
import pandas as pd
from config import AWS_REGION, ATHENA_DATABASE, ATHENA_OUTPUT_LOCATION, S3_BUCKET, CURRENT_YEAR
from table_configs import get_column_type_config, get_table_id_column
import os
import traceback
import io

##########################
#Generally there's AWS wrangler inserts that are faster and better - and then there are also inserts from my original implementation that rewrite the whole parquet file with the new entry if wrangler fails for some reason. These functions are labeled ORIGINAL VERSION but shouldn't ever really be running, just kept as a fallback
##########################

try:
    import awswrangler as wr
    HAS_WRANGLER = True
except ImportError:
    HAS_WRANGLER = False
    print("WARNING: awswrangler not available, falling back to original insert method")


def get_next_id_for_table(existing_df, table_name):
    """Slow fallback version - gets id that's one greater than the current id, finds id column automatically, shouldn't ever really be running - ORIGINAL VERSION"""
    
    id_column = None
    if not existing_df.empty:
        for col in existing_df.columns:
            if 'id' in col.lower():
                id_column = col
                break
    
    if not id_column:
        id_column = f'id_{table_name}'
    
    if existing_df.empty or id_column not in existing_df.columns:
        return id_column, 1
    
    try:
        non_null_ids = existing_df[id_column].dropna()
        if len(non_null_ids) == 0:
            return id_column, 1
        
        numeric_ids = pd.to_numeric(non_null_ids, errors='coerce').dropna()
        #Slow b/c needs access to all ids
        max_id = int(numeric_ids.max()) if len(numeric_ids) > 0 else 0
        return id_column, max_id + 1
    except:
        return id_column, 1


class DatabaseConnection:

    _connection: Optional[Cursor] = None
    
    S3_BUCKET = os.getenv('S3_BUCKET', 'cpi-uk-us-datascience-stage')
    S3_BASE_PATH = 'auxiliary-data/reference-data/reference-db-2'

    #Hardcoding in options for table locations, update to add more tables
    TABLE_FILES = {
        'institution': f'{S3_BASE_PATH}/institution/data.parquet',
        'geography': f'{S3_BASE_PATH}/geography/data.parquet',
        'sector': f'{S3_BASE_PATH}/sector/data.parquet',
        'instrument': f'{S3_BASE_PATH}/instrument/data.parquet',
        'gender': f'{S3_BASE_PATH}/gender/data.parquet',
        'data_source': f'{S3_BASE_PATH}/data_source/data.parquet',
        'recipient': f'{S3_BASE_PATH}/recipient/data.parquet',
        'multiplier': f'{S3_BASE_PATH}/multiplier/data.parquet',
        'gearing': f'{S3_BASE_PATH}/gearing/data.parquet',
        'country_coefficients': f'{S3_BASE_PATH}/country_coefficients/data.parquet',
        'exchange_rates': f'{S3_BASE_PATH}/exchange_rates/data.parquet',
        'state_control': f'{S3_BASE_PATH}/state_control/data.parquet',
        'institution_standardization': f'{S3_BASE_PATH}/institution_standardization/data.parquet',
        'geography_standardization': f'{S3_BASE_PATH}/geography_standardization/data.parquet',
        'hierarchy': f'{S3_BASE_PATH}/hierarchy/data.parquet'
    }
    
    # Table S3 locations for awswrangler inserts
    TABLE_LOCATIONS = {
        'institution': f's3://{S3_BUCKET}/{S3_BASE_PATH}/institution/',
        'geography': f's3://{S3_BUCKET}/{S3_BASE_PATH}/geography/',
        'sector': f's3://{S3_BUCKET}/{S3_BASE_PATH}/sector/',
        'instrument': f's3://{S3_BUCKET}/{S3_BASE_PATH}/instrument/',
        'multiplier': f's3://{S3_BUCKET}/{S3_BASE_PATH}/multiplier/',
        'gearing': f's3://{S3_BUCKET}/{S3_BASE_PATH}/gearing/',
        'gender': f's3://{S3_BUCKET}/{S3_BASE_PATH}/gender/',
        'data_source': f's3://{S3_BUCKET}/{S3_BASE_PATH}/data_source/',
        'recipient': f's3://{S3_BUCKET}/{S3_BASE_PATH}/recipient/',
        'country_coefficients': f's3://{S3_BUCKET}/{S3_BASE_PATH}/country_coefficients/',
        'exchange_rates': f's3://{S3_BUCKET}/{S3_BASE_PATH}/exchange_rates/',
        'state_control': f's3://{S3_BUCKET}/{S3_BASE_PATH}/state_control/',
        'institution_standardization': f's3://{S3_BUCKET}/{S3_BASE_PATH}/institution_standardization/',
        'geography_standardization': f's3://{S3_BUCKET}/{S3_BASE_PATH}/geography_standardization/',
        'hierarchy': f's3://{S3_BUCKET}/{S3_BASE_PATH}/hierarchy/'
    }



    
    
    @classmethod
    def get_connection(cls) -> Cursor:
        """Get or create a database connection """
        if cls._connection is None:
            cls._connection = connect(
                region_name=AWS_REGION,
                s3_staging_dir=ATHENA_OUTPUT_LOCATION,
                schema_name=ATHENA_DATABASE
            )
        return cls._connection



    
    @classmethod
    def get_table_data(cls, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from any table """
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        return cls.execute_query(query)



    
    @classmethod
    def execute_query(cls, query: str, parameters: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a SELECT query and return results as a pandas DataFrame """
        conn = cls.get_connection()
        cursor = conn.cursor()
        
        try:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            data = cursor.fetchall()
            
            if data and columns:
                df = pd.DataFrame(data, columns=columns)
            else:
                df = pd.DataFrame()
            
            return df
        except Exception as e:
            st.error(f"Database query error: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
            return pd.DataFrame()
        finally:
            cursor.close()



    
    @classmethod
    def check_table_exists(cls, table_name: str) -> bool:
        """Check if table exists """
        try:
            query = f"SHOW TABLES LIKE '{table_name}'"
            result = cls.execute_query(query)
            return not result.empty
        except:
            return False




    
    @classmethod
    def _read_existing_parquet(cls, table_name: str) -> pd.DataFrame:
        """Read existing data from the specific S3 parquet file, needs to be loaded into TABLE_FILES"""
        if table_name not in cls.TABLE_FILES:
            raise ValueError(f"Unknown table: {table_name}")
        
        s3_key = cls.TABLE_FILES[table_name]
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        try:
            print(f"Reading existing data from s3://{cls.S3_BUCKET}/{s3_key}")
            
            response = s3_client.get_object(Bucket=cls.S3_BUCKET, Key=s3_key)
            existing_df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            
            print(f"Found {len(existing_df)} existing rows")
            return existing_df
            
        except s3_client.exceptions.NoSuchKey:
            print(f"No existing file found at s3://{cls.S3_BUCKET}/{s3_key}, creating new table")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error reading existing data from {s3_key}: {e}")
            raise e




    
    @classmethod
    def _clean_dataframe_for_insert(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for insertion, makes sure they match expected int formats for year columns"""
        try:
            df_clean = df.copy()
            
            df_clean = df_clean.replace(['', 'None', 'null', 'NULL'], None)
            
            for col in df_clean.columns:
                if 'year' in col.lower() or 'id_' in col or col == 'last_verified':
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='ignore')
                elif col in ['double_counting_risk']:
                    # Convert boolean-like columns
                    df_clean[col] = df_clean[col].map({
                        'True': True, 'true': True, '1': True, 1: True,
                        'False': False, 'false': False, '0': False, 0: False
                    })
            
            if 'last_verified' in df_clean.columns:
                df_clean['last_verified'] = CURRENT_YEAR
            
            return df_clean
            
        except Exception as e:
            print(f"Error cleaning DataFrame: {e}")
            return df



    @classmethod
    def _apply_column_types(cls, df: pd.DataFrame, table: str) -> pd.DataFrame:
        """Explicitly casts proper data types to DataFrame columns based on table configuration"""
        
        column_config = get_column_type_config(table)
        if not column_config:
            print(f"No column type configuration found for table: {table}")
            return df
        
        for col in column_config.string_columns:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        for col in column_config.integer_columns:
            if col in df.columns:
                df[col] = df[col].astype('Int64')  # Nullable integer
        
        if column_config.float_columns:
            for col in column_config.float_columns:
                if col in df.columns:
                    df[col] = df[col].astype('Float64')  # Nullable float
        
        if column_config.boolean_columns:
            for col in column_config.boolean_columns:
                if col in df.columns:
                    df[col] = df[col].astype('boolean')  # Nullable boolean
        
        return df



        
    @classmethod
    def get_next_id_efficiently(cls, table: str) -> int:
        """Get next ID using Athena query instead of reading full table"""
        try:
            id_column = get_table_id_column(table)
            if not id_column:
                raise ValueError(f"No ID column configured for table: {table}")
            
            # Use Athena to get max ID efficiently
            query = f"SELECT MAX({id_column}) as max_id FROM {table}"
            print(f"Getting max ID with query: {query}")
            
            result = cls.execute_query(query)
            
            if result.empty or result.iloc[0]['max_id'] is None:
                print(f"No existing records found, starting with ID 1")
                return 1
                
            max_id = int(result.iloc[0]['max_id'])
            next_id = max_id + 1
            print(f"Found max ID {max_id}, next ID will be {next_id}")
            return next_id
            
        except Exception as e:
            print(f"Athena query failed, falling back to full table read: {e}")
            # Fallback to the slow method if Athena query fails
            existing_data = cls.get_table_data(table, limit=None)
            id_column, next_id = get_next_id_for_table(existing_data, table)
            return next_id
        
    
    @classmethod
    def execute_insert(cls, table: str, data: Dict[str, Any]) -> bool:
        """
        Insert a row using awswrangler if available, fallback to original method
        """
        if HAS_WRANGLER:
            return cls._execute_insert_awswrangler(table, data)
        else:
            return cls._execute_insert_original(table, data)



    
    @classmethod
    def _execute_insert_awswrangler(cls, table: str, data: Dict[str, Any]) -> bool:
        """Insert using awswrangler -"""
        try:
            print(f"=== STARTING AWSWRANGLER INSERT FOR {table} ===")

                    
            
            id_column = get_table_id_column(table)
            if not id_column:
                raise ValueError(f"No ID column configured for table: {table}")
                
            next_id = cls.get_next_id_efficiently(table)
            
            data_with_id = data.copy()
            data_with_id[id_column] = next_id
            print(f"New record will have {id_column} = {next_id}")
            
            df_to_insert = pd.DataFrame([data_with_id])
            
            df_cleaned = cls._clean_dataframe_for_insert(df_to_insert)
            
            if df_cleaned.empty:
                print("No valid data to insert after cleaning")
                return False

            df_cleaned = cls._apply_column_types(df_cleaned, table)
            
            s3_path = cls.TABLE_LOCATIONS.get(table)
            if not s3_path:
                raise ValueError(f"No S3 location configured for table: {table}")
            
            print(f"Inserting to S3 location: {s3_path}")

          
            wr.s3.to_parquet(
                df=df_cleaned,
                path=s3_path,
                dataset=True,
                mode='append',
                # database=ATHENA_DATABASE,
                # table=table,
                compression='snappy'
            )
            
            print(f"SUCCESS: Added 1 row to {table} table via awswrangler")
            
            try:
                st.cache_data.clear()
            except:
                pass
            
            print(f"=== AWSWRANGLER INSERT COMPLETE FOR {table} ===")
            return True
            
        except Exception as e:
            print(f"AWSWRANGLER INSERT ERROR for {table}: {str(e)}")
            traceback.print_exc()
            print("Falling back to original insert method")
            return cls._execute_insert_original(table, data)



            
    
    @classmethod
    def _execute_insert_original(cls, table: str, data: Dict[str, Any]) -> bool:
        """Fallback to original insert method if awswrangler fails, much slower because it rewrites whole file - ORIGINAL VERSION"""
        try:
            print(f"=== USING ORIGINAL INSERT METHOD FOR {table} ===")
            
            existing_df = cls._read_existing_parquet(table)
            original_count = len(existing_df)
            
            id_column, next_id = get_next_id_for_table(existing_df, table)
            data_with_id = data.copy()
            data_with_id[id_column] = next_id

            for key, value in data_with_id.items():
                print(f"  {key}: {value} (type: {type(value)})")
            
            if existing_df.empty:
                for field in ['last_verified', 'created_at']:
                    if field in data_with_id and data_with_id[field] is not None:
                        try:
                            original_value = data_with_id[field]
                            data_with_id[field] = int(float(data_with_id[field]))
                            
                        except (ValueError, TypeError):
                            from config import CURRENT_YEAR
                            data_with_id[field] = CURRENT_YEAR
                            
                
                new_df = pd.DataFrame([data_with_id])
            else:
                
                new_row = {}
                for col in existing_df.columns:
                    new_row[col] = data_with_id.get(col, None)
                    
                # Apply type conversion for existing DataFrame case
                for field in ['last_verified', 'created_at']:
                    if field in new_row and new_row[field] is not None:
                        try:
                            original_value = new_row[field]
                            new_row[field] = int(float(new_row[field]))
                        except (ValueError, TypeError):
                            from config import CURRENT_YEAR
                            new_row[field] = CURRENT_YEAR
                                
                new_row_df = pd.DataFrame([new_row])
                
                new_df = pd.concat([existing_df, new_row_df], ignore_index=True)
            
            return cls._write_parquet_file(table, new_df)
            
        except Exception as e:
            print(f"ORIGINAL INSERT ERROR for {table}: {str(e)}")
            traceback.print_exc()
            return False




            
    
    @classmethod
    def bulk_insert(cls, table: str, data_list: List[Dict[str, Any]]) -> bool:
        """Bulk insert using awswrangler if available, fallback to original"""
        if HAS_WRANGLER:
            return cls._bulk_insert_awswrangler(table, data_list)
        else:
            return cls._bulk_insert_original(table, data_list)
    
    @classmethod
    def _bulk_insert_awswrangler(cls, table: str, data_list: List[Dict[str, Any]]) -> bool:
        """Bulk insert using awswrangler"""
        try:
            if not data_list:
                return True
            
            print(f"=== STARTING AWSWRANGLER BULK INSERT FOR {table} ({len(data_list)} rows) ===")


            id_column = get_table_id_column(table)
            if not id_column:
                existing_data = cls.get_table_data(table, limit=1) 
                id_column, _ = get_next_id_for_table(existing_data, table)
                
            next_id = cls.get_next_id_efficiently(table)
            print(f"Using ID column: {id_column}, starting from ID: {next_id}")
            
            # Prepare records with sequential IDs
            records_with_ids = []
            for i, record in enumerate(data_list):
                record_with_id = record.copy()
                record_with_id[id_column] = next_id + i
                records_with_ids.append(record_with_id)
            
            df_to_insert = pd.DataFrame(records_with_ids)
            df_cleaned = cls._clean_dataframe_for_insert(df_to_insert) #validation to get it into correct format
        
            
            if df_cleaned.empty:
                print("No valid data to insert after cleaning")
                return False

            try:
                df_cleaned = cls._apply_column_types(df_cleaned, table)
            except AttributeError:
                pass
            
            s3_path = cls.TABLE_LOCATIONS.get(table)
            if not s3_path:
                raise ValueError(f"No S3 location configured for table: {table}")
            
            wr.s3.to_parquet(
                df=df_cleaned,
                path=s3_path,
                dataset=True,
                mode='append',
                # database=ATHENA_DATABASE,
                # table=table,
                compression='snappy'
            )
            
            print(f"SUCCESS: Added {len(df_cleaned)} rows to {table} table via awswrangler")
            
            try:
                st.cache_data.clear()
            except:
                pass
            
            return True
            
        except Exception as e:
            print(f"AWSWRANGLER BULK INSERT ERROR: {str(e)}")
            traceback.print_exc()
            return cls._bulk_insert_original(table, data_list)




            
    
    @classmethod
    def _bulk_insert_original(cls, table: str, data_list: List[Dict[str, Any]]) -> bool:
        """Fallback bulk insert using original method - ORIGINAL VERSION"""
        try:
            if not data_list:
                return True
                
            print(f"=== USING ORIGINAL BULK INSERT FOR {table} ({len(data_list)} rows) ===")
            
            existing_df = cls._read_existing_parquet(table)
            original_count = len(existing_df)
            
            id_column, next_id = get_next_id_for_table(existing_df, table)
            
            new_rows = []
            for i, data in enumerate(data_list):
                data_with_id = data.copy()
                data_with_id[id_column] = next_id + i
                
                if existing_df.empty:
                    new_rows.append(data_with_id)
                else:
                    new_row = {}
                    for col in existing_df.columns:
                        new_row[col] = data_with_id.get(col, None)

                    for field in ['last_verified', 'created_at']:
                        if field in new_row and new_row[field] is not None:
                            try:
                                new_row[field] = int(float(new_row[field]))
                            except (ValueError, TypeError):
                                from config import CURRENT_YEAR
                                new_row[field] = CURRENT_YEAR
                    
                    new_rows.append(new_row)
            
            if existing_df.empty:
                new_df = pd.DataFrame(new_rows)
            else:
                new_rows_df = pd.DataFrame(new_rows)
                new_df = pd.concat([existing_df, new_rows_df], ignore_index=True)
            
            success = cls._write_parquet_file(table, new_df)
            
            if success:
                print(f"SUCCESS: Added {len(data_list)} rows to {table}")
            
            return success
            
        except Exception as e:
            print(f"ORIGINAL BULK INSERT ERROR: {str(e)}")
            traceback.print_exc()
            return False




    
    @classmethod
    def _write_parquet_file(cls, table_name: str, df: pd.DataFrame) -> bool:
        """Write the complete DataFrame back to the S3 parquet file - ORIGINAL VERSION"""
        if table_name not in cls.TABLE_FILES:
            raise ValueError(f"Unknown table: {table_name}")
        
        s3_key = cls.TABLE_FILES[table_name]
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        try:
            print(f"Writing {len(df)} rows to s3://{cls.S3_BUCKET}/{s3_key}")
            
            # Convert DataFrame to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Upload the updated file
            s3_client.put_object(
                Bucket=cls.S3_BUCKET,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
            
            print(f"Successfully updated: s3://{cls.S3_BUCKET}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"Error writing parquet file: {e}")
            traceback.print_exc()
            return False
    
    @classmethod
    def close_connection(cls):
        """Close the database connection - ORIGINAL VERSION"""
        if cls._connection:
            cls._connection.close()
            cls._connection = None



@st.cache_resource
def get_cached_connection():
    """Get a cached database connection that persists across Streamlit reruns"""
    return DatabaseConnection.get_connection()