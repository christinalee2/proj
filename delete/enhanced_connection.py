"""
Enhanced database connection with generic table support
Extends the existing connection to work with any table configuration
"""
import boto3
from pyathena import connect
from pyathena.cursor import Cursor
from typing import Optional, Dict, Any, List
import streamlit as st
import pandas as pd
from datetime import datetime
import uuid
import os
import io

from config import AWS_REGION, ATHENA_DATABASE, ATHENA_OUTPUT_LOCATION
from table_configs import get_table_config


class EnhancedDatabaseConnection:
    """Enhanced database connection that works with any table"""
    
    _connection: Optional[Cursor] = None
    
    # S3 configuration
    S3_BUCKET = os.getenv('S3_BUCKET', 'cpi-uk-us-datascience-stage')
    S3_DATA_PATH = os.getenv('S3_DATA_PATH', 'auxiliary-data/reference-data/reference-db/')
    
    @classmethod
    def get_connection(cls) -> Cursor:
        """Get or create a database connection"""
        if cls._connection is None:
            cls._connection = connect(
                region_name=AWS_REGION,
                s3_staging_dir=ATHENA_OUTPUT_LOCATION,
                schema_name=ATHENA_DATABASE
            )
        return cls._connection
    
    @classmethod
    def execute_query(cls, query: str, parameters: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a SELECT query and return results as DataFrame"""
        conn = cls.get_connection()
        cursor = conn.cursor()
        
        try:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Get data
            data = cursor.fetchall()
            
            # Convert to DataFrame
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
    def execute_insert(cls, table: str, data: Dict[str, Any]) -> bool:
        """
        Insert data into any table by writing Parquet to S3
        
        Args:
            table: Table name
            data: Dictionary of column names and values
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get table configuration
            config = get_table_config(table)
            if not config:
                raise ValueError(f"No configuration found for table: {table}")
            
            # Convert single record to DataFrame
            df = pd.DataFrame([data])
            
            # Handle data type conversions based on table schema
            df = cls._convert_data_types(df, table)
            
            # Replace NaN with None for proper NULL handling
            df = df.where(pd.notna(df), None)
            
            # Generate unique filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            filename = f"{table}_{timestamp}.parquet"
            s3_key = f"{cls.S3_DATA_PATH}{table}/{filename}"
            
            # Write to S3 as Parquet
            s3_client = boto3.client('s3', region_name=AWS_REGION)
            
            # Convert DataFrame to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=cls.S3_BUCKET,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
            
            print(f"✅ Wrote to S3: s3://{cls.S3_BUCKET}/{s3_key}")
            return True
            
        except Exception as e:
            st.error(f"S3 insert error for {table}: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
            return False
    
    @classmethod
    def bulk_insert(cls, table: str, data_list: List[Dict[str, Any]]) -> bool:
        """
        Insert multiple records at once
        
        Args:
            table: Table name
            data_list: List of dictionaries (each dict is one row)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not data_list:
                return True
            
            # Get table configuration
            config = get_table_config(table)
            if not config:
                raise ValueError(f"No configuration found for table: {table}")
            
            # Convert list of dicts to DataFrame
            df = pd.DataFrame(data_list)
            
            # Handle data type conversions
            df = cls._convert_data_types(df, table)
            
            # Replace NaN with None
            df = df.where(pd.notna(df), None)
            
            # Generate unique filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            filename = f"{table}_bulk_{len(data_list)}rows_{timestamp}.parquet"
            s3_key = f"{cls.S3_DATA_PATH}{table}/{filename}"
            
            # Write to S3 as Parquet
            s3_client = boto3.client('s3', region_name=AWS_REGION)
            
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            s3_client.put_object(
                Bucket=cls.S3_BUCKET,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
            
            print(f"✅ Bulk wrote {len(data_list)} rows to S3: s3://{cls.S3_BUCKET}/{s3_key}")
            return True
            
        except Exception as e:
            st.error(f"Bulk insert error for {table}: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
            return False
    
    @classmethod
    def _convert_data_types(cls, df: pd.DataFrame, table: str) -> pd.DataFrame:
        """
        Convert DataFrame columns to appropriate data types based on table schema
        
        Args:
            df: DataFrame to convert
            table: Table name
            
        Returns:
            DataFrame with converted types
        """
        config = get_table_config(table)
        if not config:
            return df
        
        for field_config in config.fields:
            if field_config.name not in df.columns:
                continue
            
            column = field_config.name
            
            try:
                if field_config.field_type == 'number':
                    if 'year' in column.lower() or 'id' in column.lower():
                        # Integer fields
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    else:
                        # Float fields
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                
                elif field_config.field_type == 'boolean':
                    # Convert boolean fields
                    df[column] = df[column].map({
                        'True': True, 'true': True, '1': True, 1: True, 'Yes': True, 'yes': True,
                        'False': False, 'false': False, '0': False, 0: False, 'No': False, 'no': False,
                        None: None, '': None
                    })
                
                elif field_config.field_type in ['text', 'textarea', 'select']:
                    # String fields - ensure they're strings and strip whitespace
                    df[column] = df[column].astype(str).str.strip()
                    df[column] = df[column].replace('nan', None)
                    df[column] = df[column].replace('', None)
                
            except Exception as e:
                print(f"Warning: Could not convert column {column}: {str(e)}")
        
        return df
    
    @classmethod
    def get_table_data(cls, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get data from any table
        
        Args:
            table_name: Name of the table
            limit: Optional limit on number of rows
            
        Returns:
            DataFrame with table data
        """
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        return cls.execute_query(query)
    
    @classmethod
    def check_table_exists(cls, table_name: str) -> bool:
        """
        Check if a table exists in the database
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            query = f"SHOW TABLES LIKE '{table_name}'"
            result = cls.execute_query(query)
            return not result.empty
        except:
            return False
    
    @classmethod
    def get_table_schema(cls, table_name: str) -> pd.DataFrame:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame with column information
        """
        try:
            query = f"DESCRIBE {table_name}"
            return cls.execute_query(query)
        except Exception as e:
            st.error(f"Error getting schema for {table_name}: {str(e)}")
            return pd.DataFrame()
    
    @classmethod
    def close_connection(cls):
        """Close the database connection"""
        if cls._connection:
            cls._connection.close()
            cls._connection = None


# Enhanced cached queries for generic tables
@st.cache_data(ttl=7200)  # Cache for 2 hours
def get_generic_table_data_cached(table_name: str, limit: Optional[int] = 1000) -> pd.DataFrame:
    """
    Get cached table data for any table
    
    Args:
        table_name: Name of the table
        limit: Optional limit on rows
        
    Returns:
        Cached DataFrame
    """
    return EnhancedDatabaseConnection.get_table_data(table_name, limit)


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_generic_dropdown_options(table_name: str) -> Dict[str, List[str]]:
    """
    Get dropdown options for a specific table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary of field options
    """
    from services.generic_table_service import GenericTableServiceFactory
    
    try:
        service = GenericTableServiceFactory.get_service(table_name)
        return service.get_dropdown_options()
    except Exception as e:
        print(f"Error getting dropdown options for {table_name}: {e}")
        return {}