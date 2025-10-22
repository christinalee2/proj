import pandas as pd
from typing import List, Optional, Dict, Any
from database.connection import DatabaseConnection


class QueryService:
    """query service that works with any table"""
    
    @staticmethod
    def execute_query(query: str, parameters: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a query and return DataFrame"""
        return DatabaseConnection.execute_query(query, parameters)
    
    @staticmethod
    def execute_insert(table: str, data: Dict[str, Any]) -> bool:
        """Insert data into any table"""
        return DatabaseConnection.execute_insert(table, data)
    
    @staticmethod
    def bulk_insert(table: str, data_list: List[Dict[str, Any]]) -> bool:
        """Bulk insert data into any table"""
        return DatabaseConnection.bulk_insert(table, data_list)
    
    @staticmethod
    def get_table_data(table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from any table"""
        return DatabaseConnection.get_table_data(table_name, limit)
    
    @staticmethod
    def check_table_exists(table_name: str) -> bool:
        """Check if table exists"""
        return DatabaseConnection.check_table_exists(table_name)
    
    # Backward compatibility methods for institution table
    @staticmethod
    def get_all_institutions() -> pd.DataFrame:
        """Retrieve all institutions from the database"""
        query = """
        SELECT 
            id_institution_cpi,
            institution_cpi,
            institution_cpi_short,
            institution_type_layer1,
            institution_type_layer2,
            institution_type_layer3,
            country_sub,
            country_parent,
            last_verified
        FROM institution
        ORDER BY institution_cpi
        """
        result = DatabaseConnection.execute_query(query)
        
        # Ensure we always return a DataFrame
        if not isinstance(result, pd.DataFrame):
            if isinstance(result, list):
                if len(result) > 0:
                    return pd.DataFrame(result)
                else:
                    return pd.DataFrame(columns=[
                        'id_institution_cpi', 'institution_cpi', 'institution_cpi_short',
                        'institution_type_layer1', 'institution_type_layer2', 'institution_type_layer3',
                        'country_sub', 'country_parent', 'last_verified', 'created_by', 'created_at'
                    ])
            else:
                return pd.DataFrame(columns=[
                    'id_institution_cpi', 'institution_cpi', 'institution_cpi_short',
                    'institution_type_layer1', 'institution_type_layer2', 'institution_type_layer3',
                    'country_sub', 'country_parent', 'last_verified', 'created_by', 'created_at'
                ])
        
        return result
    
    @staticmethod
    def get_countries() -> pd.DataFrame:
        """Get all countries from geography table"""
        query = """
        SELECT DISTINCT 
            country_cpi,
            iso2_code,
            iso3_code
        FROM geography
        ORDER BY country_cpi
        """
        return DatabaseConnection.execute_query(query)
    
    @staticmethod
    def get_institution_by_name(name: str) -> Optional[pd.DataFrame]:
        """Get institution by exact name match"""
        query = """
        SELECT * FROM institution
        WHERE LOWER(institution_cpi) = LOWER(?)
        """
        result = DatabaseConnection.execute_query(query, (name,))
        return result if not result.empty else None
    
    @staticmethod
    def search_institutions_by_prefix(prefix: str, limit: int = 20) -> pd.DataFrame:
        """Search institutions by name prefix for autocomplete"""
        query = f"""
        SELECT 
            institution_cpi,
            institution_type_layer1,
            institution_type_layer2,
            country_sub
        FROM institution
        WHERE LOWER(institution_cpi) LIKE LOWER(?)
        ORDER BY institution_cpi
        LIMIT {limit}
        """
        search_pattern = f"{prefix}%"
        return DatabaseConnection.execute_query(query, (search_pattern,))
    
    @staticmethod
    def check_duplicate_institution(name: str) -> bool:
        """Check if an institution already exists (case-insensitive)"""
        query = """
        SELECT COUNT(*) as count
        FROM institution
        WHERE LOWER(institution_cpi) = LOWER(?)
        """
        result = DatabaseConnection.execute_query(query, (name,))
        return result.iloc[0]['count'] > 0 if not result.empty else False
    
    @staticmethod
    def get_institution_types() -> Dict[str, List[str]]:
        """Get distinct institution types for dropdowns"""
        types = {'layer1': [], 'layer2': [], 'layer3': []}
        
        for layer in ['layer1', 'layer2', 'layer3']:
            query = f"""
            SELECT DISTINCT institution_type_{layer}
            FROM institution
            WHERE institution_type_{layer} IS NOT NULL
            ORDER BY institution_type_{layer}
            """
            result = DatabaseConnection.execute_query(query)
            if not result.empty:
                types[layer] = result[f'institution_type_{layer}'].tolist()
        
        return types
    
    # Generic methods for any table
    @staticmethod
    def get_unique_values(table_name: str, column_name: str) -> List[str]:
        """Get unique values from any table column"""
        query = f"""
        SELECT DISTINCT {column_name}
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        ORDER BY {column_name}
        """
        result = DatabaseConnection.execute_query(query)
        return result[column_name].tolist() if not result.empty else []
    
    @staticmethod
    def search_table_by_field(table_name: str, field_name: str, search_term: str, limit: int = 20) -> pd.DataFrame:
        """Search any table by a specific field"""
        query = f"""
        SELECT *
        FROM {table_name}
        WHERE LOWER({field_name}) LIKE LOWER(?)
        ORDER BY {field_name}
        LIMIT {limit}
        """
        search_pattern = f"%{search_term}%"
        return DatabaseConnection.execute_query(query, (search_pattern,))
    
    @staticmethod
    def get_table_count(table_name: str) -> int:
        """Get row count for any table"""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = DatabaseConnection.execute_query(query)
        return result.iloc[0]['count'] if not result.empty else 0
    
    @staticmethod
    def insert_institution(data: Dict[str, Any]) -> bool:
        """Legacy method for institution insertion"""
        return DatabaseConnection.execute_insert('institution', data)
    
    @staticmethod
    def close_connection():
        """Close the database connection"""
        DatabaseConnection.close_connection()