"""
Generic cached queries that work with any table
Replaces table-specific cached queries with configurable versions
"""
import streamlit as st
import pandas as pd
from typing import Optional, Dict, List
from database.enhanced_connection import EnhancedDatabaseConnection
from table_configs import get_table_config, get_available_tables


@st.cache_data(ttl=7200)  # Cache for 2 hours
def get_table_data_cached(table_name: str, limit: Optional[int] = 1000) -> pd.DataFrame:
    """
    Get cached table data for any table
    
    Args:
        table_name: Name of the table
        limit: Optional limit on rows (None for all rows)
        
    Returns:
        Cached DataFrame with table data
    """
    try:
        return EnhancedDatabaseConnection.get_table_data(table_name, limit)
    except Exception as e:
        print(f"Error loading data for table {table_name}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_dropdown_options_cached(table_name: str) -> Dict[str, List[str]]:
    """
    Get cached dropdown options for select fields in a table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary mapping field names to option lists
    """
    options = {}
    config = get_table_config(table_name)
    
    if not config:
        return options
    
    try:
        # For each select field, get options
        for field_config in config.fields:
            if field_config.field_type == 'select':
                if field_config.options:
                    # Use predefined options
                    options[field_config.name] = field_config.options
                else:
                    # Dynamic options based on field name
                    if field_config.name in ['country_sub', 'country_parent', 'country_cpi']:
                        # Get countries from geography table
                        geo_data = get_table_data_cached('geography')
                        if not geo_data.empty and 'country_cpi' in geo_data.columns:
                            countries = sorted(geo_data['country_cpi'].dropna().unique())
                            options[field_config.name] = [''] + countries
                        else:
                            options[field_config.name] = ['']
                    
                    elif field_config.name.startswith('institution_type_'):
                        # Get institution types from existing data
                        inst_data = get_table_data_cached('institution')
                        if not inst_data.empty and field_config.name in inst_data.columns:
                            types = sorted(inst_data[field_config.name].dropna().unique())
                            options[field_config.name] = [''] + types
                        else:
                            # Fallback to common types
                            if field_config.name == 'institution_type_layer1':
                                options[field_config.name] = ['', 'Public', 'Private']
                            elif field_config.name == 'institution_type_layer2':
                                options[field_config.name] = ['', 'Funds', 'Corporation', 'Commercial FI', 'Government', 'Insurance']
                            elif field_config.name == 'institution_type_layer3':
                                options[field_config.name] = ['', 'Asset Manager', 'Bank', 'Venture Capital Fund', 'Private Equity Fund', 'Insurance Company']
                            else:
                                options[field_config.name] = ['']
                    
                    elif field_config.name == 'currency_code':
                        # Common currency codes
                        options[field_config.name] = ['', 'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'CNY', 'INR']
                    
                    else:
                        # Try to get unique values from the table itself
                        table_data = get_table_data_cached(table_name)
                        if not table_data.empty and field_config.name in table_data.columns:
                            unique_values = sorted(table_data[field_config.name].dropna().unique())
                            options[field_config.name] = [''] + unique_values[:50]  # Limit to 50 options
                        else:
                            options[field_config.name] = ['']
        
        return options
        
    except Exception as e:
        print(f"Error getting dropdown options for {table_name}: {e}")
        return {}


@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_table_summary_cached(table_name: str) -> Dict[str, any]:
    """
    Get cached summary statistics for a table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with summary statistics
    """
    try:
        df = get_table_data_cached(table_name, limit=None)  # Get all data for accurate count
        
        if df.empty:
            return {
                'total_rows': 0,
                'total_columns': 0,
                'last_updated': 'No data',
                'primary_field_unique_count': 0
            }
        
        config = get_table_config(table_name)
        primary_field = config.required_fields[0] if config and config.required_fields else None
        
        summary = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'last_updated': 'Recently',  # Could be enhanced to get actual last modified time
            'primary_field_unique_count': df[primary_field].nunique() if primary_field and primary_field in df.columns else len(df)
        }
        
        # Add field-specific statistics
        if config:
            for field_config in config.fields:
                if field_config.name in df.columns:
                    field_data = df[field_config.name]
                    summary[f'{field_config.name}_non_null'] = field_data.notna().sum()
                    
                    if field_config.field_type in ['text', 'textarea', 'select']:
                        summary[f'{field_config.name}_unique'] = field_data.nunique()
                    elif field_config.field_type == 'number':
                        if field_data.notna().any():
                            summary[f'{field_config.name}_min'] = field_data.min()
                            summary[f'{field_config.name}_max'] = field_data.max()
        
        return summary
        
    except Exception as e:
        print(f"Error getting summary for {table_name}: {e}")
        return {'total_rows': 0, 'total_columns': 0, 'last_updated': 'Error', 'primary_field_unique_count': 0}


@st.cache_data(ttl=14400)  # Cache for 4 hours
def get_all_table_schemas_cached() -> Dict[str, pd.DataFrame]:
    """
    Get cached schema information for all available tables
    
    Returns:
        Dictionary mapping table names to their schema DataFrames
    """
    schemas = {}
    
    for table_name in get_available_tables():
        try:
            schema_df = EnhancedDatabaseConnection.get_table_schema(table_name)
            schemas[table_name] = schema_df
        except Exception as e:
            print(f"Error getting schema for {table_name}: {e}")
            schemas[table_name] = pd.DataFrame()
    
    return schemas


@st.cache_resource
def get_table_service_cached(table_name: str):
    """
    Get cached table service for a specific table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Cached table service instance
    """
    from services.generic_table_service import GenericTableServiceFactory
    return GenericTableServiceFactory.get_service(table_name)


# Convenience functions for backward compatibility
def get_all_institutions_cached() -> pd.DataFrame:
    """Backward compatibility function for institution data"""
    return get_table_data_cached('institution', limit=None)


def get_countries_cached() -> pd.DataFrame:
    """Backward compatibility function for geography data"""
    return get_table_data_cached('geography', limit=None)


# Clear cache functions
def clear_table_cache(table_name: str):
    """Clear cache for a specific table"""
    # Clear all cached functions that depend on this table
    get_table_data_cached.clear()
    get_dropdown_options_cached.clear()
    get_table_summary_cached.clear()
    
    # Also clear any table-specific caches
    if table_name == 'institution':
        try:
            get_all_institutions_cached.clear()
        except:
            pass
    elif table_name == 'geography':
        try:
            get_countries_cached.clear()
        except:
            pass


def clear_all_table_caches():
    """Clear all table-related caches"""
    get_table_data_cached.clear()
    get_dropdown_options_cached.clear()
    get_table_summary_cached.clear()
    get_all_table_schemas_cached.clear()
    
    # Clear backward compatibility caches
    try:
        get_all_institutions_cached.clear()
        get_countries_cached.clear()
    except:
        pass