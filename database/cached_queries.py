"""
Cached database queries for optimal performance
All queries use st.cache_data with appropriate TTL values
"""
import streamlit as st
import pandas as pd
from database.queries import QueryService


@st.cache_data(ttl=7200)  # 2 hours - reference data rarely changes
def get_all_institutions_cached():
    """
    Cached version of get_all_institutions
    Returns all institutions from the database
    Cache expires after 2 hours
    """
    service = QueryService()
    return service.get_all_institutions()


@st.cache_data(ttl=7200)
def get_table_data_cached(table_name: str, limit: int = None) -> pd.DataFrame:
    """Get cached table data for any table"""
    from database.queries import QueryService
    return QueryService.get_table_data(table_name, limit)


@st.cache_data(ttl=7200)  # 2 hours
def get_countries_cached():
    """
    Cached version of get_countries
    Returns all countries from geography table
    Cache expires after 2 hours
    """
    service = QueryService()
    return service.get_countries()


@st.cache_data(ttl=14400)  # 4 hours - dropdown options change very rarely
def get_dropdown_options():
    """
    Load and cache all dropdown options at once
    Returns dictionary with all dropdown option lists
    Cache expires after 4 hours
    
    Returns:
        Dict with keys: type1, type2, type3, countries
    """
    existing_insts = get_all_institutions_cached()
    countries_df = get_countries_cached()
    
    country_list = countries_df['country_cpi'].tolist() if not countries_df.empty else []
    
    # Default options
    type1_options = ['', 'Public', 'Private']
    type2_options = ['', 'Funds', 'Corporation', 'Commercial FI', 'Government', 
                     'Institutional Investors', 'Bilateral DFI', 'SOE', 
                     'Multilateral Climate Funds', 'Multilateral DFI', 
                     'Export Credit Agency (ECA)', 'State-owned FI', 
                     'National DFI', 'Household/Individual', 'Public Fund', 
                     'Third Sector Organisation']
    type3_options = ['', 'Corporate', 'Venture Capital Funds', 'Commercial Bank', 
                     'Infrastructure Funds', 'Subnational Government', 'Pension Fund', 
                     'Private Equity Funds', 'Corporate & Investment Banks', 
                     'Central Government', 'Asset Manager', 'Insurance Company', 
                     'Government Agencies', 'Bank']
    
    # Merge with existing values from database
    if not existing_insts.empty:
        if 'institution_type_layer1' in existing_insts.columns:
            db_values = existing_insts['institution_type_layer1'].dropna().unique().tolist()
            type1_options = [''] + sorted(set(type1_options[1:] + db_values))
        
        if 'institution_type_layer2' in existing_insts.columns:
            db_values = existing_insts['institution_type_layer2'].dropna().unique().tolist()
            type2_options = [''] + sorted(set(type2_options[1:] + db_values))
        
        if 'institution_type_layer3' in existing_insts.columns:
            db_values = existing_insts['institution_type_layer3'].dropna().unique().tolist()
            type3_options = [''] + sorted(set(type3_options[1:] + db_values))
    
    country_options = [''] + sorted(country_list)
    
    return {
        'type1': type1_options,
        'type2': type2_options,
        'type3': type3_options,
        'countries': country_options
    }


@st.cache_resource(ttl=3600)  # 1 hour - fuzzy matcher is expensive to build
def get_fitted_matcher_cached(institutions_hash: str):
    """
    Cache the fitted fuzzy matcher
    Rebuilds every hour or when institution count changes
    
    Args:
        institutions_hash: Hash of institutions (typically the count) for cache key
    
    Returns:
        Fitted matcher object
    """
    from utils.fuzzy_matching import get_fitted_matcher
    
    existing_institutions = get_all_institutions_cached()
    return get_fitted_matcher(existing_institutions, threshold=0.85)