import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from database.queries import QueryService


# Simple, fast session-state only caching - no decorators, no conflicts

def get_all_institutions_cached():
    """Simple session state caching - fast in Docker"""
    cache_key = 'institutions_data_cache'
    time_key = 'institutions_time_cache'
    
    # Check if data exists and is fresh (within 24 hours)
    if cache_key in st.session_state and time_key in st.session_state:
        if datetime.now() - st.session_state[time_key] < timedelta(hours=24):
            return st.session_state[cache_key]
    
    # Load fresh data
    service = QueryService()
    data = service.get_all_institutions()
    
    # Cache in session state
    st.session_state[cache_key] = data
    st.session_state[time_key] = datetime.now()
    
    return data


def get_table_data_cached(table_name: str, limit: int = None) -> pd.DataFrame:
    """Simple session state caching for any table"""
    cache_key = f"table_{table_name}_{limit}"
    time_key = f"table_{table_name}_{limit}_time"
    
    # Check if data exists and is fresh (within 4 hours)
    if cache_key in st.session_state and time_key in st.session_state:
        if datetime.now() - st.session_state[time_key] < timedelta(hours=4):
            return st.session_state[cache_key]
    
    # Load fresh data
    from database.queries import QueryService
    data = QueryService.get_table_data(table_name, limit)
    
    # Cache in session state
    st.session_state[cache_key] = data
    st.session_state[time_key] = datetime.now()
    
    return data


def get_countries_cached():
    """Simple session state caching for countries"""
    cache_key = 'countries_data_cache'
    time_key = 'countries_time_cache'
    
    # Check if data exists and is fresh
    if cache_key in st.session_state and time_key in st.session_state:
        if datetime.now() - st.session_state[time_key] < timedelta(hours=24):
            return st.session_state[cache_key]
    
    # Load fresh data
    service = QueryService()
    data = service.get_countries()
    
    # Cache in session state  
    st.session_state[cache_key] = data
    st.session_state[time_key] = datetime.now()
    
    return data


def get_dropdown_options():
    """Build dropdown options from cached data - no DB calls"""
    cache_key = 'dropdown_options_cache'
    time_key = 'dropdown_options_time'
    
    # Check if options exist and are fresh
    if cache_key in st.session_state and time_key in st.session_state:
        if datetime.now() - st.session_state[time_key] < timedelta(hours=24):
            return st.session_state[cache_key]
    
    # Build from cached data (should not hit DB)
    existing_insts = get_all_institutions_cached()
    countries_df = get_countries_cached()
    
    country_list = countries_df['country_cpi'].tolist() if not countries_df.empty else []
    
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
    
    # Add DB values if available
    if not existing_insts.empty:
        for col, options in [
            ('institution_type_layer1', type1_options),
            ('institution_type_layer2', type2_options), 
            ('institution_type_layer3', type3_options)
        ]:
            if col in existing_insts.columns:
                db_values = existing_insts[col].dropna().unique().tolist()
                options[1:] = sorted(set(options[1:] + db_values))
    
    result = {
        'type1': type1_options,
        'type2': type2_options,
        'type3': type3_options,
        'countries': [''] + sorted(country_list)
    }
    
    # Cache result
    st.session_state[cache_key] = result
    st.session_state[time_key] = datetime.now()
    
    return result


def get_fitted_matcher_cached():
    """Only load fuzzy matcher when actually needed"""
    cache_key = 'fuzzy_matcher_cache'
    time_key = 'fuzzy_matcher_time'
    
    # Check if matcher exists and is fresh
    if cache_key in st.session_state and time_key in st.session_state:
        if datetime.now() - st.session_state[time_key] < timedelta(hours=24):
            return st.session_state[cache_key]
    
    # Build matcher from cached institutions
    try:
        from utils.fuzzy_matching import get_fitted_matcher
        existing_institutions = get_all_institutions_cached()
        matcher = get_fitted_matcher(existing_institutions, threshold=0.85)
        
        # Cache result
        st.session_state[cache_key] = matcher
        st.session_state[time_key] = datetime.now()
        
        return matcher
    except Exception as e:
        print(f"Error building fuzzy matcher: {e}")
        return None


def preload_critical_data():
    """Preload critical data once per session"""
    if st.session_state.get('data_preloaded', False):
        return  # Already loaded
    
    # Load critical data
    get_all_institutions_cached()
    get_countries_cached()
    get_dropdown_options()
    
    st.session_state.data_preloaded = True


def clear_all_data_cache():
    """Clear all cached data"""
    keys_to_remove = []
    for key in st.session_state.keys():
        if any(x in key for x in ['_cache', '_time', 'data_preloaded']):
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del st.session_state[key]