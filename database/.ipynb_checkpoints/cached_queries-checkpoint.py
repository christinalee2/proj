import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from database.queries import QueryService


def get_all_institutions_cached():
    """
    Ultra-optimized institutions cache - session state only, no @st.cache_data conflicts
    """
    # Check session state first - persistent across interactions
    if 'institutions_cache_data' in st.session_state:
        if 'institutions_cache_time' in st.session_state:
            cache_time = st.session_state.institutions_cache_time
            if datetime.now() - cache_time < timedelta(hours=24):
                return st.session_state.institutions_cache_data
    
    # Load from database if not cached or expired
    print("Loading institutions from database (should only happen once per session)")
    service = QueryService()
    data = service.get_all_institutions()
    
    # Store in session state for persistence
    st.session_state.institutions_cache_data = data
    st.session_state.institutions_cache_time = datetime.now()
    
    return data


def get_table_data_cached(table_name: str, limit: int = None) -> pd.DataFrame:
    """Ultra-optimized table data cache - session state only"""
    cache_key = f"table_data_{table_name}_{limit or 'all'}"
    
    # Check session state first
    if cache_key in st.session_state:
        cache_entry = st.session_state[cache_key]
        if isinstance(cache_entry, dict) and 'data' in cache_entry and 'timestamp' in cache_entry:
            if datetime.now() - cache_entry['timestamp'] < timedelta(hours=4):
                return cache_entry['data']
    
    # Load from database
    print(f"Loading {table_name} from database (should be rare)")
    from database.queries import QueryService
    data = QueryService.get_table_data(table_name, limit)
    
    # Cache in session state
    st.session_state[cache_key] = {
        'data': data,
        'timestamp': datetime.now()
    }
    
    return data


def get_countries_cached():
    """Ultra-optimized countries cache - session state only"""
    # Check session state first
    if 'countries_cache_data' in st.session_state:
        if 'countries_cache_time' in st.session_state:
            cache_time = st.session_state.countries_cache_time
            if datetime.now() - cache_time < timedelta(hours=24):
                return st.session_state.countries_cache_data
    
    # Load from database
    print("Loading countries from database (should only happen once per session)")
    service = QueryService()
    data = service.get_countries()
    
    # Store in session state
    st.session_state.countries_cache_data = data
    st.session_state.countries_cache_time = datetime.now()
    
    return data


def get_dropdown_options():
    """
    Ultra-optimized dropdown options - session state only, builds from cached data
    """
    # Check session state first for instant access
    if 'dropdown_options_cache' in st.session_state:
        cache_entry = st.session_state.dropdown_options_cache
        if isinstance(cache_entry, dict) and 'timestamp' in cache_entry:
            if datetime.now() - cache_entry['timestamp'] < timedelta(hours=24):
                return cache_entry['options']
    
    # Build options fresh using already-cached data (no new DB calls)
    existing_insts = get_all_institutions_cached()
    countries_df = get_countries_cached()
    
    country_list = countries_df['country_cpi'].tolist() if not countries_df.empty else []
    
    # Base options
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
        for col, options in [
            ('institution_type_layer1', type1_options),
            ('institution_type_layer2', type2_options),
            ('institution_type_layer3', type3_options)
        ]:
            if col in existing_insts.columns:
                db_values = existing_insts[col].dropna().unique().tolist()
                options[1:] = sorted(set(options[1:] + db_values))
    
    country_options = [''] + sorted(country_list)
    
    options = {
        'type1': type1_options,
        'type2': type2_options,
        'type3': type3_options,
        'countries': country_options
    }
    
    # Cache in session state
    st.session_state.dropdown_options_cache = {
        'options': options,
        'timestamp': datetime.now()
    }
    
    return options


def get_fitted_matcher_cached():
    """
    Ultra-optimized fuzzy matcher - only load when actually needed
    """
    # Check if already loaded in session state
    if 'fitted_matcher_cache' in st.session_state:
        if 'fitted_matcher_time' in st.session_state:
            cache_time = st.session_state.fitted_matcher_time
            if datetime.now() - cache_time < timedelta(hours=24):
                return st.session_state.fitted_matcher_cache
    
    print("Building fuzzy matcher (should be rare)")
    # Build new matcher using already-cached institutions
    try:
        from utils.fuzzy_matching import get_fitted_matcher
        existing_institutions = get_all_institutions_cached()
        matcher = get_fitted_matcher(existing_institutions, threshold=0.85)
        
        # Cache in session state
        st.session_state.fitted_matcher_cache = matcher
        st.session_state.fitted_matcher_time = datetime.now()
        
        return matcher
    except Exception as e:
        print(f"Error building fuzzy matcher: {e}")
        return None


def clear_all_caches():
    """Clear all optimized caches"""
    cache_keys = [
        'institutions_cache_data', 'institutions_cache_time',
        'countries_cache_data', 'countries_cache_time', 
        'dropdown_options_cache', 'fitted_matcher_cache', 'fitted_matcher_time'
    ]
    for key in cache_keys:
        if key in st.session_state:
            del st.session_state[key]
    
    # Clear table-specific caches
    keys_to_clear = [key for key in st.session_state.keys() 
                   if key.startswith('table_data_')]
    for key in keys_to_clear:
        del st.session_state[key]
    
    print("All caches cleared")


def warm_all_caches():
    """Pre-load all critical data for instant access"""
    if not st.session_state.get('caches_warmed', False):
        print("Warming all caches...")
        get_all_institutions_cached()
        get_countries_cached()
        get_dropdown_options()
        # Don't warm fuzzy matcher - only load when needed
        st.session_state.caches_warmed = True
        print("All caches warmed")