import streamlit as st
import pandas as pd
import pickle
import os
from datetime import datetime, timedelta
from database.queries import QueryService

# Optimized session-based caching for Docker performance
class SessionCache:
    """Session-based cache that doesn't rely on disk I/O - much faster in Docker"""
    
    def __init__(self):
        self._init_cache()
    
    def _init_cache(self):
        if 'session_cache' not in st.session_state:
            st.session_state.session_cache = {
                'data': {},
                'timestamps': {},
                'ttl_hours': {}
            }
    
    def save(self, key, data, ttl_hours=24):
        """Save data to session state with TTL"""
        try:
            self._init_cache()
            st.session_state.session_cache['data'][key] = data
            st.session_state.session_cache['timestamps'][key] = datetime.now()
            st.session_state.session_cache['ttl_hours'][key] = ttl_hours
        except Exception as e:
            print(f"Error saving to session cache: {e}")
    
    def load(self, key):
        """Load data from session state if not expired"""
        try:
            self._init_cache()
            cache = st.session_state.session_cache
            
            if key not in cache['data']:
                return None
            
            timestamp = cache['timestamps'].get(key)
            ttl_hours = cache['ttl_hours'].get(key, 24)
            
            if timestamp and datetime.now() - timestamp > timedelta(hours=ttl_hours):
                # Data expired, remove it
                for cache_dict in cache.values():
                    if key in cache_dict:
                        del cache_dict[key]
                return None
            
            return cache['data'][key]
        except Exception as e:
            print(f"Error loading from session cache: {e}")
            return None

# Global cache instance - drop-in replacement for disk_cache
disk_cache = SessionCache()

@st.cache_data(ttl=86400, show_spinner=False, max_entries=1)
def get_all_institutions_cached():
    """
    Session-optimized institutions cache - loads once per session
    Much faster than disk cache in Docker
    """
    # Check session state first - persistent across interactions
    if 'institutions_cache_data' in st.session_state:
        if 'institutions_cache_time' in st.session_state:
            cache_time = st.session_state.institutions_cache_time
            if datetime.now() - cache_time < timedelta(hours=24):
                return st.session_state.institutions_cache_data
    
    # Load from database if not cached or expired
    service = QueryService()
    data = service.get_all_institutions()
    
    # Store in session state for persistence
    st.session_state.institutions_cache_data = data
    st.session_state.institutions_cache_time = datetime.now()
    
    return data

@st.cache_data(ttl=86400, show_spinner=False)
def get_table_data_cached(table_name: str, limit: int = None) -> pd.DataFrame:
    """Session-optimized table data cache"""
    cache_key = f"table_data_{table_name}_{limit or 'all'}"
    
    # Check session state first
    if cache_key in st.session_state:
        cache_entry = st.session_state[cache_key]
        if isinstance(cache_entry, dict) and 'data' in cache_entry and 'timestamp' in cache_entry:
            if datetime.now() - cache_entry['timestamp'] < timedelta(hours=4):
                return cache_entry['data']
    
    # Load from database
    from database.queries import QueryService
    data = QueryService.get_table_data(table_name, limit)
    
    # Cache in session state
    st.session_state[cache_key] = {
        'data': data,
        'timestamp': datetime.now()
    }
    
    return data

@st.cache_data(ttl=86400, show_spinner=False)
def get_countries_cached():
    """Session-optimized countries cache"""
    # Check session state first
    if 'countries_cache_data' in st.session_state:
        if 'countries_cache_time' in st.session_state:
            cache_time = st.session_state.countries_cache_time
            if datetime.now() - cache_time < timedelta(hours=24):
                return st.session_state.countries_cache_data
    
    # Load from database
    service = QueryService()
    data = service.get_countries()
    
    # Store in session state
    st.session_state.countries_cache_data = data
    st.session_state.countries_cache_time = datetime.now()
    
    return data

@st.cache_data(ttl=86400, show_spinner=False)
def get_dropdown_options():
    """
    Session-optimized dropdown options cache
    Returns dictionary with all dropdown option lists
    """
    # Check session state first for fast access
    if 'dropdown_options_cache' in st.session_state:
        cache_entry = st.session_state.dropdown_options_cache
        if isinstance(cache_entry, dict) and 'timestamp' in cache_entry:
            if datetime.now() - cache_entry['timestamp'] < timedelta(hours=24):
                return cache_entry['options']
    
    # Build options fresh using cached data (avoid new DB calls)
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

@st.cache_resource(ttl=86400, show_spinner=False, max_entries=1) 
def get_fitted_matcher_cached():
    """
    Cache the fitted fuzzy matcher with disk persistence
    Rebuilds every 24 hours - removed hash dependency for performance
    
    Returns:
        Fitted matcher object
    """
    # Try disk cache first
    matcher = disk_cache.load("fuzzy_matcher")
    if matcher is not None:
        return matcher
    
    # Build new matcher
    from utils.fuzzy_matching import get_fitted_matcher
    existing_institutions = get_all_institutions_cached()
    matcher = get_fitted_matcher(existing_institutions, threshold=0.85)
    
    # Save to disk cache
    disk_cache.save("fuzzy_matcher", matcher, ttl_hours=24)
    
    return matcher