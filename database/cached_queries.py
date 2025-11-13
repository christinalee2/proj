import streamlit as st
import pandas as pd
import pickle
import os
from datetime import datetime, timedelta
from database.queries import QueryService

# Enhanced disk caching for Docker persistence
class DiskCache:
    def __init__(self, cache_dir="/tmp/streamlit_cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    def save(self, key, data, ttl_hours=24):
        """Save data to disk with TTL"""
        try:
            cache_file = os.path.join(self.cache_dir, f"{key}.pkl")
            expiry = datetime.now() + timedelta(hours=ttl_hours)
            cache_data = {'data': data, 'expiry': expiry}
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
        except Exception as e:
            print(f"Error saving to disk cache: {e}")
    
    def load(self, key):
        """Load data from disk if not expired"""
        try:
            cache_file = os.path.join(self.cache_dir, f"{key}.pkl")
            if not os.path.exists(cache_file):
                return None
            
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
            
            if datetime.now() > cache_data['expiry']:
                os.remove(cache_file)
                return None
            
            return cache_data['data']
        except Exception as e:
            print(f"Error loading from disk cache: {e}")
            return None

# Global cache instance
disk_cache = DiskCache()

@st.cache_data(ttl=86400, show_spinner=False, max_entries=1)  # 24 hours, no spinner
def get_all_institutions_cached():
    """
    Cached version of get_all_institutions with disk persistence
    Returns all institutions from the database
    Cache expires after 24 hours
    """
    # Try disk cache first
    data = disk_cache.load("institutions_data")
    if data is not None:
        return data
    
    # Load from database if not in disk cache
    service = QueryService()
    data = service.get_all_institutions()
    
    # Save to disk cache
    disk_cache.save("institutions_data", data, ttl_hours=24)
    
    return data

@st.cache_data(ttl=86400, show_spinner=False)  # 24 hours, no spinner  
def get_table_data_cached(table_name: str, limit: int = None) -> pd.DataFrame:
    """Get cached table data for any table with disk persistence"""
    cache_key = f"table_data_{table_name}_{limit or 'all'}"
    
    # Try disk cache first
    data = disk_cache.load(cache_key)
    if data is not None:
        return data
    
    # Load from database
    from database.queries import QueryService
    data = QueryService.get_table_data(table_name, limit)
    
    # Save to disk cache
    disk_cache.save(cache_key, data, ttl_hours=24)
    
    return data

@st.cache_data(ttl=86400, show_spinner=False) 
def get_countries_cached():
    """
    Cached version of get_countries with disk persistence
    Returns all countries from geography table
    Cache expires after 24 hours
    """
    # Try disk cache first
    data = disk_cache.load("countries_data")
    if data is not None:
        return data
    
    # Load from database
    service = QueryService()
    data = service.get_countries()
    
    # Save to disk cache
    disk_cache.save("countries_data", data, ttl_hours=24)
    
    return data

@st.cache_data(ttl=86400, show_spinner=False)  # 24 hours - dropdown options don't change often
def get_dropdown_options():
    """
    Load and cache all dropdown options at once with disk persistence
    Returns dictionary with all dropdown option lists
    
    Returns:
        Dict with keys: type1, type2, type3, countries
    """
    # Try disk cache first
    options = disk_cache.load("dropdown_options")
    if options is not None:
        return options
    
    # Build options fresh
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
    
    options = {
        'type1': type1_options,
        'type2': type2_options,
        'type3': type3_options,
        'countries': country_options
    }
    
    # Save to disk cache
    disk_cache.save("dropdown_options", options, ttl_hours=24)
    
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