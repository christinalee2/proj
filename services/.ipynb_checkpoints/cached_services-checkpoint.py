import streamlit as st
from database.queries import QueryService
from services.institution_service import InstitutionService
from services.institution_lookup_service import InstitutionLookupService


@st.cache_resource(ttl=None)  # Never expire - services are stateless
def get_query_service():
    """
    Cached query service instance
    Returns the same QueryService instance across all reruns
    """
    return QueryService()


@st.cache_resource(ttl=None)
def get_institution_service():
    """
    Cached institution service instance
    Returns the same InstitutionService instance across all reruns
    """
    return InstitutionService()


@st.cache_resource(ttl=14400)  # Refreshes every 4 hours, not super necessary
def get_lookup_service():
    """
    Cached lookup service with valid countries
    Rebuilds every hour to pick up new countries
    """
    from database.cached_queries import get_all_institutions_cached
    
    existing_institutions = get_all_institutions_cached()
    
    valid_countries = set()
    if not existing_institutions.empty:
        if 'country_sub' in existing_institutions.columns:
            valid_countries.update(existing_institutions['country_sub'].dropna().unique())
        if 'country_parent' in existing_institutions.columns:
            valid_countries.update(existing_institutions['country_parent'].dropna().unique())
    
    return InstitutionLookupService(valid_countries=list(valid_countries))