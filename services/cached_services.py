import streamlit as st
from database.queries import QueryService
from services.institution_service import InstitutionService
from services.institution_lookup_service import InstitutionLookupService


class OptimizedServices:
    """Optimized service caching using session state for Docker performance"""
    
    @staticmethod
    def get_query_service():
        """Get cached query service - persists in session state"""
        if 'query_service' not in st.session_state:
            st.session_state.query_service = QueryService()
        return st.session_state.query_service
    
    @staticmethod  
    def get_institution_service():
        """Get cached institution service"""
        if 'institution_service' not in st.session_state:
            st.session_state.institution_service = InstitutionService()
        return st.session_state.institution_service
    
    @staticmethod
    def get_lookup_service():
        """Get cached lookup service with optimized data loading"""
        # Check if service exists and isn't too old
        if ('lookup_service' not in st.session_state or 
            'lookup_service_created' not in st.session_state):
            
            # Use session-cached institutions instead of fresh DB call
            from database.cached_queries import get_all_institutions_cached
            existing_institutions = get_all_institutions_cached()
            
            valid_countries = set()
            if not existing_institutions.empty:
                for col in ['country_sub', 'country_parent']:
                    if col in existing_institutions.columns:
                        valid_countries.update(existing_institutions[col].dropna().unique())
            
            st.session_state.lookup_service = InstitutionLookupService(
                valid_countries=list(valid_countries)
            )
            st.session_state.lookup_service_created = st.session_state.get('lookup_service_created', None)
        
        return st.session_state.lookup_service

        
    @staticmethod
    def get_standardization_service():
        """Get cached standardization service"""
        if 'standardization_service' not in st.session_state:
            from services.standardization_service import StandardizationService
            st.session_state.standardization_service = StandardizationService()
        return st.session_state.standardization_service
    
    @staticmethod
    def get_hierarchy_service():
        """Get cached hierarchy service"""
        if 'hierarchy_service' not in st.session_state:
            from services.hierarchy_service import HierarchyService
            st.session_state.hierarchy_service = HierarchyService()
        return st.session_state.hierarchy_service




# Backward compatibility - drop-in replacements
@st.cache_resource(ttl=None)
def get_query_service():
    """Legacy compatibility - use optimized session state version"""
    return OptimizedServices.get_query_service()

@st.cache_resource(ttl=None)  
def get_institution_service():
    """Legacy compatibility - use optimized session state version"""
    return OptimizedServices.get_institution_service()

@st.cache_resource(ttl=14400)
def get_lookup_service():
    """Legacy compatibility - use optimized session state version"""
    return OptimizedServices.get_lookup_service()