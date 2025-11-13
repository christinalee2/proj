import streamlit as st
from database.queries import QueryService
from services.institution_service import InstitutionService
from services.institution_lookup_service import InstitutionLookupService


class OptimizedServices:
    """Ultra-optimized service caching - services created once and never recreated"""
    
    @staticmethod
    def get_query_service():
        """Get cached query service - created once per session"""
        if 'query_service' not in st.session_state:
            print("Creating QueryService (should only happen once)")
            st.session_state.query_service = QueryService()
        return st.session_state.query_service
    
    @staticmethod  
    def get_institution_service():
        """Get cached institution service - created once per session"""
        if 'institution_service' not in st.session_state:
            print("Creating InstitutionService (should only happen once)")
            st.session_state.institution_service = InstitutionService()
        return st.session_state.institution_service
    
    @staticmethod
    def get_lookup_service():
        """Get cached lookup service - created once with pre-cached data"""
        if 'lookup_service' not in st.session_state:
            print("Creating InstitutionLookupService (should only happen once)")
            
            # Use already-cached institutions data (no DB call)
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
        
        return st.session_state.lookup_service
    
    @staticmethod
    def get_standardization_service():
        """Get cached standardization service - created once per session"""
        if 'standardization_service' not in st.session_state:
            print("Creating StandardizationService (should only happen once)")
            from services.standardization_service import StandardizationService
            st.session_state.standardization_service = StandardizationService()
        return st.session_state.standardization_service
    
    @staticmethod
    def get_hierarchy_service():
        """Get cached hierarchy service - created once per session"""
        if 'hierarchy_service' not in st.session_state:
            print("Creating HierarchyService (should only happen once)")
            from services.hierarchy_service import HierarchyService
            st.session_state.hierarchy_service = HierarchyService()
        return st.session_state.hierarchy_service
    
    @staticmethod
    def get_validation_service():
        """Get cached validation service - created once per session"""
        if 'validation_service' not in st.session_state:
            print("Creating ValidationService (should only happen once)")
            from services.validation_service import ValidationService
            st.session_state.validation_service = ValidationService()
        return st.session_state.validation_service
    
    @staticmethod
    def warm_all_services():
        """Pre-initialize all services for instant access"""
        print("Warming all services...")
        UltraOptimizedServices.get_query_service()
        UltraOptimizedServices.get_institution_service()
        UltraOptimizedServices.get_standardization_service()
        UltraOptimizedServices.get_hierarchy_service()
        UltraOptimizedServices.get_validation_service()
        # Don't warm lookup_service as it depends on institutions data
        print("All services warmed and ready")
    
    @staticmethod
    def clear_all_services():
        """Clear all cached services"""
        service_keys = [
            'query_service', 'institution_service', 'lookup_service',
            'standardization_service', 'hierarchy_service', 'validation_service'
        ]
        for key in service_keys:
            if key in st.session_state:
                del st.session_state[key]
        print("All services cleared")


# Backward compatibility functions that now use ultra-optimized versions
def get_query_service():
    """Legacy compatibility - ultra-optimized"""
    return OptimizedServices.get_query_service()

def get_institution_service():
    """Legacy compatibility - ultra-optimized"""
    return OptimizedServices.get_institution_service()

def get_lookup_service():
    """Legacy compatibility - ultra-optimized"""
    return OptimizedServices.get_lookup_service()

def get_standardization_service():
    """Legacy compatibility - ultra-optimized"""
    return OptimizedServices.get_standardization_service()

def get_hierarchy_service():
    """Legacy compatibility - ultra-optimized"""
    return OptimizedServices.get_hierarchy_service()

def get_validation_service():
    """Legacy compatibility - ultra-optimized"""
    return OptimizedServices.get_validation_service()