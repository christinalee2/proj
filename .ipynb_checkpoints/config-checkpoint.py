import os
from typing import Dict, List
from datetime import datetime
import streamlit as st 


def get_env_var(key: str, default: str = '') -> str:
    """
    Get environment variable from .env file (local ver) or Streamlit secrets (cloud ver)
    """
    value = os.getenv(key)
    if value:
        return value
    
    try:
        return st.secrets[key]
    except (KeyError, AttributeError):
        return default

AWS_REGION = get_env_var('AWS_REGION', 'us-east-1')
S3_BUCKET = get_env_var('S3_BUCKET', 'cpi-uk-us-datascience-stage')
ATHENA_DATABASE = get_env_var('ATHENA_DATABASE', 'ref_testing_2')
ATHENA_OUTPUT_LOCATION = get_env_var('ATHENA_OUTPUT_LOCATION', f's3://{S3_BUCKET}/auxiliary-data/reference-data/reference-db-2/athena-query-results/')

OPENAI_API_KEY = get_env_var('OPENAI_API_KEY', '')

AWS_ACCESS_KEY_ID = get_env_var('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = get_env_var('AWS_SECRET_ACCESS_KEY', '')

CURRENT_YEAR = int(datetime.now().year)
FUZZY_MATCH_THRESHOLD = 85
MAX_BULK_UPLOAD_ROWS = 1000

AUDIT_FIELDS = ['created_by', 'created_at']

YEAR_FIELD_PATTERNS = ['last_verified', 'year', 'year_added']


def should_auto_populate_year(field_name: str) -> bool:
    """Check if a field should be auto-populated with current year"""
    field_lower = field_name.lower()
    return any(pattern in field_lower for pattern in YEAR_FIELD_PATTERNS)

def get_audit_data(username: str) -> Dict[str, str]:
    """
    Get audit data (created at, created by)
    
    Args:
        username: Username performing the operation
        
    Returns:
        Dictionary with audit fields
    """
    current_time = CURRENT_YEAR
    
    return {
        'created_by': username,
        'created_at': CURRENT_YEAR,
    }
