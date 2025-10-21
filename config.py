import os
from typing import Dict, List
from datetime import datetime


def get_env_var(key: str, default: str = '') -> str:
    """
    Get environment variable from .env file (local) or Streamlit secrets (cloud)
    
    Args:
        key: Environment variable name
        default: Default value if not found
        
    Returns:
        Environment variable value
    """
    # First try environment variables (from .env locally)
    value = os.getenv(key)
    if value:
        return value
    
    # Then try Streamlit secrets (for cloud deployment)
    try:
        return st.secrets[key]
    except (KeyError, AttributeError):
        return default

AWS_REGION = get_env_var('AWS_REGION', 'us-east-1')
S3_BUCKET = get_env_var('S3_BUCKET', 'cpi-uk-us-datascience-stage')
ATHENA_DATABASE = get_env_var('ATHENA_DATABASE', 'ref_testing')
ATHENA_OUTPUT_LOCATION = get_env_var('ATHENA_OUTPUT_LOCATION', f's3://{S3_BUCKET}/auxiliary-data/reference-data/reference-db/athena-query-results/')

OPENAI_API_KEY = get_env_var('OPENAI_API_KEY', '')

AWS_ACCESS_KEY_ID = get_env_var('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = get_env_var('AWS_SECRET_ACCESS_KEY', '')

CURRENT_YEAR = datetime.now().year
FUZZY_MATCH_THRESHOLD = 85
MAX_BULK_UPLOAD_ROWS = 1000

SUFFIX_MAPPINGS: Dict[str, Dict[str, str]] = {
    'layer1': {
        'llc': 'Private',
        'ltd': 'Private',
        'limited': 'Private',
        'inc': 'Private',
        'incorporated': 'Private',
        'corp': 'Private',
        'corporation': 'Private',
        'gmbh': 'Private',
        'sarl': 'Private',
        'srl': 'Private',
        'pvt': 'Private',
        'pty': 'Private',
        'pte': 'Private',
        'bv': 'Private',
        'nv': 'Private',
        'ag': 'Private',
        'sa': 'Private',
        'sas': 'Private',
        'ab': 'Private',
        
        # Public indicators
        'plc': 'Public',
        'public limited company': 'Public',
        'se': 'Public',
        'oyj': 'Public',
        'spa': 'Public',
        'public': 'Public',
    },
    'layer2': {
        'bank': 'Commercial FI',
        'banking': 'Commercial FI',
        'bancorp': 'Commercial FI',
        'credit union': 'Commercial FI',
        'savings': 'Commercial FI',
        'trust': 'Commercial FI',
        
        'fund': 'Funds',
        'funds': 'Funds',
        'capital': 'Funds',
        'investment': 'Funds',
        'investments': 'Funds',
        'ventures': 'Funds',
        'partners': 'Funds',
        'asset management': 'Funds',
        
        'insurance': 'Insurance',
        'assurance': 'Insurance',
        'reinsurance': 'Insurance',
        
        'corporation': 'Corporation',
        'corp': 'Corporation',
        'company': 'Corporation',
        'industries': 'Corporation',
        'group': 'Corporation',
    },
    'layer3': {
        'asset management': 'Asset Manager',
        'investment management': 'Asset Manager',
        
        'venture': 'Venture Capital Fund',
        'ventures': 'Venture Capital Fund',
        'vc': 'Venture Capital Fund',
        
        'private equity': 'Private Equity Fund',
        'equity partners': 'Private Equity Fund',
        
        # Pension Funds
        'pension': 'Pension Fund',
        'retirement': 'Pension Fund',
        
        # Banks
        'bank': 'Bank',
        'banking': 'Bank',
        'bancorp': 'Bank',
        
        # Insurance
        'insurance': 'Insurance Company',
        'assurance': 'Insurance Company',
    }
}

# Table Schemas - for reference and validation
TABLE_SCHEMAS = {
    'institution': {
        'required_fields': ['institution_cpi', 'last_verified'],
        'optional_fields': [
            'id_institution',
            'institution_cpi_short',
            'institution_type_layer1',
            'institution_type_layer2',
            'institution_type_layer3',
            'double_counting_risk',
            'country_sub',
            'country_parent',
            'contact_info',
            'comments'
        ]
    },
    'geography': {
        'required_fields': ['id_geography_cpi', 'country_cpi'],
        'optional_fields': [
            'region_cpi', 'region_cpi_granular', 'region_cpi_additional',
            'year_added', 'oecd_membership', 'dac_membership',
            'income_level', 'unfccc_classification', 'wb_classification',
            'r3_ipcc', 'r6_ipcc', 'r10_ipcc', 'development_status',
            'development_status_2', 'sids', 'lldc', 'region_un_m49',
            'sub_region1_un_m49', 'sub_region2_un_m49', 'm49_code',
            'iso2_code', 'iso_numeric_code', 'iso3_code', 'global_north_south'
        ]
    },
    'sector': {
        'required_fields': ['sector_key', 'sector'],
        'optional_fields': [
            'c1', 'sub_sector', 'c2', 'solution', 'c3',
            're', 'ff', 'ee', 'og', 'lt', 'mi', 'ad'
        ]
    },
    'instrument': {
        'required_fields': ['id_instrument', 'instrument_type'],
        'optional_fields': [
            'instrument_type_layer2', 'instrument_concessional',
            'definition', 'info_quality', 'categorization',
            'description', 'example'
        ]
    }
}

AVAILABLE_TABLES = [
    'institution',
    'geography',
    'sector',
    'instrument',
    'gender',
    'data_source',
    'recipient',
    'country_coefficients',
    'exchange_rates',
    'state_control'
]

ENRICHMENT_PROMPT_TEMPLATE = """
You are a financial data analyst. Given the institution name "{institution_name}", provide:
1. Whether it's a Public or Private entity
2. The type of institution (e.g., Commercial FI, Funds, Corporation, Insurance, etc.)
3. More specific classification (e.g., Asset Manager, Bank, Venture Capital Fund, etc.)
4. Primary operating country (ISO country code if possible)
5. Headquarters country (ISO country code if possible)
6. Brief description (1-2 sentences)
7. Reliable source URLs for verification (official website, LinkedIn, etc.)

Format your response as JSON:
{{
    "institution_type_layer1": "Public/Private",
    "institution_type_layer2": "Category",
    "institution_type_layer3": "Specific Type",
    "country_sub": "Country Code",
    "country_parent": "Country Code",
    "description": "Brief description",
    "sources": ["url1", "url2"]
}}

If you cannot determine any field with confidence, use null for that field.
"""