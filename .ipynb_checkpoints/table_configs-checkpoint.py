from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pandas as pd


@dataclass
class FieldConfig:
    """Configuration for a single field"""
    name: str
    display_name: str
    field_type: str  # 'text', 'select', 'number', 'boolean', 'textarea'
    required: bool = False
    options: Optional[List[str]] = None  
    help_text: Optional[str] = None
    placeholder: Optional[str] = None
    validation_fn: Optional[callable] = None
    category: str = 'main', 'advanced'


@dataclass
class TableConfig:
    """Configuration for a table upload interface"""
    table_name: str
    display_name: str
    description: str
    primary_key_field: str
    required_fields: List[str]
    fields: List[FieldConfig]
    has_single_entry: bool = True
    has_bulk_upload: bool = True
    custom_validation_fn: Optional[callable] = None
    duplicate_check_fields: Optional[List[str]] = None


def validate_year(value: Any) -> bool:
    """Validate year is reasonable"""
    if not value:
        return True
    try:
        year = int(value)
        return 1900 <= year <= 2100
    except:
        return False


def validate_decimal(value: Any) -> bool:
    """Validate decimal format"""
    if not value:
        return True
    try:
        float(value)
        return True
    except:
        return False


def validate_boolean(value: Any) -> bool:
    """Validate boolean values"""
    if value is None or value == '':
        return True
    return str(value).lower() in ['true', 'false', '1', '0', 'yes', 'no']


AUDIT_FIELDS = [
    FieldConfig('created_by', 'Created By', 'text', category='audit',
               help_text='Username who created this record'),
    FieldConfig('created_at', 'Created At', 'number', category='audit',
               help_text='Timestamp when record was created'),
]


#Can add any tables you want to these, should produce a clean version 
TABLE_CONFIGS = {
    'institution': TableConfig(
        table_name='institution',
        display_name='Institution',
        description='Enter institution. If institution already exists in database, do not upload again. If there is a non-exact match click Keep to add to standardization table.',
        primary_key_field='id_institution_cpi',
        required_fields=['institution_cpi'],
        duplicate_check_fields=['institution_cpi'],
        fields=[
            FieldConfig('institution_cpi', 'Institution Name', 'text', required=True,
                       help_text='Full name of the institution',
                       placeholder='Enter institution name...'),
            FieldConfig('institution_type_layer1', 'Type Layer 1', 'select', category='main',
                       help_text='Public or Private classification'),
            FieldConfig('institution_type_layer2', 'Type Layer 2', 'select', category='main'),
            FieldConfig('institution_type_layer3', 'Type Layer 3', 'select', category='main'),
            FieldConfig('country_sub', 'Subsidiary Country', 'select', category='main',
                       help_text='Country where institution operates'),
            FieldConfig('country_parent', 'Parent Country', 'select', category='main',
                       help_text='Country where headquarters is located'),
            FieldConfig('double_counting_risk', 'Double Counting Risk', 'select', category='main'),
            
            FieldConfig('institution_cpi_short', 'Short Name', 'text', category='advanced',
                       help_text='Abbreviated name (for known acronyms/shortened forms)'),
            FieldConfig('contact_info', 'Contact Information', 'textarea',category='advanced',
                       help_text='Contact details, website, etc.'),
            FieldConfig('comments', 'Comments', 'textarea',category='advanced',
                       help_text='Additional notes or comments'),
        ] + AUDIT_FIELDS
    ),
    
    # 'geography': TableConfig(
    #     table_name='geography',
    #     display_name='Geography',
    #     description='Countries, regions, and geographical classifications',
    #     primary_key_field='id_geography_cpi',
    #     required_fields=['country_cpi'],
    #     duplicate_check_fields=['country_cpi'],
    #     fields=[
    #         FieldConfig('country_cpi', 'Country Name', 'text', required=True,
    #                    help_text='Full country name',
    #                    placeholder='Enter country name...'),
    #         FieldConfig('region_cpi', 'Region', 'select', category='main',
    #                    help_text='Regional classification'),
    #         FieldConfig('region_cpi_granular', 'Granular Region', 'select', category='main'),
    #         FieldConfig('region_cpi_additional', 'Additional Region', 'select', category='main'),
    #         FieldConfig('global_north_south', 'Global North/South', 'select', category='main'),
    #         FieldConfig('oecd_membership', 'OECD Member', 'select', category='main'),
    #         FieldConfig('dac_membership', 'DAC Member', 'select', category='main'),
    #         FieldConfig('income_level', 'Income Level', 'select', category='main'),
    #         FieldConfig('development_status', 'Development Status', 'select', category='main'),
    #         FieldConfig('iso2_code', 'ISO2 Code', 'text',
    #                    help_text='2-letter ISO country code',
    #                    placeholder='US, GB, etc.', category='main'),
    #         FieldConfig('iso_numeric_code', 'ISO Numeric Code', 'text',
    #                    help_text='numeric ISO code', category='main'),
    #         FieldConfig('iso3_code', 'ISO3 Code', 'text',
    #                    help_text='3-letter ISO country code',
    #                    placeholder='USA, GBR, etc.', category='main'),
    #         FieldConfig('sids', 'Small Island Developing States', 'select', category='main'),
    #         FieldConfig('lldc', 'Landlocked Developing Countries', 'select', category='main'),
    #         FieldConfig('unfccc_classification', 'UNFCCC Classification', 'select', category='main'),
    #         FieldConfig('wb_classification', 'World Bank Classification', 'select', category='main'),
    #         FieldConfig('r3_ipcc', 'IPCC R3', 'select', category='main'),
    #         FieldConfig('r6_ipcc', 'IPCC R6', 'select', category='main'),
    #         FieldConfig('r10_ipcc', 'IPCC R10', 'select', category='main'),
    #         FieldConfig('development_status', 'CPI Development Status', 'select', category='main'),
    #         FieldConfig('development_status_2', 'CPI Development Status 2', 'select', category='main'),
    #         FieldConfig('region_un_m49', 'UN M49 Region', 'select', category='main'),
    #         FieldConfig('m49_code', 'M49 code', 'number', category='main'),
    #         FieldConfig('sub_region1_un_m49', 'M49 Sub-region', 'select', category='main'),
    #         FieldConfig('year_added', 'Year Added', 'number')  
    #     ] + AUDIT_FIELDS
    # ),

    
    'instrument': TableConfig(
        table_name='instrument',
        display_name='Instrument',
        description='Instruments',
        primary_key_field='id_instrument',
        required_fields=['original_name'],
        duplicate_check_fields=['original_name'],
        fields=[
            FieldConfig('original_name', 'Original Instrument Name (from source)', 'text', required=True,
                       help_text='Type of financial instrument',
                       placeholder='Enter instrument name...'),
            FieldConfig('instrument_type', 'Instrument Type', 'select', category='main',
                       help_text='Type of financial instrument',
                       placeholder='Enter instrument type...'),
            FieldConfig('instrument_type_layer2', 'Type Layer 2', 'select', category='main'),
            FieldConfig('definition', 'Definition', 'textarea',
                       help_text='Clear definition of this instrument', category='main'),
            FieldConfig('info_quality', 'Information Quality', 'text', category='main',
                       help_text='Status of information'),
            FieldConfig('categorization', 'Categorization', 'select', category='main'),
            FieldConfig('description', 'Description', 'textarea',
                       help_text='Detailed description', category='main'),
            FieldConfig('example', 'Example', 'textarea',
                       help_text='Example of this instrument in use', category='main'),
        ] + AUDIT_FIELDS
    ),

    'gearing': TableConfig(
        table_name='gearing',
        display_name='Gearing Ratios',
        description='Gearing ratios',
        primary_key_field='id_gearing',
        required_fields=['sector_re'],
        duplicate_check_fields=['gearing', 'sector_re', 'country_cpi', 'region_cpi', 'source'],
        fields=[
            FieldConfig('sector_re', 'Sector', 'select', category='main', required=True,
                       help_text='Sector'),
            FieldConfig('country_cpi', 'Country', 'text', category='main',
                       help_text='Country'),
            FieldConfig('region_cpi', 'Region', 'select', category='main'),
            FieldConfig('gearing', 'Gearing Ratio', 'number', category='main'),
            FieldConfig('source', 'Source', 'test', category='main'),
            FieldConfig('last_verified', 'Last verified', 'number')  
        ] + AUDIT_FIELDS
    ),

    'multiplier': TableConfig(
        table_name='multiplier',
        display_name='Multipliers',
        description='Multipliers',
        primary_key_field='id_multiplier',
        required_fields=['sub_sector_source'],
        duplicate_check_fields=['multiplier_local', 'sub_sector_source'],
        fields=[
            FieldConfig('sub_sector_source', 'Sub-sector name (from source)', 'text', required=True),
            FieldConfig('multiplier_local', 'Local Multiplier', 'number', category='main'),
            FieldConfig('sub_sector_bnef', 'Sub-sector name (BNEF)', 'select', category='main'),
            FieldConfig('country_cpi', 'Country', 'text', category='main'),
            FieldConfig('region_cpi', 'Region', 'select', category='main'),
            FieldConfig('currency', 'Currency', 'select', category='main'),
            FieldConfig('conversion_rate', 'Conversion', 'select', category='main'),
            FieldConfig('multiplier_usd', 'Multiplier in USD', 'number', category='main'),
            FieldConfig('data_source_type', 'Data Source', 'select', category='main'),
            FieldConfig('notes', 'Notes', 'text', category='main'),
            FieldConfig('last_verified', 'Last verified', 'number'),
            FieldConfig('year_of_analysis', 'Year of analysis', 'number')  
        ] + AUDIT_FIELDS
    ),

    
    'exchange_rates': TableConfig(
        table_name='exchange_rates',
        display_name='Exchange Rates',
        description='Currency exchange rates by country and year',
        primary_key_field='id_fx',
        required_fields=['country_cpi'],
        duplicate_check_fields=['country_cpi', 'currency_code', 'year'],
        fields=[
            FieldConfig('country_cpi', 'Country', 'select',
                       options=[],  # Will be populated from geography table
                       required=True,
                       help_text='Country for this exchange rate'),
            FieldConfig('currency_code', 'Currency Code', 'text', category='main',
                       help_text='3-letter currency code (USD, EUR, etc.)',
                       placeholder='USD, EUR, GBP...'),
            FieldConfig('fx_rate', 'Exchange Rate', 'number', category='main',
                       validation_fn=validate_decimal,
                       help_text='Exchange rate to USD'),
            FieldConfig('year', 'Year', 'number')  
        ] + AUDIT_FIELDS
    ), 
    
    'institution_standardization': TableConfig(
        table_name='institution_standardization',
        display_name='Institution Standardization',
        description='Standardization table for institutions. Typically no need to edit.',
        primary_key_field='id_institution',
        required_fields=['institution_original'],
        duplicate_check_fields=['institution_cpi', 'institution_original'],
        fields=[
            FieldConfig('institution_original', 'Institution Name', 'text', required=True,
                       help_text='Full name of the institution',
                       placeholder='Enter institution name...'),
            FieldConfig('institution_cpi', 'Institution Standardized Name', 'select', category='main',
                       help_text='Only enter if you know the mapping for standardization already or adding a new institution.'),
        ] + AUDIT_FIELDS
    ),
    
    # 'geography_standardization': TableConfig(
    #     table_name='geography_standardization',
    #     display_name='Geography Standardization',
    #     description='Standardization table for geography list. Typically no need to edit.',
    #     primary_key_field='id_geography',
    #     required_fields=['country_original'],
    #     duplicate_check_fields=['country_cpi', 'country_original'],
    #     fields=[
    #         FieldConfig('country_original', 'Country Name', 'text', required=True,
    #                    help_text='Full name of the country',
    #                    placeholder='Enter country name...'),
    #         FieldConfig('country_cpi', 'Country Standardized Name', 'select', category='main',
    #                    help_text='Only enter if you know the mapping for standardization already or adding a new country.'),
    #     ] + AUDIT_FIELDS
    # ),
}


def get_table_config(table_name: str) -> Optional[TableConfig]:
    """Get configuration for a specific table"""
    return TABLE_CONFIGS.get(table_name)


def get_available_tables() -> List[str]:
    """Get list of available table names"""
    return list(TABLE_CONFIGS.keys())


def get_table_display_names() -> Dict[str, str]:
    """Get mapping of table names to display names"""
    return {name: config.display_name for name, config in TABLE_CONFIGS.items()}

