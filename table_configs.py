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


#Can add any tables you want to these, should produce a clean version 
TABLE_CONFIGS = {
    'institution': TableConfig(
        table_name='institution',
        display_name='Institution',
        description='Enter institution. If institution already exists in database, do not upload again. If there is a non-exact match click Keep to add to standardization table.',
        primary_key_field='id_institution',
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
        ]
    ),
    
    'geography': TableConfig(
        table_name='geography',
        display_name='Geography',
        description='Countries, regions, and geographical classifications',
        primary_key_field='id_geography_cpi',
        required_fields=['country_cpi'],
        duplicate_check_fields=['country_cpi'],
        fields=[
            FieldConfig('country_cpi', 'Country Name', 'text', required=True,
                       help_text='Full country name',
                       placeholder='Enter country name...'),
            FieldConfig('region_cpi', 'Region', 'select', category='main',
                       help_text='Regional classification'),
            FieldConfig('region_cpi_granular', 'Granular Region', 'select', category='main'),
            FieldConfig('region_cpi_additional', 'Additional Region', 'select', category='main'),
            FieldConfig('global_north_south', 'Global North/South', 'select', category='main'),
            FieldConfig('oecd_membership', 'OECD Member', 'select', category='main'),
            FieldConfig('dac_membership', 'DAC Member', 'select', category='main'),
            FieldConfig('income_level', 'Income Level', 'select', category='main'),
            FieldConfig('development_status', 'Development Status', 'select', category='main'),
            FieldConfig('iso2_code', 'ISO2 Code', 'text',
                       help_text='2-letter ISO country code',
                       placeholder='US, GB, etc.', category='main'),
            FieldConfig('iso_numeric_code', 'ISO Numeric Code', 'text',
                       help_text='numeric ISO code', category='main'),
            FieldConfig('iso3_code', 'ISO3 Code', 'text',
                       help_text='3-letter ISO country code',
                       placeholder='USA, GBR, etc.', category='main'),
            FieldConfig('sids', 'Small Island Developing States', 'select', category='main'),
            FieldConfig('lldc', 'Landlocked Developing Countries', 'select', category='main'),
            FieldConfig('unfccc_classification', 'UNFCCC Classification', 'select', category='main'),
            FieldConfig('wb_classification', 'World Bank Classification', 'select', category='main'),
            FieldConfig('r3_ipcc', 'IPCC R3', 'select', category='main'),
            FieldConfig('r6_ipcc', 'IPCC R6', 'select', category='main'),
            FieldConfig('r10_ipcc', 'IPCC R10', 'select', category='main'),
            FieldConfig('development_status', 'CPI Development Status', 'select', category='main'),
            FieldConfig('development_status_2', 'CPI Development Status 2', 'select', category='main'),
            FieldConfig('region_un_m49', 'UN M49 Region', 'select', category='main'),
            FieldConfig('m49_code', 'M49 code', 'select', category='main'),
            FieldConfig('sub_region1_un_m49', 'M49 Sub-region', 'select', category='main'),
        ]
    ),
                
    # 'sector': TableConfig(
    #     table_name='sector',
    #     display_name='Sector',
    #     description='Economic sectors and subsectors for classification',
    #     primary_key_field='sector_key',
    #     required_fields=['sector'],
    #     duplicate_check_fields=['sector'],
    #     fields=[
    #         FieldConfig('sector_key', 'Sector Key', 'text', required=True,
    #                    help_text='Unique identifier for this sector',
    #                    placeholder='Enter sector key...'),
    #         FieldConfig('sector', 'Sector Name', 'text', required=True,
    #                    help_text='Name of the sector',
    #                    placeholder='Enter sector name...'),
    #         FieldConfig('c1', 'Category 1', 'text'),
    #         FieldConfig('sub_sector', 'Sub Sector', 'text',
    #                    help_text='More specific sector classification'),
    #         FieldConfig('c2', 'Category 2', 'text'),
    #         FieldConfig('solution', 'Solution Type', 'text',
    #                    help_text='Type of climate solution'),
    #         FieldConfig('c3', 'Category 3', 'text'),
    #         FieldConfig('re', 'Renewable Energy', 'select',
    #                    options=['', 'True', 'False'],
    #                    help_text='Is this renewable energy related?'),
    #         FieldConfig('ff', 'Fossil Fuel', 'select',
    #                    options=['', 'True', 'False']),
    #         FieldConfig('ee', 'Energy Efficiency', 'select',
    #                    options=['', 'True', 'False']),
    #         FieldConfig('og', 'Oil & Gas', 'select',
    #                    options=['', 'True', 'False']),
    #         FieldConfig('lt', 'Low Temperature', 'select',
    #                    options=['', 'True', 'False']),
    #         FieldConfig('mi', 'Mitigation', 'select',
    #                    options=['', 'True', 'False']),
    #         FieldConfig('ad', 'Adaptation', 'select',
    #                    options=['', 'True', 'False']),
    #     ]
    # ),
    
    'instrument': TableConfig(
        table_name='instrument',
        display_name='Instrument',
        description='Instruments',
        primary_key_field='id_instrument',
        required_fields=['original_name', 'instrument_type'],
        duplicate_check_fields=['original_name'],
        fields=[
            FieldConfig('instrument_original', 'Original Instrument Name (from source)', 'text', category='main',
                       help_text='Type of financial instrument',
                       placeholder='Enter instrument name...'),
            FieldConfig('instrument_type', 'Instrument Type', 'text', category='main',
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
        ]
    ),

    'gearing': TableConfig(
        table_name='gearing',
        display_name='Gearing Ratios',
        description='Gearing ratios',
        primary_key_field='id_gearing',
        required_fields=['gearing', 'sector_re', 'country_cpi', 'region_cpi', 'source'],
        duplicate_check_fields=['gearing', 'sector_re', 'country_cpi', 'region_cpi', 'source'],
        fields=[
            FieldConfig('sector_re', 'Sector', 'select', category='main',
                       help_text='Sector'),
            FieldConfig('country_cpi', 'Country', 'text', category='main',
                       help_text='Country'),
            FieldConfig('region_cpi', 'Region', 'select', category='main'),
            FieldConfig('gearing', 'Gearing Ratio', 'text', category='main'),
            FieldConfig('source', 'Source', 'test', category='main'),
        ]
    ),

    'multiplier': TableConfig(
        table_name='multiplier',
        display_name='Multipliers',
        description='Multipliers',
        primary_key_field='id_multiplier',
        required_fields=['multiplier_local', 'sub_sector_source'],
        duplicate_check_fields=['multiplier_local', 'sub_sector_source'],
        fields=[
            FieldConfig('multiplier_local', 'Multiplier', 'text', category='main'),
            FieldConfig('sub_sector_source', 'Sub-sector name (from source)', 'text', category='main'),
            FieldConfig('sub_sector_bnef', 'Sub-sector name (BNEF)', 'select', category='main'),
            FieldConfig('country_cpi', 'Country', 'text', category='main'),
            FieldConfig('region_cpi', 'Region', 'select', category='main'),
            FieldConfig('currency', 'Currency', 'text', category='main'),
            FieldConfig('conversion', 'Conversion', 'select', category='main'),
            FieldConfig('multiplier_usd', 'Multiplier', 'text', required=True),
            FieldConfig('data_source_type', 'Data Source', 'select', category='main'),
            FieldConfig('notes', 'Notes', 'text', category='main'),
        ]
    ),
    
    # 'data_source': TableConfig(
    #     table_name='data_source',
    #     display_name='Data Source',
    #     description='Sources of data and their classifications',
    #     primary_key_field='data_source_code',
    #     required_fields=['data_source_name'],
    #     duplicate_check_fields=['data_source_code', 'data_source_name'],
    #     fields=[
    #         FieldConfig('data_source_name', 'Data Source Name', 'text', required=True,
    #                    help_text='Name of the data source',
    #                    placeholder='Enter data source name...'),
    #         FieldConfig('data_source_code', 'Data Source Code', 'text',
    #                    help_text='Unique code for this data source',
    #                    placeholder='Enter code...'),
    #     ]
    # ),
    
    'exchange_rates': TableConfig(
        table_name='exchange_rates',
        display_name='Exchange Rates',
        description='Currency exchange rates by country and year',
        primary_key_field='id_fx',
        required_fields=['country_cpi', 'currency_code', 'year', 'fx_rate'],
        duplicate_check_fields=['country_cpi', 'currency_code', 'year'],
        fields=[
            FieldConfig('country_cpi', 'Country', 'select',
                       options=[],  # Will be populated from geography table
                       required=True,
                       help_text='Country for this exchange rate'),
            FieldConfig('currency_code', 'Currency Code', 'text', required=True,
                       help_text='3-letter currency code (USD, EUR, etc.)',
                       placeholder='USD, EUR, GBP...', category='main'),
            FieldConfig('fx_rate', 'Exchange Rate', 'number', required=True,
                       validation_fn=validate_decimal,
                       help_text='Exchange rate to USD', category='main'),
        ]
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
        ]
    ),
    
    'geography_standardization': TableConfig(
        table_name='geography_standardization',
        display_name='Geography Standardization',
        description='Standardization table for geography list. Typically no need to edit.',
        primary_key_field='id_geography',
        required_fields=['country_original'],
        duplicate_check_fields=['country_cpi', 'country_original'],
        fields=[
            FieldConfig('country_original', 'Country Name', 'text', required=True,
                       help_text='Full name of the country',
                       placeholder='Enter country name...'),
            FieldConfig('country_cpi', 'Country Standardized Name', 'select', category='main',
                       help_text='Only enter if you know the mapping for standardization already or adding a new country.'),
        ]
    ),
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

