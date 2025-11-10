from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pandas as pd


@dataclass
class FieldConfig:
    """Configuration for a single field in form
    - have to be careful when setting field type, make sure it matches parquet schema otherwise will cause issues
    - name is name of column in table, display_name is what you want it to show up as (institution_type_layer1 vs Type Layer 1)
    """
    name: str
    display_name: str
    field_type: str  # 'text', 'select', 'number', 'boolean', 'textarea'
    required: bool = False
    options: Optional[List[str]] = None  
    help_text: Optional[str] = None
    detailed_help: Optional[str] = None
    placeholder: Optional[str] = None
    validation_fn: Optional[callable] = None
    category: str = 'main', 'advanced'


@dataclass
class TableConfig:
    """Configuration for a table upload interface - this is for a whole new table"""
    table_name: str
    display_name: str
    description: str
    general_description: str 
    primary_key_field: str
    required_fields: List[str]
    fields: List[FieldConfig]
    has_single_entry: bool = True
    has_bulk_upload: bool = True
    custom_validation_fn: Optional[callable] = None
    duplicate_check_fields: Optional[List[str]] = None
    has_standardization: bool = False


@dataclass
class ColumnTypeConfig:
    """Configuration for column data types for AWS Wrangler, useful to cast explicitly"""
    string_columns: List[str]
    integer_columns: List[str]
    float_columns: Optional[List[str]] = None
    boolean_columns: Optional[List[str]] = None


def validate_year(value: Any) -> bool:
    if not value:
        return True
    try:
        year = int(value)
        return 1900 <= year <= 2100
    except:
        return False


def validate_decimal(value: Any) -> bool:
    if not value:
        return True
    try:
        float(value)
        return True
    except:
        return False


def validate_boolean(value: Any) -> bool:
    if value is None or value == '':
        return True
    return str(value).lower() in ['true', 'false', '1', '0', 'yes', 'no']


AUDIT_FIELDS = [
    FieldConfig('created_by', 'Created By', 'text', category='audit',
               help_text='Username who created this record'),
    FieldConfig('created_at', 'Created At', 'number', category='audit',
               help_text='Timestamp when record was created'),
]




#Can add any tables you want to these - this will add a new table to the dropdown list of choices 
#General_description will show up right under when the user chooses their table (step 1)
#Description shows up under the "Add New ___"
#Help_text is for fields that don't require a lot of extra help (just a little ?)
#Detailed_help is for fields that need a lot of documentation (expandable toggle), can insert tables in markdown, generally have one or the other 
TABLE_CONFIGS = {
    'institution': TableConfig(
        table_name='institution',
        display_name='Institution',
        has_standardization=True,
        description="First, check to see if the original institution name currently exists in the reference table. If there is an 'institution already exists' message, you do not need to update this institution. If the original institution name does not currently exist in the Institution (Standardized Name) reference table, you will either need to map the original institution name to an existing standardized name or enter a new standardized name if no existing option is suitable. ",
        general_description="""Institution is a two-piece reference table that operates using two datasets:
        
Institution (Classifications): Classifies standardised institution names across:  
a) Institution type layer 1: distinguishes between public and private institutions.  
b) Institution type layer 2: provides further categorisation to layer 1, such as corporations, commercial financial institutions, or private funds for private entities, and multilateral development finance institutions or governments for public entities.  
c) Institution type layer 3 (if applicable): offers additional subcategories, such as private equity funds, venture capital funds, and infrastructure funds.  
d) Subsidiary location: Country of residence for the institution.  
e) Parent location: Country of residence for the parent institution. 
        
Institution (Standardized Name): Maps original institution names from raw data sources to standardised CPI institution names. This prevents CPI from having multiple names for the same institution.

For more detailed documentation see [institution](https://www.notion.so/cpi-all/institution_list_cpi-28fefb28632b804b8b96fb7ca466937e) and [institution standandardization mapping](https://www.notion.so/cpi-all/institution_list_all-28fefb28632b8051b09ee27a17d8b6c7).""", 
        primary_key_field='id_institution_cpi',
        required_fields=['institution_cpi'],
        duplicate_check_fields=['institution_cpi'],
        fields=[
            FieldConfig('institution_cpi', 'Institution Name', 'text', required=True,
                       help_text='Enter full institution name without acronyms',
                       placeholder='Type original institution name here...'),
            
            FieldConfig('institution_type_layer1', 'Type Layer 1', 'select', category='main',
                       detailed_help="""
| Category | Definition | Examples |
|----------|------------|----------|
| Public | Organisations that are majority-owned (>50%) or effectively controlled by a government or public authority. This includes central, regional, or local governments, state-owned enterprises (SOEs) and financial institutions (SOFIs), and multilateral, bilateral, and national development finance institutions (DFIs). | Ministries, central banks, sovereign wealth funds, state-owned utilities, public universities, multilateral DFIs (e.g., World Bank, AfDB) |
| Private | Organisations that are majority-owned (>50%) by private individuals, corporations, or other non-government entities.  | Commercial banks, private investment firms, family offices, privately held corporations, listed companies without majority state ownership |
| Unknown | Ownership and control information cannot be reliably determined after reasonable research. This should be used sparingly and only when sufficient information is not publicly available. | Entities with limited disclosure or unverified ownership structures |

For subsidiaries of public institutions, classify according to the ownership of the subsidiary itself, not the parent organisation, unless the parent’s control clearly determines operations. To find the relevant information for the above classification, we recommend following the below steps (in order):  
1. Visit the official company website and check the “About”, “Governance”, or “Investor Relations” sections to see if you can find any ownership and shareholder information.  
2. Visit reputable financial or business sources to search for information. For example:  
a) Bloomberg. Google search “[institution name] Bloomberg” and select the company profile link. You will need a subscription to see all the company details, but a summary is still available.  
b) Reuters. Google search “[institution name] Reuters” and select the Stock Price and Latest News link. Scroll down and you will see the “Company Information” section.  
c) Crunchbase. Google search “[institution name] Crunchbase” and select the Company Profile & Funding link. The header will have the industry of the institution. Scroll down company details.""", required=True),
            
            FieldConfig('institution_type_layer2', 'Type Layer 2', 'select', category='main', detailed_help="""
When in doubt, review the organisation’s core purpose, source of capital, and ownership structure. The aim here is to determine the institutions core function and financial role (i.e., is it producing goods/services, investing, lending, or managing funds?) and relate that to the below categories. 

**Private Institutions**  
| Layer 2 Category | Definition | Examples | Key Indicators |
|------------------|------------|----------|----------------|
| Corporations | Companies engaged in commercial, industrial, or service activities across any sector. May operate in multiple sectors (e.g., energy, water, manufacturing). | Energy utilities, construction firms, conglomerates | Incorporated companies, non-financial operations, sector-based activities |
| Households/Individuals | Family-level economic actors, including high-net-worth individuals (HNWIs) and their intermediaries. | Private investors, family offices, trusts | Ownership by individuals or families, small-scale or personal investments |
| Commercial Financial Institutions (FIs) | Providers of private finance and banking services, typically offering loans, credit, and investment banking services. | Commercial banks, investment banks | Licensed financial institutions, primarily profit-driven |
| Institutional Investors | Entities investing large pools of capital on behalf of others. | Pension funds, insurance companies, asset managers | Long-term investment horizon, fiduciary role |
| Funds | Pooled investment vehicles that raise and manage capital for investment purposes. | Incoming | Incoming |

**Public Institutions**  
| Layer 2 Category | Definition | Examples | Key Indicators |
|------------------|------------|----------|----------------|
| Multilateral DFIs | Multilateral financial institutions providing finance for development objectives. Owned by multiple countries and operating internationally. | World Bank, African Development Bank, ADB | Multiple government shareholders, global or regional mandate |
| Bilateral DFIs | Bilateral financial institutions providing finance for development objectives. Owned by a single country, providing international development finance. | CDC Group (UK), Proparco (France) | Single government ownership, overseas development focus |
| National DFIs | National financial institutions providing finance for development objectives. Owned by a single country, financing domestic development. | BNDES (Brazil), KfW (Germany) | Single government ownership, domestic development focus |
| Governments and Their Agencies | Entities managing or disbursing public budgets. Includes central government bodies and agencies financing domestic activities. | Ministries, national departments, central banks, city councils, regional development agencies | National-level operations, Subnational jurisdiction |
| National and Multilateral Climate Funds | Funds established by governments or international bodies to direct climate-related finance. | Green Climate Fund (multilateral), Indonesia Climate Change Trust Fund (national) | Climate finance focus, government or multilateral ownership | 
| State-Owned Enterprises (SOEs) | Entities at least 50% government-owned, engaged in commercial, industrial, or service activities across any sector. | State-owned energy utilities | Incoming|
| State-Owned Financial Institutions (SOFIs) | State-owned providers of finance and banking services, typically offering loans, credit, and investment banking services. | State-owned banks | Incoming |
| Public Funds | Institutional investors or asset managers operating under public ownership. | Public pension funds, sovereign investment funds | Fund management under public control |

To find the relevant information for the above classification, we recommend following the below steps (in order):  
1. Visit the official company website and check the “About”, “Governance”, or “Investor Relations” sections to see if you can find any ownership and shareholder information. 

2. Visit reputable financial or business sources to search for information. For example:  
a) Bloomberg. Google search “[institution name] Bloomberg” and select the company profile link. You will need a subscription to see all the company details, but a summary is still available.  
b) Reuters. Google search “[institution name] Reuters” and select the Stock Price and Latest News link. Scroll down and you will see the “Company Information” section.  
c) Crunchbase. Google search “[institution name] Crunchbase” and select the Company Profile & Funding link. The header will have the industry of the institution. Scroll down company details.""", required=True),
            
            FieldConfig('institution_type_layer3', 'Type Layer 3', 'select', category='main', detailed_help="""
To fill in Institution Type Layer 3:   
1. Identify the Layer 2 category first (e.g., Funds or DFIs).  
2. Select the correct Layer 3 sub-type based on ownership structure, scope of activity, or sector focus.  
3. If no Layer 3 category applies, leave Layer 3 blank (not all Layer 2 categories have sub-types). 

**Private Institutions**  
| Layer 2 | Layer 3 | Definition/Description | Examples | Classification Notes |
|---------|---------|------------------------|----------|----------------------|
| Funds | Private Equity Funds | Investment funds that take ownership positions in private companies or conduct buyouts, often with a long-term and high-risk profile. | Blackstone, Carlyle Group, EQT | Invest primarily in unlisted companies; high return expectations. |
| Venture Capital Funds | Funds that invest in early-stage or high-growth companies, often in technology or innovation sectors. | Sequoia Capital, Andreessen Horowitz | Early-stage, high-risk/high-growth investment strategy. |
| Infrastructure Funds | Funds that invest in infrastructure assets such as energy, transport, or utilities. | Brookfield Infrastructure Partners, Macquarie Infrastructure Fund | Focused on long-term, stable returns from infrastructure projects. |

**Public Institutions**  
| Layer 2 | Layer 3 | Definition/Description | Examples | Classification Notes |
|---------|---------|------------------------|----------|----------------------|
| Development Finance Institutions (DFIs) | Multilateral and Regional DFIs | Institutions owned by multiple countries that finance international or regional development. | World Bank, African Development Bank (AfDB), Asian Development Bank (ADB) | Multilateral ownership (≥2 governments), cross-border operations. |
| Development Finance Institutions (DFIs) | Bilateral DFIs | Institutions owned by a single country that provide development finance internationally. | British International Investment (BII/CDC), Proparco (France), DEG (Germany) | Single-country ownership; invests abroad for development impact. |
| Development Finance Institutions (DFIs) | National DFIs | Publicly owned financial institutions providing development finance within their home country. | BNDES (Brazil), KfW (Germany), Development Bank of the Philippines | Domestic focus; national development objectives. |
| Governments and Their Agencies | National | Central or federal government bodies that manage national budgets and implement policies. | Ministries of Finance, Central Banks, National Planning Agencies | Operate at the sovereign or federal level. |
| Governments and Their Agencies | Subnational | Regional, state, or municipal governments and agencies financing projects within their jurisdiction. | State governments, city development agencies | Operate below national level; regional or local mandate. |
| National and Multilateral Climate Funds | National Climate Funds | Funds established and owned by a single national government to manage and direct climate finance domestically. | Indonesia Climate Change Trust Fund, Rwanda Green Fund (FONERWA) | Nationally owned; climate-specific focus. |
| National and Multilateral Climate Funds | Multilateral Climate Funds | Funds owned collectively by multiple governments to finance climate projects internationally, often managed by an international organisation. | Green Climate Fund (GCF), Climate Investment Funds (CIFs) | Multilateral ownership; global or regional climate finance mandate. |

To find the relevant information for the above classification, we recommend following the below steps (in order):  
1. Visit the official company website and check the “About”, “Governance”, or “Investor Relations” sections to see if you can find any ownership and shareholder information.  
2. Visit reputable financial or business sources to search for information. For example:  
a) Bloomberg. Google search “[institution name] Bloomberg” and select the company profile link. You will need a subscription to see all the company details, but a summary is still available.  
b) Reuters. Google search “[institution name] Reuters” and select the Stock Price and Latest News link. Scroll down and you will see the “Company Information” section.  
c) Crunchbase. Google search “[institution name] Crunchbase” and select the Company Profile & Funding link. The header will have the industry of the institution. Scroll down company details. 
""", required=True),
            FieldConfig('country_sub', 'Subsidiary Country', 'select', category='main',
                       help_text='Subsidiary country where institution operates', required=True),
            FieldConfig('country_parent', 'Parent Country', 'select', category='main',
                       help_text='Country where headquarters is located', required=True),
            FieldConfig('double_counting_risk', 'Double Counting Risk', 'select', category='main'),
            FieldConfig('institution_cpi_short', 'Short Name', 'text', category='advanced',
                       help_text='Abbreviated name (for known acronyms/shortened forms)'),
            FieldConfig('contact_info', 'Contact Information', 'textarea',category='advanced',
                       help_text='Contact details, website, etc.'),
            FieldConfig('comments', 'Comments', 'textarea',category='advanced',
                       help_text='Additional notes or comments'),
        ] + AUDIT_FIELDS
    ),
    
    'instrument': TableConfig(
        table_name='instrument',
        display_name='Instrument',
        description='Add the name of the instrument as it is in source. Do not enter duplicates. Information quality refers to the status of the information e.g. "Introduced in 2021" or "Removed in 2022"',
        general_description='Documentation incoming.', 
        primary_key_field='id_instrument',
        required_fields=['original_name'],
        duplicate_check_fields=['original_name'],
        fields=[
            FieldConfig('original_name', 'Original Instrument Name (from source)', 'text', required=True,
                       help_text='Type of financial instrument',
                       placeholder='Enter instrument name...'),
            FieldConfig('original_name_2', 'Original Name 2', 'text', category='main'),
            FieldConfig('original_name_3', 'Original Name 3', 'text', category='main'),
            FieldConfig('instrument_type', 'Instrument Type', 'select', category='main',
                       help_text='Type of financial instrument',
                       placeholder='Enter instrument type...', required=True),
            FieldConfig('instrument_type_layer2', 'Type Layer 2', 'select', category='main', required=True),
            FieldConfig('definition', 'Definition', 'textarea',
                       help_text='Clear definition of this instrument', category='main'),
            FieldConfig('info_quality', 'Information Quality', 'text', category='main',
                       help_text='Status of information', required=True),
            FieldConfig('categorization', 'Categorization', 'select', category='main', required=True),
            FieldConfig('description', 'Description', 'textarea',
                       help_text='Detailed description', category='main'),
            FieldConfig('example', 'Example', 'textarea',
                       help_text='Example of this instrument in use', category='main'),
        ] + AUDIT_FIELDS
    ),

    'gearing': TableConfig(
        table_name='gearing',
        display_name='Gearing Ratios',
        description='Enter gearing ratio as decimal, debt:equity.',
        general_description='Gearing ratios by country, year, and sector. See [documentation](https://www.notion.so/cpi-all/Gearing-287efb28632b80529bfef8ccfc97de17) for details.', 
        primary_key_field='id_gearing',
        required_fields=['sector_re'],
        duplicate_check_fields=['gearing', 'sector_re', 'country_cpi', 'last_verified'],
        fields=[
            FieldConfig('sector_re', 'Solution', 'select', category='main', required=True,
                       help_text='solution'),
            FieldConfig('country_cpi', 'Country', 'select', category='main',
                       help_text='Country', required=True),
            FieldConfig('region_cpi', 'Region', 'select', category='main', required=True),
            FieldConfig('gearing', 'Gearing Ratio', 'number', category='main', required=True),
            FieldConfig('source', 'Source', 'test', category='main', required=True),
            FieldConfig('last_verified', 'Year', 'number', category='main', required=True)  
        ] + AUDIT_FIELDS
    ),

    'multiplier': TableConfig(
        table_name='multiplier',
        display_name='Multipliers',
        description='Multiplier - update',
        general_description='Multipliers by sector and country. See [documentation](https://www.notion.so/cpi-all/Multiplier-287efb28632b80838219d6fa2b3b50dc) for details.', 
        primary_key_field='id_multiplier',
        required_fields=['sub_sector_source'],
        duplicate_check_fields=['sub_sector_bnef', 'country', 'year_of_analysis'],
        fields=[
            FieldConfig('sub_sector_source', 'Sub-sector name (from source)', 'text', required=True),
            FieldConfig('multiplier_local', 'Local Multiplier', 'number', category='main', required=True),
            FieldConfig('sub_sector_bnef', 'Sub-sector name (BNEF)', 'select', category='main', required=True),
            FieldConfig('country_cpi', 'Country', 'select', category='main', required=True),
            FieldConfig('region_cpi', 'Region', 'select', category='main', required=True),
            FieldConfig('currency', 'Currency', 'select', category='main', required=True),
            FieldConfig('conversion_rate', 'Conversion', 'select'),
            FieldConfig('multiplier_usd', 'Multiplier in USD', 'number', category='main', required=True),
            FieldConfig('data_source_type', 'Data Source', 'select', category='main', required=True),
            FieldConfig('notes', 'Notes', 'text', category='main'),
            FieldConfig('last_verified', 'Last verified', 'number'),
            FieldConfig('year_of_analysis', 'Year of analysis', 'number', category='main', required=True)  
        ] + AUDIT_FIELDS
    ),

    
    # 'exchange_rates': TableConfig(
    #     table_name='exchange_rates',
    #     display_name='Exchange Rates',
    #     description='Currency exchange rates by country and year',
    #     general_description='Exchange rates by country and year. Documentation incoming.', 
    #     primary_key_field='id_fx',
    #     required_fields=['country_cpi'],
    #     duplicate_check_fields=['country_cpi', 'year'],
    #     fields=[
    #         FieldConfig('country_cpi', 'Country', 'select',
    #                    required=True,
    #                    help_text='Country for this exchange rate'),
    #         FieldConfig('currency_code', 'Currency Code', 'text', category='main',
    #                    help_text='3-letter currency code (USD, EUR, etc.)',
    #                    placeholder='USD, EUR, GBP...'),
    #         FieldConfig('fx_rate', 'Exchange Rate', 'number', category='main',
    #                    validation_fn=validate_decimal,
    #                    help_text='Exchange rate to USD'),
    #         FieldConfig('year', 'Year', 'number',category='main')  
    #     ] + AUDIT_FIELDS
    # ), 
    
    # 'institution_standardization': TableConfig(
    #     table_name='institution_standardization',
    #     display_name='Institution Mapping',
    #     description='Standardization mapping table for institutions. Typically no need to edit directly. If you are adding a value, type in the original unstandardized name into the Original Name field. Type the standardized value into the Institution Standardized Name section and choose from the correct option listed (needs to be an existing institution).',
    #     general_description='Institution standardization links institution names found in raw data sources to standardised CPI institution names. See [documentation](https://www.notion.so/cpi-all/institution_list_all-28fefb28632b8051b09ee27a17d8b6c7) for details.', 
    #     primary_key_field='id_institution',
    #     required_fields=['institution_original', 'institution_cpi'],
    #     duplicate_check_fields=['institution_original'],
    #     fields=[
    #         FieldConfig('institution_original', 'Institution Original Name', 'text', category='main', required=True,
    #                    help_text='Full name of the institution',
    #                    placeholder='Enter institution name...'),
    #         FieldConfig('institution_cpi', 'Institution Standardized Name', 'institution_search', category='main', required=True,
    #                    help_text='Only enter if you know the mapping for standardization already or adding a new institution.'),
    #     ] + AUDIT_FIELDS
    # ),

    'hierarchy': TableConfig(
        table_name='hierarchy',
        display_name='Institution Hierarchy',
        description='Define parent-child relationships between institutions. Ownership defaults to 1 but can input proportion as a decimal. Controlling institution automatically selected for proportions over 0.5.',
        general_description='Hierarchy table describes relationships between institutions (e.g. government branches, subsidiaries, etc.) Documentation incoming.', 
        primary_key_field='id_hierarchy',
        required_fields=['parent_institution', 'child_institution'],
        duplicate_check_fields=['id_parent', 'id_child'],
        fields=[
            FieldConfig('parent_institution', 'Parent Institution', 'text', required=True,
                       help_text='Name of the parent institution (must exist in institution table)',
                       placeholder='Search for parent institution...'),
            FieldConfig('child_institution', 'Child Institution', 'text', required=True,
                       help_text='Name of the child institution (must exist in institution table)',
                       placeholder='Search for child institution...'),
            FieldConfig('percent_ownership', 'Ownership Percentage', 'number', 
                       help_text='Ownership percentage as decimal (0.0 to 1.0, e.g., 0.51 for 51%)'),
            FieldConfig('is_controlling_institution', 'Is Controlling', 'boolean',
                       help_text='True if parent has controlling interest (>50% ownership)'),
            FieldConfig('relationship_type', 'Relationship Type', 'text',
                       help_text='Type of relationship (e.g., "subsidiary", "division", "branch")',
                       placeholder='Enter relationship type...'),
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


COLUMN_TYPE_CONFIGS = {
    'institution': ColumnTypeConfig(
        string_columns=[
            'institution_cpi', 'institution_cpi_short',
            'institution_type_layer1', 'institution_type_layer2', 'institution_type_layer3',
            'country_sub', 'country_parent', 'double_counting_risk',
            'contact_info', 'comments', 'created_by'
        ],
        integer_columns=['id_institution_cpi', 'last_verified', 'created_at']
    ),
    
    'instrument': ColumnTypeConfig(
        string_columns=[
            'original_name', 'instrument_type', 'instrument_type_layer2',
            'definition', 'info_quality', 'categorization', 'description',
            'example', 'created_by'
        ],
        integer_columns=['id_instrument', 'created_at']
    ),
    
    'gearing': ColumnTypeConfig(
        string_columns=[
            'sector_re', 'country_cpi', 'region_cpi', 'source', 'created_by'
        ],
        integer_columns=['id_gearing', 'last_verified', 'created_at'],
        float_columns=['gearing']
    ),
    
    'multiplier': ColumnTypeConfig(
        string_columns=[
            'sub_sector_source', 'sub_sector_bnef', 'country_cpi', 'region_cpi',
            'currency', 'conversion_rate', 'data_source_type', 'notes', 'created_by'
        ],
        integer_columns=['id_multiplier', 'last_verified', 'year_of_analysis', 'created_at'],
        float_columns=['multiplier_local', 'multiplier_usd']
    ),
    
    'exchange_rates': ColumnTypeConfig(
        string_columns=['country_cpi', 'currency_code', 'created_by'],
        integer_columns=['id_fx', 'year', 'created_at'],
        float_columns=['fx_rate']
    ),
    
    'institution_standardization': ColumnTypeConfig(
        string_columns=['institution_original', 'institution_cpi', 'created_by'],
        integer_columns=['id_institution', 'id_institution_cpi', 'created_at']
    ),
    
    'hierarchy': ColumnTypeConfig(
        string_columns=[
            'parent_institution', 'child_institution', 'relationship_type', 'created_by'
        ],
        integer_columns=['id_hierarchy', 'id_parent', 'id_child', 'created_at'],
        float_columns=['percent_ownership'],
        boolean_columns=['is_controlling_institution']
    ),
}


TABLE_ID_COLUMNS = {
    'institution': 'id_institution_cpi',
    'instrument': 'id_instrument', 
    'gearing': 'id_gearing',
    'multiplier': 'id_multiplier',
    'exchange_rates': 'id_fx',
    'institution_standardization': 'id_institution',
    'hierarchy': 'id_hierarchy'
}

# This is for filtering the insittution dropdown menu so that if the user selects Public only Public layer type 2 shows up, etc.
INSTITUTION_TYPE_HIERARCHY = {
    'Public': {
        'Government': [
            'Central Government',
            'Subnational Government', 
            'Government Agencies'
        ],
        'State-owned Enterprise': [
            'Corporate'
        ],
        'Bilateral DFI': [
            'Bank'
        ],
        'Multilateral DFI': [
            'Bank'
        ],
        'National DFI': [
            'Bank'
        ],
        'Multilateral Climate Funds': [
            'Public Fund'
        ],
        'State-owned FI': [
            'Commercial Bank',
            'Corporate & Investment Banks'
        ],
        'Export Credit Agency (ECA)': [
            'Bank'
        ],
        'Public Fund': [
            'Pension Fund'
        ]
    },
    'Private': {
        'Corporate': [
            'Corporate'
        ],
        'Commercial FI': [
            'Commercial Bank',
            'Corporate & Investment Banks',
            'Asset Manager',
            'Insurance Company'
        ],
        'Funds': [
            'Private Equity Funds',
            'Venture Capital Funds',
            'Infrastructure Funds'
        ],
        'Institutional Investors': [
            'Pension Fund',
            'Asset Manager',
            'Insurance Company'
        ],
        'Household/Individual': [],
        'Third Sector Organisation': []
    }
}

# Fallback options for when no specific mapping exists
DEFAULT_TYPE2_OPTIONS = [
    '', 'Funds', 'Corporation', 'Commercial FI', 'Government', 
    'Institutional Investors', 'Bilateral DFI', 'SOE', 
    'Multilateral Climate Funds', 'Multilateral DFI', 
    'Export Credit Agency (ECA)', 'State-owned FI', 
    'National DFI', 'Household/Individual', 'Public Fund', 
    'Third Sector Organisation'
]

DEFAULT_TYPE3_OPTIONS = [
    '', 'Corporate', 'Venture Capital Funds', 'Commercial Bank', 
    'Infrastructure Funds', 'Subnational Government', 'Pension Fund', 
    'Private Equity Funds', 'Corporate & Investment Banks', 
    'Central Government', 'Asset Manager', 'Insurance Company', 
    'Government Agencies', 'Bank'
]

def get_filtered_type2_options(type1_value: Optional[str], existing_values: Optional[List[str]] = None) -> List[str]:
    """Get filtered type2 options based on type1 selection"""
    if not type1_value or type1_value == '':
        base_options = DEFAULT_TYPE2_OPTIONS.copy()
    else:
        type2_mapping = INSTITUTION_TYPE_HIERARCHY.get(type1_value, {})
        if type2_mapping:
            base_options = [''] + list(type2_mapping.keys())
        else:
            base_options = DEFAULT_TYPE2_OPTIONS.copy()
    
    if existing_values:
        all_options = set(base_options[1:])
        all_options.update(existing_values)
        base_options = [''] + sorted(list(all_options))
    
    return base_options

def get_filtered_type3_options(type1_value: Optional[str], type2_value: Optional[str], 
                               existing_values: Optional[List[str]] = None) -> List[str]:
    """Get filtered type3 options based on type1 and type2 selections"""
    if not type1_value or type1_value == '' or not type2_value or type2_value == '':
        base_options = DEFAULT_TYPE3_OPTIONS.copy()
    else:
        type2_mapping = INSTITUTION_TYPE_HIERARCHY.get(type1_value, {})
        if type2_mapping and type2_value in type2_mapping:
            type3_options = type2_mapping[type2_value]
            base_options = [''] + type3_options
        else:
            base_options = DEFAULT_TYPE3_OPTIONS.copy()
    
    if existing_values:
        all_options = set(base_options[1:])
        all_options.update(existing_values)
        base_options = [''] + sorted(list(all_options))
    
    return base_options

def validate_type_hierarchy(type1: Optional[str], type2: Optional[str], type3: Optional[str]) -> Dict[str, Any]:
    """Validate that the selected type hierarchy is consistent"""
    result = {
        'valid': True,
        'warnings': [],
        'suggestions': []
    }
    
    # Check if type2 is valid for type1
    if type1 and type2:
        valid_type2_options = get_filtered_type2_options(type1)
        if type2 not in valid_type2_options:
            result['valid'] = False
            result['warnings'].append(f"'{type2}' is not typically associated with '{type1}' institutions")
            result['suggestions'].append(f"Consider: {', '.join(valid_type2_options[1:3])}")
    
    # Check if type3 is valid for type1/type2 combination
    if type1 and type2 and type3:
        valid_type3_options = get_filtered_type3_options(type1, type2)
        if type3 not in valid_type3_options:
            result['valid'] = False
            result['warnings'].append(f"'{type3}' is not typically associated with '{type1}' -> '{type2}' institutions")
            if len(valid_type3_options) > 1:
                result['suggestions'].append(f"Consider: {', '.join(valid_type3_options[1:3])}")
    
    return result
def get_table_id_column(table_name: str) -> Optional[str]:
    return TABLE_ID_COLUMNS.get(table_name)

def get_column_type_config(table_name: str) -> Optional[ColumnTypeConfig]:
    return COLUMN_TYPE_CONFIGS.get(table_name)

def get_all_column_configs() -> Dict[str, ColumnTypeConfig]:
    return COLUMN_TYPE_CONFIGS
    

def get_table_config(table_name: str) -> Optional[TableConfig]:
    return TABLE_CONFIGS.get(table_name)


def get_available_tables() -> List[str]:
    return list(TABLE_CONFIGS.keys())


def get_table_display_names() -> Dict[str, str]:
    return {name: config.display_name for name, config in TABLE_CONFIGS.items()}

