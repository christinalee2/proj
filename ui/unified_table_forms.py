import streamlit as st
import pandas as pd
import io
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass

from table_configs import get_table_config, TableConfig
from services.institution_service import InstitutionService
from services.institution_lookup_service import InstitutionLookupService
from database.cached_queries import get_table_data_cached
from database.queries import QueryService
from utils.text_processing import TextProcessor
from utils.fuzzy_matching import get_fitted_matcher
from services.standardization_service import StandardizationService
import time
from config import CURRENT_YEAR, should_auto_populate_year, get_audit_data, AUDIT_FIELDS
from services.hierarchy_service import HierarchyService
from ui.hierarchy_ui import render_hierarchy_options_for_duplicates, render_hierarchy_options_for_fuzzy_matches, render_new_institution_hierarchy_option, render_institution_search_widget


@dataclass
class ValidationResult:
    """Result of validating one row"""
    row_index: int
    status: str
    issues: List[str]
    fuzzy_matches: List[Tuple[str, float]]
    suggested_action: str
    data: Dict



def normalize_name(name: str) -> str:
    """Normalize name for comparison"""
    if not name:
        return ""
    return TextProcessor.normalize_institution_name(name).lower().strip()


def check_exact_duplicate(input_value: str, existing_df: pd.DataFrame, primary_field: str, standardization_df: Optional[pd.DataFrame] = None) -> Optional[Dict[str, str]]:
    """Check for exact duplicate in institution table and institution_standardization table then institution short for acronyms"""
    if existing_df.empty or primary_field not in existing_df.columns:
        return None
    
    normalized_input = normalize_name(input_value)
    
    # Check main institution table first
    for _, row in existing_df.iterrows():
        existing_value = str(row.get(primary_field, '')).strip()
        if normalize_name(existing_value) == normalized_input:
            return {
                'type': 'main_table',
                'match': existing_value,
                'source': 'institution table'
            }
    
    # Check standardization table
    if primary_field == 'institution_cpi':
        try:
            if not standardization_df.empty and 'institution_original' in standardization_df.columns:
                for _, row in standardization_df.iterrows():
                    existing_value = str(row.get('institution_original', '')).strip()
                    if normalize_name(existing_value) == normalized_input:
                        # Get the standardized name
                        standardized_name = str(row.get('institution_cpi', '')).strip()
                        return {
                            'type': 'standardization',
                            'match': existing_value,
                            'source': 'standardization table',
                            'standardized_name': standardized_name
                        }
        except Exception as e:
            print(f"Error checking institution_standardization: {e}")

    # Check acronyms in main table
    if primary_field == 'institution_cpi' and 'institution_cpi_short' in existing_df.columns:
            try:
                for _, row in existing_df.iterrows():
                    existing_short = str(row.get('institution_cpi_short', '')).strip()
                    if existing_short and normalize_name(existing_short) == normalized_input:
                        existing = str(row.get('institution_cpi', '')).strip()
                        return {
                            'type': 'acronym',
                            'match': f"{existing_short} found as {existing}",
                            'source': 'institution table'
                        }
            except Exception as e:
                print(f"Error checking institution_cpi_short: {e}")
    
    return None


def check_compound_duplicate(form_data: Dict[str, Any], existing_df: pd.DataFrame, duplicate_check_fields: List[str]) -> Optional[str]:
    """Check for exact duplicate using multiple fields like Germany, 2021, Wind for example"""
    if existing_df.empty or not duplicate_check_fields:
        return None
    
    missing_fields = [field for field in duplicate_check_fields if field not in existing_df.columns]
    if missing_fields:
        return None
    
    input_values = {}
    for field in duplicate_check_fields:
        value = form_data.get(field)
        if value is None or str(value).strip() == '':
            continue  # Skip empty values
        
        if isinstance(value, str):
            input_values[field] = normalize_name(value) if field in ['institution_cpi', 'original_name'] else str(value).strip().lower()
        else:
            input_values[field] = str(value).strip()
    
    if not input_values:
        return None
    
    for _, row in existing_df.iterrows():
        match_count = 0
        total_fields = len(input_values)
        
        for field, input_val in input_values.items():
            existing_value = row.get(field)
            if existing_value is None:
                continue
                
            if isinstance(existing_value, str):
                if field in ['institution_cpi', 'original_name']:
                    existing_normalized = normalize_name(existing_value)
                else:
                    existing_normalized = str(existing_value).strip().lower()
            else:
                existing_normalized = str(existing_value).strip()
            
            if input_val == existing_normalized:
                match_count += 1
        
        if match_count == total_fields and match_count > 0:
            match_desc = []
            for field in duplicate_check_fields:
                if field in input_values:
                    match_desc.append(f"{field}: {row.get(field, 'N/A')}")
            return " | ".join(match_desc)
    
    return None




def check_fuzzy_matches(input_value: str, existing_df: pd.DataFrame, primary_field: str) -> List[Tuple[str, float]]:
    """Find fuzzy matches in any table's primary field"""
    if existing_df.empty or primary_field not in existing_df.columns:
        return []
    
    try:
        temp_df = existing_df.copy()
        temp_df['institution_cpi'] = temp_df[primary_field]  # Fuzzy matcher expects this column name
        
        matcher = get_fitted_matcher(temp_df, threshold=0.85)
        matches = matcher.find_similar_institutions(
            query=input_value,
            institution_df=temp_df,
            limit=5,
            tfidf_top_k=50
        )
        # Filter out exact matches and return original field values
        return [(name, score) for name, score in matches 
                if normalize_name(name) != normalize_name(input_value)]
    except Exception as e:
        print(f"Error in fuzzy matching: {e}")
        return []





def render_lookup_sources_compact(lookup_result, key_suffix=""):
    """Render lookup sources in a compact expandable format"""
    if not lookup_result or not lookup_result.sources:
        return
    
    valid_sources = [s for s in lookup_result.sources if s.get('url') and s.get('title')]
    if not valid_sources:
        return
    
    with st.expander(f"Sources ({len(valid_sources)} used)", expanded=False):
        st.caption("Click links to verify the information:")
        for idx, source in enumerate(valid_sources, 1):
            # Truncate title if too long
            title = source['title']
            if len(title) > 60:
                title = title[:57] + "..."
            
            url = source['url']
            try:
                domain = url.split('/')[2] if '://' in url else url.split('/')[0]
                domain = domain.replace('www.', '')
            except:
                domain = "source"
            
            st.markdown(f"**{idx}.** [{title}]({url}) `{domain}`")




            

def get_table_dropdown_options(table_name: str, config, existing_data: pd.DataFrame):
    """
    Dropdown options, for institution draws geography options from the table directly, otherwise loads from geography table directly as other lists are non-complete
    """
    options = {}
    
    try:        
        if existing_data.empty:
            existing_data = get_table_data_cached(table_name, limit=None)
            if existing_data.empty:
                for field_config in config.fields:
                    if field_config.field_type == 'select':
                        options[field_config.name] = ['']
                return options
        
        for field_config in config.fields:
            if field_config.field_type == 'select':
                field_name = field_config.name
                
                # Handle country fields
                if field_name == 'country_cpi' and table_name != 'institution':
                    if 'geography_countries' not in st.session_state:
                        try:
                            geo_data = get_table_data_cached('geography', limit=None)
                            if not geo_data.empty and 'country_cpi' in geo_data.columns:
                                countries = sorted(geo_data['country_cpi'].dropna().unique())
                                st.session_state['geography_countries'] = [str(c) for c in countries if str(c).strip()]
                                print(f"Loaded {len(st.session_state['geography_countries'])} countries from geography table (cached in session)")
                            else:
                                st.session_state['geography_countries'] = []
                                print("Geography table empty or missing country_cpi column")
                        except Exception as e:
                            print(f"Could not load geography data: {e}")
                            st.session_state['geography_countries'] = []
                    
                    # Use cached geography countries
                    if st.session_state['geography_countries']:
                        options[field_name] = [''] + st.session_state['geography_countries']
                    else:
                        if field_name in existing_data.columns:
                            unique_values = existing_data[field_name].dropna().unique()
                            unique_strings = sorted([str(v) for v in unique_values if str(v).strip()])
                            options[field_name] = [''] + unique_strings
                            print(f"Fallback: using {len(unique_strings)} countries from {table_name} table")
                        else:
                            options[field_name] = ['']
                
                elif field_name in ['country_sub', 'country_parent'] and table_name == 'institution':
                    # Institution already has a complete list so avoids having to reload multiple times
                    if field_name in existing_data.columns:
                        unique_values = existing_data[field_name].dropna().unique()
                        unique_strings = sorted([str(v) for v in unique_values if str(v).strip()])
                        options[field_name] = [''] + unique_strings
                    else:
                        options[field_name] = ['']
                
                elif field_name in existing_data.columns:
                    unique_values = existing_data[field_name].dropna().unique()
                    unique_strings = sorted([str(v) for v in unique_values if str(v).strip()])
                    options[field_name] = [''] + unique_strings
                else:
                    options[field_name] = ['']
        
        return options
        
    except Exception as e:
        print(f"Error getting dropdown options for {table_name}: {e}")
        for field_config in config.fields:
            if field_config.field_type == 'select':
                options[field_config.name] = ['']
        return options




def auto_populate_data(data: Dict[str, Any], username: str) -> Dict[str, Any]:
    """
    Auto-populate year fields and audit fields for any table entry
    
    Args:
        data: The form data dictionary
        username: Username for audit trail
        
    Returns:
        Updated data dictionary with auto-populated fields
    """
    enhanced_data = data.copy()
    
    # Auto-populate year fields with current year if empty
    for field_name, value in enhanced_data.items():
        if should_auto_populate_year(field_name):
            if not value or str(value).strip() == '':
                enhanced_data[field_name] = CURRENT_YEAR
                print(f"Auto-populated {field_name} with {CURRENT_YEAR}")
    
    audit_data = get_audit_data(username)
    enhanced_data.update(audit_data)
    
    return enhanced_data


def create_table_entry(table_name: str, data: Dict[str, Any], user: str = "system") -> Dict[str, Any]:
    """Create entry in any table using appropriate service"""

    data = auto_populate_data(data, user)
    
    if table_name == 'institution':
        # Use existing institution service
        service = InstitutionService()
        return service.create_institution(
            institution_name=data.get('institution_cpi', ''),
            institution_type_layer1=data.get('institution_type_layer1'),
            institution_type_layer2=data.get('institution_type_layer2'),
            institution_type_layer3=data.get('institution_type_layer3'),
            country_sub=data.get('country_sub'),
            country_parent=data.get('country_parent'),
            institution_cpi_short=data.get('institution_cpi_short'),
            double_counting_risk=data.get('double_counting_risk'),
            contact_info=data.get('contact_info'),
            comments=data.get('comments'),
            user=user,
            created_by=data.get('created_by'),
            created_at=data.get('created_at'),
        )
    elif table_name == 'institution_standardization':
        # Special handling for institution_standardization to get the institution ID
        try:
            config = get_table_config(table_name)
            if not config:
                return {'success': False, 'message': f'No configuration for table {table_name}'}
            
            clean_data = {}
            
            institution_cpi_name = data.get('institution_cpi')
            institution_id = st.session_state.get(f"{table_name}_primary_id")
            
            if not institution_id:
                for key in st.session_state.keys():
                    if 'institution_cpi' in key and key.endswith('_id'):
                        institution_id = st.session_state[key]
                        break
            
            if institution_id and institution_cpi_name:
                clean_data['id_institution_cpi'] = int(institution_id)
                clean_data['institution_cpi'] = institution_cpi_name
            else:
                return {'success': False, 'message': 'Institution ID not found. Please select an institution from the search.'}
            
            for field_config in config.fields:
                field_name = field_config.name
                if field_name == 'institution_cpi':
                    continue  # Already handled above
                if field_name in data and data[field_name] is not None and str(data[field_name]).strip() != '':
                    clean_data[field_name] = data[field_name]
            
            clean_data['created_at'] = CURRENT_YEAR
            clean_data['created_by'] = user
            
            clean_data = {k: v for k, v in clean_data.items() if v is not None}
            
            query_service = QueryService()
            success = query_service.execute_insert(table_name, clean_data)
            
            if success:
                return {
                    'success': True,
                    'entry_id': clean_data.get('id_institution_cpi'),
                    'message': 'Institution standardization entry created successfully'
                }
            else:
                return {'success': False, 'message': 'Failed to insert into database'}
                
        except Exception as e:
            return {'success': False, 'message': f'Error creating entry: {str(e)}'}
        
    else:
        # For other tables, use direct database insertion, could probably make institution here too, I just set it up on institution service initially
        try:
            # Get table configuration
            config = get_table_config(table_name)
            if not config:
                return {'success': False, 'message': f'No configuration for table {table_name}'}
            
            clean_data = {}
            
            for field_config in config.fields:
                field_name = field_config.name
                if field_name in data and data[field_name] is not None and str(data[field_name]).strip() != '':
                    clean_data[field_name] = data[field_name]
            
            clean_data['created_at'] = CURRENT_YEAR     # Explicit integer like institution service
            clean_data['created_by'] = user             # Explicit string like institution service

            # year_fields = ['last_verified', 'year', 'year_added', 'year_of_analysis']
            # for year_field in year_fields:
            #     if any(field.name == year_field for field in config.fields):
            #         clean_data[year_field] = CURRENT_YEAR  

            year_fields = ['last_verified', 'year', 'year_added', 'year_of_analysis']
            for year_field in year_fields:
                if any(field.name == year_field for field in config.fields):
                    if year_field not in clean_data or not clean_data[year_field]:
                        clean_data[year_field] = CURRENT_YEAR

            integer_fields = ['m49_code', 'iso_numeric_code']
            for field in integer_fields:
                if field in clean_data:
                    value = clean_data[field]
                    if value is None or str(value).strip() == '':
                        clean_data[field] = None  
                    else:
                        try:
                            clean_data[field] = int(float(str(value).strip()))
                        except (ValueError, TypeError):
                            clean_data[field] = None
            
            clean_data = {k: v for k, v in clean_data.items() if v is not None}
            
            query_service = QueryService()
            success = query_service.execute_insert(table_name, clean_data)
            
            if success:
                return {
                    'success': True,
                    'entry_id': clean_data.get(list(clean_data.keys())[0]),
                    'message': 'Entry created successfully',
                    # 'new_id': new_id
                }
            else:
                return {
                    'success': False,
                    'message': 'Failed to insert into database'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error creating entry: {str(e)}'
            }






def render_form_field(field_config, dropdown_options: Dict[str, List[str]], key_suffix: str, existing_institutions: pd.DataFrame = None) -> Any:
    """Render a single form field with improved prefill handling"""
    field_key = f"{field_config.name}_{key_suffix}"
    
    default_value = ''
    
    prefill_mapping = {
        'institution_type_layer1': 'prefill_type1',
        'institution_type_layer2': 'prefill_type2', 
        'institution_type_layer3': 'prefill_type3',
        'country_parent': 'prefill_parent',
        'country_sub': 'prefill_sub'
    }
    
    prefill_key = prefill_mapping.get(field_config.name)
    if prefill_key and st.session_state.get(prefill_key):
        default_value = st.session_state[prefill_key]
    
    if should_auto_populate_year(field_config.name) and not default_value:
        default_value = CURRENT_YEAR

    if field_config.field_type == 'institution_search': # different widget for looking up institutions so that it gets existing value and id
        if existing_institutions is None or existing_institutions.empty:
            if 'institution_standardization_institutions' in st.session_state:
                existing_institutions = st.session_state['institution_standardization_institutions']
            else:
                st.error("Institution data not available for search")
                return None
        
        selected_name, selected_id = render_institution_search_widget(
            key=field_key,
            label=field_config.display_name,
            existing_institutions=existing_institutions,
            help_text=field_config.help_text,
            placeholder="Start typing to search institutions..."
        )
        
        if selected_name and selected_id:
            st.session_state[f"{field_key}_name"] = selected_name
            st.session_state[f"{field_key}_id"] = selected_id
            return selected_name 
        else:
            return None


    elif field_config.field_type == 'text':
        return st.text_input(
            field_config.display_name,
            value=default_value,
            placeholder=field_config.placeholder or f"Enter {field_config.display_name.lower()}...",
            help=field_config.help_text,
            key=field_key
        )
    
    elif field_config.field_type == 'textarea':
        return st.text_area(
            field_config.display_name,
            value=default_value,
            placeholder=field_config.placeholder or f"Enter {field_config.display_name.lower()}...",
            help=field_config.help_text,
            key=field_key
        )
    
    elif field_config.field_type == 'number':
        if should_auto_populate_year(field_config.name):  
            return st.number_input(
                field_config.display_name,
                help=field_config.help_text,
                key=field_key,
                step=1,
                value=default_value
            )
        else:
            return st.number_input(
                field_config.display_name,
                help=field_config.help_text,
                key=field_key,
                format="%.6f",
                value=None
            )
    
    elif field_config.field_type == 'select':
        options = dropdown_options.get(field_config.name, [''])
        index = 0
        
        if default_value:
            if default_value in options:
                index = options.index(default_value)
            else:
                options = [''] + [default_value] + [opt for opt in options[1:] if opt != default_value]
                index = 1  
        
        return st.selectbox(
            field_config.display_name,
            options=options,
            index=index,
            help=field_config.help_text,
            key=field_key
        )
    
    elif field_config.field_type == 'boolean':
        return st.checkbox(
            field_config.display_name,
            help=field_config.help_text,
            key=field_key
        )
    
    else:
        return st.text_input(
            field_config.display_name,
            value=default_value,
            help=field_config.help_text,
            key=field_key
        )

        


def get_table_reference_data(table_name: str, config):
    """Get reference data needed for a specific table"""
    session_key = f'{table_name}_reference_data'
    
    if session_key not in st.session_state:
        with st.spinner(f"Loading {table_name} reference data..."):
            standardization_data = None
            existing_hierarchy_data = None
            if table_name == 'institution':
                try:
                    standardization_data = get_table_data_cached('institution_standardization', limit=None)
                except Exception as e:
                    print(f"Could not load standardization data: {e}")
                    standardization_data = pd.DataFrame()
            
                try:
                    existing_hierarchy_data = get_table_data_cached('hierarchy', limit=None)
                except Exception as e:
                    print(f"Could not load hierarchy data: {e}")
                    existing_hierarchy_data = pd.DataFrame()
            
            elif table_name == 'institution_standardization':
                # For institution_standardization, we need the institution table data for search
                try:
                    # Load existing institutions for the search widget
                    institution_data = get_table_data_cached('institution', limit=None)
                    st.session_state['institution_standardization_institutions'] = institution_data
                except Exception as e:
                    print(f"Could not load institution data: {e}")
                    st.session_state['institution_standardization_institutions'] = pd.DataFrame()
                    
            existing_data = get_table_data_cached(table_name, limit=None)
            
            dropdown_options = get_table_dropdown_options(table_name, config, existing_data)
            
            st.session_state[session_key] = {
                'existing_data': existing_data,
                'standardization_data': standardization_data,
                'dropdown_options': dropdown_options,
                'existing_hierarchy_data': existing_hierarchy_data
            }
    
    return st.session_state[session_key]



    
def render_unified_single_entry_form(table_name: str):
    """
    Unified single entry form with duplicate checking and Keep functionality for any table
    """
    config = get_table_config(table_name)
    if not config:
        st.error(f"No configuration found for table: {table_name}")
        return
        
    if st.session_state.get('_cache_needs_clear', False):
        st.cache_data.clear()
        st.session_state['_cache_needs_clear'] = False
    
    st.subheader(f"Add New {config.display_name}")
    st.markdown(config.description)
    st.markdown("---")
    
    reference_data = get_table_reference_data(table_name, config)
    existing_data = reference_data['existing_data']
    dropdown_options = reference_data['dropdown_options']
    standardization_data = reference_data.get('standardization_data')
    existing_hierarchy_data = reference_data.get('existing_hierarchy_data')
    
    primary_field = config.required_fields[0] if config.required_fields else config.fields[0].name
    primary_field_config = next((f for f in config.fields if f.name == primary_field), None)
    
    standardization_service = StandardizationService()
    
    if primary_field_config:
                
        if primary_field_config.field_type == 'institution_search':
            if table_name == 'institution_standardization':
                search_data = st.session_state.get('institution_standardization_institutions', pd.DataFrame())
            else:
                search_data = existing_data
                
            primary_value, institution_id = render_institution_search_widget(
                key=f"{table_name}_primary",
                label=primary_field_config.display_name,
                existing_institutions=search_data,
                help_text=primary_field_config.help_text,
                placeholder="Start typing to search institutions..."
            )
            
            # Store the institution ID for later use
            if institution_id:
                st.session_state[f"{table_name}_primary_id"] = institution_id
        else:
            # For regular text fields
            primary_value = st.text_input(
                primary_field_config.display_name,
                placeholder=primary_field_config.placeholder or f"Enter {primary_field_config.display_name.lower()}...",
                help=primary_field_config.help_text,
                key=f"{table_name}_primary"
            )

        if 'last_primary_value' not in st.session_state:
            st.session_state['last_primary_value'] = ''
        
        if primary_value != st.session_state['last_primary_value']:
            # Primary value changed, clear hierarchy-related session state
            keys_to_clear = [
                'hierarchy_match_name', 'hierarchy_match_type', 'show_hierarchy_form',
                'hierarchy_relationship_choice' 
            ]
            for key in keys_to_clear:
                st.session_state.pop(key, None)
            
            old_expander_key = f"hierarchy_expander_opened_{st.session_state['last_primary_value']}"
            old_new_inst_expander_key = f"new_hierarchy_expander_opened_{st.session_state['last_primary_value']}"
            st.session_state.pop(old_expander_key, None)
            st.session_state.pop(old_new_inst_expander_key, None)
            
            st.session_state['last_primary_value'] = primary_value
            
        
        if primary_value and len(str(primary_value).strip()) >= 3:
            

            exact = check_exact_duplicate(primary_value, existing_data, primary_field, standardization_data)
            if exact:
                if isinstance(exact, dict):
                    if exact['type'] == 'standardization':
                        st.error(f"'{primary_value}' already exists in the standardization table - standardized name is '{exact['standardized_name']}'")
                    else:
                        st.error(f"'{primary_value}' already exists in the {exact['source']}")
                else:
                    # Fallback for old string format
                    st.error(f"'{exact}' already exists in the {config.display_name.lower()} table.")
                
                st.session_state['hierarchy_match_name'] = exact
                st.session_state['hierarchy_match_type'] = 'exact'
                st.session_state['show_hierarchy_form'] = True
              
            try:
                fuzzy = check_fuzzy_matches(primary_value, existing_data, primary_field)
                if fuzzy:
                    st.info(f"Found {len(fuzzy)} similar {config.display_name.lower()}(s).")
                    # st.caption("Similar entries found. Click 'Keep' to use your entry and create a standardization mapping.")
                    if config.has_standardization:
                        st.caption("Similar entries found. If there is a true match click 'Keep' to create a standardization mapping between your entered name and the standardized name. Otherwise, proceed with institution form to enter a fully new institution.")
                    else:
                        st.caption("Similar entries found.")

        
                    for i, (name, score) in enumerate(fuzzy):
                        col1, col2 = st.columns([4, 1])
                        
                        with col1:
                            details = []
                            try:
                                match_row = existing_data[existing_data[primary_field] == name]
                                if not match_row.empty:
                                    if 'country_sub' in match_row.columns:
                                        country = match_row.iloc[0].get('country_sub', '')
                                        if country and str(country).strip():
                                            details.append(str(country))
                            except:
                                pass
                            
                            detail_str = f" ({', '.join(details)})" if details else ""
                            st.text(f"• {name}{detail_str} - {score * 100:.1f}% match")
                       
                        with col2:
                            if config.has_standardization:
                                if st.button("Keep", key=f"keep_{table_name}_{i}", help=f"Use '{primary_value}' and map to '{name}'"):
                                    if table_name == 'institution':
                                        result = standardization_service.process_keep_institution(primary_value, name, standardization_data, existing_data)
                                    elif table_name == 'geography':
                                        result = standardization_service.process_keep_geography(primary_value, name)
                                    else:
                                        result = {'success': False, 'message': 'Keep functionality not available for this table'}
                                    
                                    if result['success']:
                                        st.success(result['message'])
                                        st.info(f"Added to standardization table.")
    
                                        st.session_state['hierarchy_match_name'] = name  # The specific match that was kept
                                        st.session_state['hierarchy_match_type'] = 'kept'
                                        st.session_state['show_hierarchy_form'] = True
                                        
                                        st.rerun()
                                    else:
                                        st.error(result['message'])
                                
                                # st.rerun()
                            
                        
                            
            except Exception as e:
                st.error(f"Error checking for similar entries: {str(e)}")

        
            if table_name == 'institution' and st.session_state.get('hierarchy_match_name'):
                match_name = st.session_state['hierarchy_match_name']
                match_type = st.session_state.get('hierarchy_match_type', 'unknown')

                expander_state_key = f"hierarchy_expander_opened_{primary_value}"
                
                if expander_state_key not in st.session_state:
                    st.session_state[expander_state_key] = False
        
                
                with st.expander("Create Hierarchy Relationship", expanded=st.session_state[expander_state_key]):
                    if not st.session_state[expander_state_key]:
                        st.session_state[expander_state_key] = True

                    if match_type == 'exact':
                        st.info(f"Create a parent-child relationship with the existing institution: {match_name}")
                    elif match_type == 'fuzzy':
                        st.info(f"Create a parent-child relationship with the matched institution: {match_name}")
                    elif match_type == 'kept':
                        st.info(f"Create a parent-child relationship with the kept institution: {match_name}")
                    
                    if "match_hierarchy_choice" not in st.session_state:
                        st.session_state["match_hierarchy_choice"] = "As Parent Institution"
                    
                    relationship_choice = st.radio(
                        f"How should '{match_name}' be used in the hierarchy?",
                        ["As Parent Institution", "As Child Institution"],
                        index=["As Parent Institution", "As Child Institution"].index(st.session_state["match_hierarchy_choice"]),
                        key="match_hierarchy_radio",
                        help="Choose whether the matched institution should be parent or child"
                    )
                    
                    st.session_state["match_hierarchy_choice"] = relationship_choice
                                        
                    if relationship_choice == "As Parent Institution":
                        st.write(f"**{match_name}** will be the PARENT institution")
                        
                        child_name, child_id = render_institution_search_widget(
                            key="match_child",
                            label="Select Child Institution",
                            existing_institutions=existing_data,
                            help_text="Institution that will be owned/controlled by this parent"
                        )
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            percent_ownership = st.number_input(
                                "Ownership Percentage",
                                min_value=0.0,
                                max_value=1.0,
                                value=1.0,
                                step=0.01,
                                format="%.2f",
                                key="match_ownership",
                                help="Enter as decimal (e.g., 0.51 for 51%)"
                            )
                        
                        with col2:
                            is_controlling = st.checkbox(
                                "Is Controlling Institution",
                                value=percent_ownership > 0.5,
                                key="match_controlling",
                                help="Check if ownership percentage > 50%"
                            )
                        
                        relationship_type_text = st.text_input(
                            "Relationship Type",
                            placeholder="e.g., subsidiary, division, branch",
                            key="match_rel_type",
                            help="Describe the type of relationship"
                        )
                        
                        if child_name and child_id:
                            if st.button("Create Relationship", key="match_submit"):
                                hierarchy_service = HierarchyService()
                                result = hierarchy_service.create_hierarchy_entry(
                                    parent_institution=match_name,
                                    child_institution=child_name,
                                    percent_ownership=percent_ownership,
                                    relationship_type=relationship_type_text,
                                    user=st.session_state.get('username', 'analyst'),
                                    existing_institutions=existing_data,    
                                    existing_hierarchy=existing_hierarchy_data
                                )
                                
                                if result['success']:
                                    st.success("Hierarchy relationship created successfully!")
                                    st.cache_data.clear()
                                    for key in ['hierarchy_match_name', 'hierarchy_match_type', 'match_hierarchy_choice']:
                                        st.session_state.pop(key, None)
                                    keys_to_clear = [k for k in st.session_state.keys() if 'hierarchy' in k.lower() or 'match_' in k]
                                    for key in keys_to_clear:
                                        st.session_state.pop(key, None)
                                    st.rerun()
                                else:
                                    st.error(f"Failed to create hierarchy: {result['message']}")
                        else:
                            st.info("Please select a child institution to create the relationship")
                    
                    else:  # As Child Institution
                        st.write(f"**{match_name}** will be the CHILD institution")
                        
                        # Search for parent institution
                        parent_name, parent_id = render_institution_search_widget(
                            key="match_parent",
                            label="Select Parent Institution",
                            existing_institutions=existing_data,
                            help_text="Institution that owns/controls this child"
                        )
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            percent_ownership = st.number_input(
                                "Ownership Percentage",
                                min_value=0.0,
                                max_value=1.0,
                                value=1.0,
                                step=0.01,
                                format="%.2f",
                                key="match_child_ownership",
                                help="Enter as decimal (e.g., 0.51 for 51%)"
                            )
                        
                        with col2:
                            is_controlling = st.checkbox(
                                "Is Controlling Institution",
                                value=percent_ownership > 0.5,
                                key="match_child_controlling",
                                help="Check if ownership percentage > 50%"
                            )
                        
                        relationship_type_text = st.text_input(
                            "Relationship Type",
                            placeholder="e.g., subsidiary, division, branch",
                            key="match_child_rel_type",
                            help="Describe the type of relationship"
                        )
                        
                        if parent_name and parent_id:
                            if st.button("Create Relationship", key="match_child_submit"):
                                hierarchy_service = HierarchyService()
                                result = hierarchy_service.create_hierarchy_entry(
                                    parent_institution=parent_name,
                                    child_institution=match_name,
                                    percent_ownership=percent_ownership,
                                    relationship_type=relationship_type_text,
                                    user=st.session_state.get('username', 'analyst'),
                                    existing_institutions=existing_data,    
                                    existing_hierarchy=existing_hierarchy_data
                                )
                                
                                if result['success']:
                                    st.success("Hierarchy relationship created successfully!")
                                    st.cache_data.clear()
                                    # Clear hierarchy session state
                                    for key in ['hierarchy_match_name', 'hierarchy_match_type', 'match_hierarchy_choice']:
                                        st.session_state.pop(key, None)
                                    keys_to_clear = [k for k in st.session_state.keys() if 'hierarchy' in k.lower() or 'match_' in k]
                                    for key in keys_to_clear:
                                        st.session_state.pop(key, None)
                                    st.rerun()
                                else:
                                    st.error(f"Failed to create hierarchy: {result['message']}")
                        else:
                            st.info("Please select a parent institution to create the relationship")
                    
    
    # Auto-lookup button (only for institution table)
    if table_name == 'institution' and primary_value and len(str(primary_value).strip()) >= 3:
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("Auto-Lookup", key="lookup_btn", help="Automatically find institution details from trusted sources"):
                with st.spinner("Searching trusted sources and extracting data..."):
                    try:
                        
                        valid_countries = set()
                        if not existing_data.empty:
                            if 'country_sub' in existing_data.columns:
                                valid_countries.update(existing_data['country_sub'].dropna().unique())
                            if 'country_parent' in existing_data.columns:
                                valid_countries.update(existing_data['country_parent'].dropna().unique())
                        
                        lookup_service = InstitutionLookupService(valid_countries=list(valid_countries))
                        result = lookup_service.lookup_institution(primary_value)
                        
                        st.session_state['lookup_result'] = result
                        st.session_state['lookup_used'] = False
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Lookup failed: {str(e)}")
    
   

    if table_name == 'institution' and st.session_state.get('lookup_result') and not st.session_state.get('lookup_used', False):
        lookup_result = st.session_state['lookup_result']
        confidence = lookup_result.confidence_score
        
        if confidence >= 0.9:
            st.success(f"High confidence match ({confidence * 100:.0f}%)")
        elif confidence >= 0.7:
            st.info(f"Good match found ({confidence * 100:.0f}%)")
        else:
            st.warning(f"Low confidence match ({confidence * 100:.0f}%) - Please verify")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Institution Details:**")
            if lookup_result.institution_type_layer1:
                st.write(f"• **Type Layer 1:** {lookup_result.institution_type_layer1}")
            if lookup_result.institution_type_layer2:
                st.write(f"• **Type Layer 2:** {lookup_result.institution_type_layer2}")
            if lookup_result.institution_type_layer3:
                st.write(f"• **Type Layer 3:** {lookup_result.institution_type_layer3}")
        
        with col2:
            st.markdown("**Geographic Details:**")
            if lookup_result.parent_country:
                st.write(f"• **Parent Country:** {lookup_result.parent_country}")
            if lookup_result.subsidiary_country:
                st.write(f"• **Subsidiary Country:** {lookup_result.subsidiary_country}")
        
        if lookup_result.reasoning:
            with st.expander("Why these values?", expanded=False):
                st.write(lookup_result.reasoning)
        
        render_lookup_sources_compact(lookup_result)
        
        col1, col2, col3 = st.columns([2, 1, 2])
        with col2:
            if st.button("Use These Values", key="use_lookup", type="primary", use_container_width=True):
                keys_to_clear = []
                for key in list(st.session_state.keys()):
                    if (key.startswith(f"{table_name}_req_") or 
                        key.startswith(f"{table_name}_opt_") or 
                        key.startswith(f"{table_name}_adv_")):
                        keys_to_clear.append(key)
                
                for key in keys_to_clear:
                    st.session_state.pop(key, None)
                
                for key in ['prefill_type1', 'prefill_type2', 'prefill_type3', 'prefill_parent', 'prefill_sub']:
                    st.session_state.pop(key, None)
                
                st.session_state['prefill_type1'] = lookup_result.institution_type_layer1
                st.session_state['prefill_type2'] = lookup_result.institution_type_layer2
                st.session_state['prefill_type3'] = lookup_result.institution_type_layer3
                st.session_state['prefill_parent'] = lookup_result.parent_country
                st.session_state['prefill_sub'] = lookup_result.subsidiary_country
                st.session_state['lookup_used'] = True
                
                st.rerun()
    
    # st.markdown("---")
    # st.subheader(f"{config.display_name} Details")
    
    # Create the main form
    with st.form(key=f"{table_name}_entry_form", clear_on_submit=False):
        form_data = {}
        if primary_field_config:
            form_data[primary_field] = primary_value
        
        remaining_fields = [f for f in config.fields if f.name != primary_field]
        required_fields = [f for f in remaining_fields if f.required and getattr(f, 'category', 'main') == 'main']
        optional_main_fields = [f for f in remaining_fields if not f.required and getattr(f, 'category', 'main') == 'main']
        advanced_fields = [f for f in remaining_fields if getattr(f, 'category', 'main') == 'advanced']
        
        if required_fields:
            cols = st.columns(2)
            for i, field_config in enumerate(required_fields):
                with cols[i % 2]:
                    help_key = f"help_{field_config.name}_req_{i}"
                
                    field_col, toggle_col = st.columns([0.8, 0.2])
                    
                    with field_col:
                        form_data[field_config.name] = render_form_field(
                            field_config,
                            dropdown_options,
                            f"{table_name}_req_{i}",
                            existing_data
                        )
                    
                    with toggle_col:
                        if getattr(field_config, "detailed_help", None):
                            show_help = st.toggle(
                                "Help",
                                key=help_key,
                                label_visibility="collapsed"
                            )
                        else:
                            show_help = False
        
                    if getattr(field_config, "detailed_help", None) and show_help:
                        st.markdown(field_config.detailed_help)
                        # st.info(field_config.detailed_help)
                                
                   
        
        if optional_main_fields:
            st.subheader("Secondary Fields")
            cols = st.columns(2)
            for i, field_config in enumerate(optional_main_fields):
                with cols[i % 2]:
                    field_col, help_col = st.columns([0.9, 0.1])
                    
                    with field_col:
                        form_data[field_config.name] = render_form_field(field_config, dropdown_options, f"{table_name}_opt_{i}", existing_data)
                    
                    with help_col:
                        if hasattr(field_config, 'detailed_help') and field_config.detailed_help:
                            with st.expander("+", expanded=False):
                                st.markdown(field_config.detailed_help)
                # with cols[i % 2]:
                #     form_data[field_config.name] = render_form_field(field_config, dropdown_options, f"{table_name}_opt_{i}", existing_data)
        
        if advanced_fields:
            with st.expander("Additional Information"):
                cols = st.columns(2)
                for i, field_config in enumerate(advanced_fields):
                    with cols[i % 2]:
                        form_data[field_config.name] = render_form_field(field_config, dropdown_options, f"{table_name}_adv_{i}", existing_data)

        # Real-time compound duplicate checking for tables with multiple duplicate check fields
        # Note: This is now inside the form but still works for validation
        compound_duplicate_error = None
        if config.duplicate_check_fields and len(config.duplicate_check_fields) > 1:
            # Check if we have values for multiple duplicate check fields
            compound_check_values = {}
            for field_name in config.duplicate_check_fields:
                value = form_data.get(field_name)
                if value is not None and str(value).strip():
                    compound_check_values[field_name] = value
            
            # Only check if we have at least 2 values filled out
            if len(compound_check_values) >= 2:
                compound_duplicate = check_compound_duplicate(form_data, existing_data, config.duplicate_check_fields)
                if compound_duplicate:
                    compound_duplicate_error = f"Duplicate entry found: {compound_duplicate}"
                    st.error(compound_duplicate_error)
                    st.caption("This exact combination of values already exists in the database.")

        # Form submit button
        st.markdown("---")
        
        # Create two columns for submit button layout
        col1, col3 = st.columns([2, 1])
        
        with col1:
            form_submitted = st.form_submit_button(f"Add {config.display_name}", type="primary", use_container_width=True)
        
        # Handle form submission
        if form_submitted:
            # Check for validation errors
            missing_required = []
            for field_name in config.required_fields:
                if not form_data.get(field_name) or str(form_data[field_name]).strip() == '':
                    field_display = next((f.display_name for f in config.fields if f.name == field_name), field_name)
                    missing_required.append(field_display)
            
            # Don't proceed if there are validation errors
            if missing_required:
                st.error(f"Please fill in required fields: {', '.join(missing_required)}")
            elif compound_duplicate_error:
                # Already displayed above, just don't proceed
                pass
            else:
                # Get hierarchy data if it exists (for institution table)
                hierarchy_form_data = None
                if table_name == 'institution':
                    # Check if hierarchy form data was set outside the form
                    hierarchy_form_data = st.session_state.get('current_hierarchy_form_data')
                
                # Process the submission
                with st.spinner(f"Creating {config.display_name.lower()}..."):
                    result = create_table_entry(
                        table_name,
                        form_data,
                        user=st.session_state.get('username', 'analyst')
                    )
                    
                    if result['success']:
                        st.success(f"{config.display_name} created successfully!")
        
                        # Handle hierarchy creation for institutions
                        if table_name == 'institution' and hierarchy_form_data:
                            with st.spinner("Creating hierarchy relationship..."):
                                time.sleep(1)  # Brief delay to ensure institution is in database

                                if 'new_id' in result:
                                    new_institution_id = result['new_id']
                                else:
                                    # Calculate what the new ID would be so we don't have to reload/cache institutions
                                    try:   
                                        max_id_query = "SELECT MAX(id_institution_cpi) as max_id FROM institution"
                                        max_id_result = QueryService().execute_query(max_id_query)
                                        if not max_id_result.empty and max_id_result.iloc[0]['max_id'] is not None:
                                            new_institution_id = int(max_id_result.iloc[0]['max_id'])
                                        else:
                                            new_institution_id = 1  # First institution
                                    except:
                                        st.error("Could not determine new institution ID")
                                        new_institution_id = None
                                
                                if new_institution_id:
                                    hierarchy_service = HierarchyService()

                                    if hierarchy_form_data['mode'] == 'new_as_parent':
                                        # New institution is parent, use existing child from search
                                        hierarchy_result = hierarchy_service.create_hierarchy_entry_direct(
                                            parent_id=new_institution_id,
                                            parent_name=form_data[primary_field],  # User's input
                                            child_id=hierarchy_form_data['child_id'],
                                            child_name=hierarchy_form_data['child_institution'],
                                            percent_ownership=hierarchy_form_data.get('percent_ownership'),
                                            relationship_type=hierarchy_form_data.get('relationship_type'),
                                            user=st.session_state.get('username', 'analyst')
                                        )
                                    elif hierarchy_form_data['mode'] == 'new_as_child':
                                        # New institution is child, use existing parent from search
                                        hierarchy_result = hierarchy_service.create_hierarchy_entry_direct(
                                            parent_id=hierarchy_form_data['parent_id'],
                                            parent_name=hierarchy_form_data['parent_institution'],
                                            child_id=new_institution_id,
                                            child_name=form_data[primary_field],  # User's input
                                            percent_ownership=hierarchy_form_data.get('percent_ownership'),
                                            relationship_type=hierarchy_form_data.get('relationship_type'),
                                            user=st.session_state.get('username', 'analyst')
                                        )
                                    
                                    if hierarchy_result['success']:
                                        st.success("Hierarchy relationship created successfully!")
                                    else:
                                        st.error(f"Hierarchy creation failed: {hierarchy_result['message']}")
                                        
                        # Clean up session state
                        st.session_state[f'_cache_needs_clear'] = True
                        
                        if table_name == 'institution':
                            st.session_state.pop('lookup_result', None)
                            st.session_state.pop('lookup_used', None)
                            for key in ['prefill_type1', 'prefill_type2', 'prefill_type3', 'prefill_parent', 'prefill_sub']:
                                st.session_state.pop(key, None)
                            # Clear hierarchy form keys
                            for key in ['new_relationship_choice', 'new_ownership', 'new_controlling', 'new_rel_type', 'new_child_ownership', 'new_child_controlling', 'new_child_rel_type']:
                                st.session_state.pop(key, None)
                            # Clear current hierarchy form data
                            st.session_state.pop('current_hierarchy_form_data', None)
                    else:
                        st.error(result['message'])
    
    

    
    # Add hierarchy form for new institutions (outside the main form)
    if table_name == 'institution':
        # Clear any previous hierarchy form data when primary value changes
        if primary_value != st.session_state.get('last_hierarchy_primary_value'):
            st.session_state.pop('current_hierarchy_form_data', None)
            st.session_state['last_hierarchy_primary_value'] = primary_value

        new_inst_expander_key = f"new_hierarchy_expander_opened_{primary_value}"
    
        if new_inst_expander_key not in st.session_state:
            st.session_state[new_inst_expander_key] = False
        
        with st.expander("Add Hierarchy Relationship (Optional)", expanded=st.session_state[new_inst_expander_key]):
            st.write("Create a parent-child relationship for this new institution")
            
            if "hierarchy_relationship_choice" not in st.session_state:
                st.session_state["hierarchy_relationship_choice"] = "No Relationship"
            
            relationship_choice = st.radio(
                "How should this new institution be related?",
                ["No Relationship", "As Parent Institution", "As Child Institution"],
                index=["No Relationship", "As Parent Institution", "As Child Institution"].index(st.session_state["hierarchy_relationship_choice"]),
                key="hierarchy_radio",
                help="Choose the role of this new institution in the hierarchy"
            )
            
            st.session_state["hierarchy_relationship_choice"] = relationship_choice
            
            if relationship_choice != "No Relationship":
                
                if relationship_choice == "As Parent Institution":
                    st.write(f"**{primary_value}** will be the PARENT institution")
                    
                    # Search for child institution
                    child_name, child_id = render_institution_search_widget(
                        key="new_child",
                        label="Select Child Institution",
                        existing_institutions=existing_data,
                        help_text="Institution that will be owned/controlled by this new parent"
                    )
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        percent_ownership = st.number_input(
                            "Ownership Percentage",
                            min_value=0.0,
                            max_value=1.0,
                            value=1.0,
                            step=0.01,
                            format="%.2f",
                            key="new_ownership"
                        )
                    
                    with col2:
                        is_controlling = st.checkbox(
                            "Is Controlling",
                            value=percent_ownership > 0.5,
                            key="new_controlling"
                        )
                    
                    relationship_type_text = st.text_input(
                        "Relationship Type",
                        placeholder="e.g., subsidiary, division",
                        key="new_rel_type"
                    )
                    
                    # Store hierarchy data in session state for form submission
                    if child_name and child_id:
                        st.session_state['current_hierarchy_form_data'] = {
                            'parent_institution': primary_value,
                            'child_institution': child_name,
                            'child_id': child_id,
                            'percent_ownership': percent_ownership,
                            'is_controlling_institution': is_controlling,
                            'relationship_type': relationship_type_text,
                            'mode': 'new_as_parent'
                        }
                        st.info(f"Hierarchy relationship configured: {primary_value} → {child_name}")
                    else:
                        st.session_state.pop('current_hierarchy_form_data', None)
                
                else:  # As Child Institution
                    st.write(f"**{primary_value}** will be the CHILD institution")
                    
                    parent_name, parent_id = render_institution_search_widget(
                        key="new_parent",
                        label="Select Parent Institution",
                        existing_institutions=existing_data,
                        help_text="Institution that owns/controls this new child"
                    )
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        percent_ownership = st.number_input(
                            "Ownership Percentage",
                            min_value=0.0,
                            max_value=1.0,
                            value=1.0,
                            step=0.01,
                            format="%.2f",
                            key="new_child_ownership"
                        )
                    
                    with col2:
                        is_controlling = st.checkbox(
                            "Is Controlling",
                            value=percent_ownership > 0.5,
                            key="new_child_controlling"
                        )
                    
                    relationship_type_text = st.text_input(
                        "Relationship Type",
                        placeholder="e.g., subsidiary, division",
                        key="new_child_rel_type"
                    )
                    
                    # Store hierarchy data in session state for form submission
                    if parent_name and parent_id:
                        st.session_state['current_hierarchy_form_data'] = {
                            'parent_institution': parent_name,
                            'parent_id': parent_id,
                            'child_institution': primary_value,
                            'percent_ownership': percent_ownership,
                            'is_controlling_institution': is_controlling,
                            'relationship_type': relationship_type_text,
                            'mode': 'new_as_child'
                        }
                        st.info(f"Hierarchy relationship configured: {parent_name} → {primary_value}")
                    else:
                        st.session_state.pop('current_hierarchy_form_data', None)
            else:
                # No relationship selected, clear any stored data
                st.session_state.pop('current_hierarchy_form_data', None)
    
   
    
   



def render_unified_bulk_upload(table_name: str):
    """
    Enhanced unified bulk upload interface with inline fuzzy matching
    """
    config = get_table_config(table_name)
    if not config:
        st.error(f"No configuration found for table: {table_name}")
        return

    reference_data = get_table_reference_data(table_name, config)
    existing_data = reference_data['existing_data']
    dropdown_options = reference_data['dropdown_options']
    standardization_data = reference_data.get('standardization_data')
    
    st.subheader(f"Bulk Upload to {config.display_name} Table")
    st.markdown(config.description)
    
    session_key = f'bulk_upload_{table_name}'
    init_bulk_upload_session_state(session_key)
    
    render_template_download(table_name, config)
    
    st.markdown("---")
    
    uploaded_file = st.file_uploader(
        "Choose CSV or Excel file",
        type=['csv', 'xlsx', 'xls'],
        key=f"upload_{table_name}"
    )
    
    if uploaded_file is not None:
        df = process_uploaded_file(uploaded_file, config, session_key)
        
        if df is not None:
            validation_results = run_bulk_validation(df, table_name, config, session_key, existing_data)
            
            if validation_results:
                render_enhanced_bulk_upload_grid(validation_results, config, session_key, table_name, existing_data)


def render_enhanced_bulk_upload_grid(validation_results: List[ValidationResult], config: TableConfig, session_key: str, table_name: str, existing_data: pd.DataFrame):
    """Enhanced bulk upload grid with inline fuzzy matching"""
    valid_results = [r for r in validation_results if r.status == 'valid']
    fuzzy_results = [r for r in validation_results if r.status == 'fuzzy_match']
    duplicate_results = [r for r in validation_results if r.status == 'duplicate']
    error_results = [r for r in validation_results if r.status == 'missing_required']
    
    visible_count = len([r for r in (valid_results + fuzzy_results) 
                        if st.session_state[f'{session_key}_user_decisions'].get(r.row_index) != 'skip'])
    
    pending_mappings = len(st.session_state.get(f'{session_key}_pending_mappings', {}))

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("Total Rows", len(validation_results))
    with col2:
        st.metric("Reviewing", visible_count)
    with col3:
        st.metric("Similar Matches", len(fuzzy_results))
    with col4:
        st.metric("Duplicates", len(duplicate_results))
    with col5:
        insert_count = sum(1 for d in st.session_state[f'{session_key}_user_decisions'].values() if d == 'insert')
        st.metric("Will Insert", insert_count)
    with col6:
        st.metric("Pending Maps", pending_mappings)
    
    st.markdown("---")
    
    if duplicate_results:
        with st.expander(f"Duplicates ({len(duplicate_results)}) - Click to review"):
            for result in duplicate_results:
                primary_field = config.required_fields[0] if config.required_fields else config.fields[0].name
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.text(result.data.get(primary_field, 'N/A'))
                with col2:
                    st.caption(f"Matches: {result.fuzzy_matches[0][0] if result.fuzzy_matches else 'Unknown'}")
                with col3:
                    if st.button("Keep Anyway", key=f"keep_dup_{result.row_index}"):
                        result.status = 'valid'
                        st.session_state[f'{session_key}_user_decisions'][result.row_index] = 'insert'
                        st.rerun()
    
    if error_results:
        with st.expander(f"Errors ({len(error_results)}) - Missing required data"):
            for result in error_results:
                st.error(f"Row {result.row_index + 1}: {result.issues[0]}")
    
    st.subheader(f"Review & Edit Records ({visible_count} rows)")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Auto-Lookup All Missing Data", use_container_width=True):
            run_batch_lookup(valid_results + fuzzy_results, table_name, session_key, existing_data)
    with col2:
        if st.button("Skip All with Similar Matches", use_container_width=True):
            for result in fuzzy_results:
                st.session_state[f'{session_key}_user_decisions'][result.row_index] = 'skip'
            st.rerun()
    with col3:
        if st.button("Reset All Decisions", use_container_width=True):
            st.session_state[f'{session_key}_user_decisions'] = {
                r.row_index: r.suggested_action for r in validation_results
            }
            st.rerun()
    
    st.markdown("---")
    
    render_enhanced_grid_header(config)
    
    rows_to_show_fuzzy = [r for r in fuzzy_results if st.session_state[f'{session_key}_user_decisions'].get(r.row_index) != 'skip']
    rows_to_show_valid = [r for r in valid_results if st.session_state[f'{session_key}_user_decisions'].get(r.row_index) != 'skip']
    
    all_to_display = rows_to_show_fuzzy + rows_to_show_valid
    
    total_rows = len(all_to_display)
    rows_per_page = 50
    total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)
    
    if total_rows > rows_per_page:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            current_page = st.number_input(
                f"Page (showing {rows_per_page} rows per page)",
                min_value=1,
                max_value=max(1, total_pages),
                value=1,
                key=f"{session_key}_page"
            )
        
        start_idx = (current_page - 1) * rows_per_page
        end_idx = start_idx + rows_per_page
        paginated_rows = all_to_display[start_idx:end_idx]
        
        st.info(f"Showing rows {start_idx + 1}-{min(end_idx, total_rows)} of {total_rows}")
    else:
        paginated_rows = all_to_display
    
    for result in paginated_rows:
        render_enhanced_grid_row(result, config, session_key, table_name, existing_data)
    
    # Upload button
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Upload to Database", type="primary", use_container_width=True):
            execute_unified_bulk_insert(validation_results, config, session_key, table_name)


def render_enhanced_grid_header(config: TableConfig):
    """Enhanced grid header - dynamic based on table configuration"""
    st.markdown("""
    <style>
    .institution-name {
        font-size: 15px;
        color: #000000;
        font-weight: 500;
        padding: 8px 0;
    }
    .fuzzy-row {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 8px;
        margin: 4px 0;
        border-radius: 4px;
    }
    </style>
    """, unsafe_allow_html=True)
    
    main_fields = [f for f in config.fields if f.category == 'main' and f.name not in ['created_by', 'created_at']]
    
    primary_field = next((f for f in config.fields if f.name in config.required_fields), config.fields[0])
    if primary_field not in main_fields:
        main_fields.insert(0, primary_field)

    #Limited to 6 fields for now, may need to change later if some forms need more than that, but will make ui clunky
    display_fields = main_fields[:6]
    
    if len(display_fields) <= 3:
        col_widths = [3] + [2] * len(display_fields) + [1, 0.5]  
    else:
        col_widths = [2] + [1.5] * len(display_fields) + [0.5, 0.3]  
    
    cols = st.columns(col_widths)
    
    for i, field in enumerate(display_fields):
        with cols[i]:
            st.markdown(f"**{field.display_name}**")
    
    with cols[len(display_fields)]:
        st.markdown("**Lookup**")
    with cols[len(display_fields) + 1]:
        st.markdown("")


def render_enhanced_grid_row(result: ValidationResult, config: TableConfig, session_key: str, table_name: str, existing_data: pd.DataFrame):
    """Enhanced grid row with slim blue info box for fuzzy matches"""
    
    if f'{session_key}_edited_data' not in st.session_state:
        st.session_state[f'{session_key}_edited_data'] = {}
    
    if result.row_index not in st.session_state[f'{session_key}_edited_data']:
        st.session_state[f'{session_key}_edited_data'][result.row_index] = result.data.copy()
    
    row_data = st.session_state[f'{session_key}_edited_data'][result.row_index]
    
    is_fuzzy_match = result.status == 'fuzzy_match' and result.fuzzy_matches
    
    container = st.container()
    
    with container:
        if is_fuzzy_match:
            col1, col2 = st.columns([4, 1])
            with col1:
                matches_text = ', '.join([f'{name} ({score*100:.0f}%)' for name, score in result.fuzzy_matches[:3]])
                st.info(f"**Similar institutions found:** {matches_text}")
            with col2:
                if st.button("Match", key=f"match_btn_{result.row_index}", help="Select which institution to map to"):
                    st.session_state[f'show_match_dropdown_{result.row_index}'] = True
                    st.rerun()
        
        if st.session_state.get(f'show_match_dropdown_{result.row_index}', False):
            match_options = [f"{name} ({score*100:.0f}%)" for name, score in result.fuzzy_matches]
            
            selected_match = st.selectbox(
                "Select institution to map to:",
                match_options,
                key=f"match_select_{result.row_index}"
            )
            
            col_confirm, col_cancel = st.columns(2)
            
            with col_confirm:
                if st.button("Confirm", key=f"confirm_match_{result.row_index}", type="primary"):
                    selected_match_name = selected_match.split(' (')[0]  # Remove the percentage part
                    user_input = row_data.get(config.required_fields[0] if config.required_fields else config.fields[0].name, '')
                    
                    st.session_state[f'{session_key}_pending_mappings'][result.row_index] = {
                        'user_input': user_input,
                        'matched_name': selected_match_name,
                        'table_type': 'institution' if table_name == 'institution' else 'geography'
                    }
                    
                    st.session_state[f'{session_key}_user_decisions'][result.row_index] = 'skip'
                    
                    st.session_state[f'show_match_dropdown_{result.row_index}'] = False
                    
                    st.success(f"Mapping queued: {user_input} → {selected_match_name}")
                    st.rerun()
            
            with col_cancel:
                if st.button("Cancel", key=f"cancel_match_{result.row_index}"):
                    st.session_state[f'show_match_dropdown_{result.row_index}'] = False
                    st.rerun()
        
        main_fields = [f for f in config.fields if f.category == 'main' and f.name not in ['created_by', 'created_at']]
        
        primary_field = next((f for f in config.fields if f.name in config.required_fields), config.fields[0])
        if primary_field not in main_fields:
            main_fields.insert(0, primary_field)
        
        # Limit to 6 fields for display + lookup button + action button
        display_fields = main_fields[:6]
        
        if len(display_fields) <= 3:
            col_widths = [3] + [2] * len(display_fields) + [1, 0.5] 
        else:
            col_widths = [2] + [1.5] * len(display_fields) + [0.5, 0.3]  
        
        cols = st.columns(col_widths)
        
        dropdown_options = get_table_dropdown_options(table_name, config, existing_data)
        
        lookup_result = st.session_state.get(f'{session_key}_lookup_results', {}).get(result.row_index)
        
        with cols[0]:
            name_display = row_data.get(config.required_fields[0] if config.required_fields else config.fields[0].name, '')
            if is_fuzzy_match:
                name_display = f"{name_display}"
            st.markdown(f"<div class='institution-name'>{name_display}</div>", unsafe_allow_html=True)
        
        # Render the other fields dynamically
        for i, field in enumerate(display_fields[1:], 1): 
            if i < len(cols) - 2:  
                with cols[i]:
                    current_value = row_data.get(field.name, '')
                    
                    if table_name == 'institution' and lookup_result:
                        if field.name == 'institution_type_layer1' and hasattr(lookup_result, 'institution_type_layer1'):
                            current_value = current_value or lookup_result.institution_type_layer1 or ''
                        elif field.name == 'institution_type_layer2' and hasattr(lookup_result, 'institution_type_layer2'):
                            current_value = current_value or lookup_result.institution_type_layer2 or ''
                        elif field.name == 'institution_type_layer3' and hasattr(lookup_result, 'institution_type_layer3'):
                            current_value = current_value or lookup_result.institution_type_layer3 or ''
                        elif field.name == 'country_sub' and hasattr(lookup_result, 'subsidiary_country'):
                            current_value = current_value or lookup_result.subsidiary_country or ''
                        elif field.name == 'country_parent' and hasattr(lookup_result, 'parent_country'):
                            current_value = current_value or lookup_result.parent_country or ''
                    
                    if field.field_type == 'select':
                        options = dropdown_options.get(field.name, [''])
                        idx = options.index(current_value) if current_value in options else 0
                        
                        new_val = st.selectbox(
                            field.name,
                            options,
                            index=idx,
                            key=f"{field.name}_{result.row_index}_{session_key}",
                            label_visibility="collapsed"
                        )
                        
                        if new_val != row_data.get(field.name):
                            st.session_state[f'{session_key}_edited_data'][result.row_index][field.name] = new_val
                    
                    elif field.field_type == 'text':
                        new_val = st.text_input(
                            field.name,
                            value=current_value,
                            key=f"{field.name}_{result.row_index}_{session_key}",
                            label_visibility="collapsed"
                        )
                        
                        if new_val != row_data.get(field.name):
                            st.session_state[f'{session_key}_edited_data'][result.row_index][field.name] = new_val
                    
                    elif field.field_type == 'number':
                        try:
                            current_numeric = float(current_value) if current_value and str(current_value).strip() else 0.0
                        except:
                            current_numeric = 0.0
                            
                        new_val = st.number_input(
                            field.name,
                            value=current_numeric,
                            key=f"{field.name}_{result.row_index}_{session_key}",
                            label_visibility="collapsed"
                        )
                        
                        if new_val != row_data.get(field.name):
                            st.session_state[f'{session_key}_edited_data'][result.row_index][field.name] = new_val
                    
                    else:
                        st.text(current_value or '')

        lookup_col_index = len(display_fields) if len(display_fields) < len(cols) - 1 else len(cols) - 2
        with cols[lookup_col_index]:
            if table_name == 'institution':
                button_col, info_col = st.columns([1, 1])
                
                with button_col:
                    if st.button("🔍", key=f"lookup_btn_{result.row_index}_{session_key}", help="Auto-lookup"):
                        run_single_lookup(result, table_name, session_key, existing_data)
                
                with info_col:
                    if lookup_result and lookup_result.sources:
                        if hasattr(st, 'popover'):
                            with st.popover("📋", help=f"View {len(lookup_result.sources)} sources"):
                                st.caption("**Sources used for lookup:**")
                                for idx, source in enumerate(lookup_result.sources[:5], 1):  # Show top 5
                                    title = source.get('title', 'Source')
                                    if len(title) > 40:
                                        title = title[:37] + "..."
                                    url = source.get('url', '#')
                                    st.markdown(f"[{idx}. {title}]({url})")
                                
                                if len(lookup_result.sources) > 3:
                                    st.caption(f"... and {len(lookup_result.sources) - 3} more sources")
                                
                                confidence = getattr(lookup_result, 'confidence_score', None)
                                if confidence:
                                    st.caption(f"Confidence: {confidence*100:.0f}%")
                        else:
                            if st.button("📋", key=f"sources_btn_{result.row_index}_{session_key}", 
                                       help=f"Click to see {len(lookup_result.sources)} sources"):
                                st.session_state[f'show_sources_{result.row_index}_{session_key}'] = True
                                st.rerun()
                    elif lookup_result:
                        confidence = getattr(lookup_result, 'confidence_score', None)
                        if confidence:
                            confidence_color = "🟢" if confidence >= 0.8 else "🟡" if confidence >= 0.6 else "🔴"
                            st.caption(f"{confidence_color} {confidence*100:.0f}%")
            else:
                st.markdown("")

        
        # Action button
        action_col_index = len(display_fields) + 1 if len(display_fields) < len(cols) - 1 else len(cols) - 1
        with cols[action_col_index]:
            # Discard button (X) - also removes pending mappings
            if st.button("✕", key=f"discard_row_{result.row_index}_{session_key}", help="Remove this row"):
                st.session_state[f'{session_key}_user_decisions'][result.row_index] = 'skip'
                
                if result.row_index in st.session_state.get(f'{session_key}_pending_mappings', {}):
                    del st.session_state[f'{session_key}_pending_mappings'][result.row_index]
                
                if f'show_match_dropdown_{result.row_index}' in st.session_state:
                    del st.session_state[f'show_match_dropdown_{result.row_index}']
                st.rerun()
        
        st.markdown("---")



        
def init_bulk_upload_session_state(session_key: str):
    for key in ['df', 'validation_results', 'edited_data', 'user_decisions', 'upload_complete', 'upload_results', 'pending_mappings']:
        if f'{session_key}_{key}' not in st.session_state:
            if key == 'pending_mappings':
                st.session_state[f'{session_key}_{key}'] = {}
            elif key != 'edited_data' and key != 'user_decisions':
                st.session_state[f'{session_key}_{key}'] = None
            else:
                st.session_state[f'{session_key}_{key}'] = {}


def render_template_download(table_name: str, config: TableConfig):
    """General rules for configuring example excel download with example values"""
    with st.expander("Download Template"):
        template_data = {}
        for field_config in config.fields:
            # Generate meaningful examples based on field type and name
            if field_config.field_type == 'number':
                if 'year' in field_config.name.lower() or field_config.name in ['last_verified', 'created_at', 'year_added', 'year_of_analysis']:
                    example_value = 2025
                elif field_config.name in ['gearing', 'multiplier_local', 'multiplier_usd', 'fx_rate', 'conversion_rate']:
                    example_value = 1.5
                elif field_config.name in ['m49_code', 'iso_numeric_code']:
                    example_value = 840  # Example numeric code
                else:
                    example_value = 1.0
            elif field_config.field_type == 'boolean':
                example_value = 'True'
            elif field_config.field_type == 'select':
                example_value = f'Example {field_config.display_name}'
            elif field_config.field_type == 'textarea':
                if field_config.name in ['definition', 'description']:
                    example_value = f'Description of this {field_config.display_name.lower()}'
                elif field_config.name in ['comments', 'notes']:
                    example_value = 'Additional notes or comments'
                elif field_config.name in 'contact_info':
                    example_value = 'Emails, names, etc.'
                else:
                    example_value = f'Example {field_config.display_name}'
            else:  # text fields
                if 'country' in field_config.name.lower():
                    example_value = 'United States of America'
                elif 'country_sub' in field_config.name.lower():
                    example_value = 'United States of America'
                elif 'country_parent' in field_config.name.lower():
                    example_value = 'United States of America'
                elif 'currency' in field_config.name.lower():
                    example_value = 'USD'
                elif 'institution_cpi' in field_config.name.lower():
                    example_value = 'AES Corp'
                elif 'institution_cpi_short' in field_config.name.lower():
                    example_value = 'AES'
                elif 'institution_original' in field_config.name.lower():
                    example_value = 'AES'
                elif 'institution_type_layer1' in field_config.name.lower():
                    example_value = 'Private'
                elif 'institution_type_layer2' in field_config.name.lower():
                    example_value = 'Corporation'
                elif 'institution_type_layer3' in field_config.name.lower():
                    example_value = 'Corporate'
                elif field_config.name in ['sector_re', 'sub_sector_source', 'sub_sector_bnef']:
                    example_value = 'Solar'
                elif field_config.name in ['instrument_original', 'instrument_type']:
                    example_value = 'Green Bond'
                elif field_config.name in ['iso2_code']:
                    example_value = 'US'
                elif field_config.name in ['iso3_code']:
                    example_value = 'USA'
                else:
                    example_value = f'Example {field_config.display_name}'

            
            template_data[field_config.name] = [example_value, '']
        
        template_df = pd.DataFrame(template_data)
        
        col1, col2 = st.columns(2)
        with col1:
            csv_buffer = io.StringIO()
            template_df.to_csv(csv_buffer, index=False)
            st.download_button(
                "CSV Template",
                csv_buffer.getvalue(),
                f"{table_name}_template.csv",
                "text/csv",
                use_container_width=True
            )
        with col2:
            excel_buffer = io.BytesIO()
            template_df.to_excel(excel_buffer, index=False, engine='openpyxl')
            st.download_button(
                "Excel Template",
                excel_buffer.getvalue(),
                f"{table_name}_template.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )


def process_uploaded_file(uploaded_file, config: TableConfig, session_key: str) -> Optional[pd.DataFrame]:
    """Process uploaded file and validate columns"""
    if st.session_state[f'{session_key}_df'] is None or uploaded_file.name != st.session_state.get(f'{session_key}_last_file'):
        with st.spinner("Loading file..."):
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file, engine='c')
                else:
                    df = pd.read_excel(uploaded_file, engine='openpyxl')
                
                df.columns = df.columns.str.strip()
                df = df.where(pd.notna(df), None)
                
                missing_columns = [field for field in config.required_fields if field not in df.columns]
                if missing_columns:
                    st.error(f"Missing required columns: {', '.join(missing_columns)}")
                    return None
                
                st.session_state[f'{session_key}_df'] = df
                st.session_state[f'{session_key}_last_file'] = uploaded_file.name
                st.session_state[f'{session_key}_validation_results'] = None
                
                return df
                
            except Exception as e:
                st.error(f"Error parsing file: {str(e)}")
                return None
    
    return st.session_state[f'{session_key}_df']


def run_bulk_validation(df: pd.DataFrame, table_name: str, config: TableConfig, session_key: str, existing_data: pd.DataFrame) -> Optional[List[ValidationResult]]:
    """Run validation with duplicate checking on bulk upload"""
    if st.session_state[f'{session_key}_validation_results'] is None:
        with st.spinner("Validating entries and checking for duplicates..."):
            if existing_data.empty:
                existing_data = get_table_data_cached(table_name, limit=None)
            primary_field = config.required_fields[0] if config.required_fields else config.fields[0].name
            
            validation_results = []
            
            for idx, row in df.iterrows():
                row_data = row.to_dict()
                
                result = validate_bulk_row(row_data, idx, existing_data, primary_field, config)
                validation_results.append(result)
            
            st.session_state[f'{session_key}_validation_results'] = validation_results
            st.session_state[f'{session_key}_user_decisions'] = {
                result.row_index: result.suggested_action for result in validation_results
            }
            st.session_state[f'{session_key}_edited_data'] = {
                result.row_index: result.data.copy() for result in validation_results
            }
    
    return st.session_state[f'{session_key}_validation_results']


def validate_bulk_row(row_data: Dict, row_index: int, existing_data: pd.DataFrame, primary_field: str, config: TableConfig) -> ValidationResult:
    """Validate a single row in bulk upload"""
    issues = []
    status = 'valid'
    fuzzy_matches = []
    
    primary_value = row_data.get(primary_field)
    if not primary_value or str(primary_value).strip() == '':
        return ValidationResult(
            row_index=row_index,
            status='missing_required',
            issues=[f'Missing {primary_field}'],
            fuzzy_matches=[],
            suggested_action='skip',
            data=row_data
        )
    
    # Check for exact duplicate
    exact = check_exact_duplicate(str(primary_value), existing_data, primary_field)
    if exact:
        return ValidationResult(
            row_index=row_index,
            status='duplicate',
            issues=[f'Exact match: {exact}'],
            fuzzy_matches=[(exact, 1.0)],
            suggested_action='skip',
            data=row_data
        )
    
    # Check for fuzzy matches
    try:
        fuzzy_matches = check_fuzzy_matches(str(primary_value), existing_data, primary_field)
        if fuzzy_matches:
            status = 'fuzzy_match'
    except Exception as e:
        print(f"Fuzzy matching error for row {row_index}: {e}")
    
    suggested_action = 'skip' if status == 'duplicate' else 'insert'
    
    return ValidationResult(
        row_index=row_index,
        status=status,
        issues=issues,
        fuzzy_matches=fuzzy_matches,
        suggested_action=suggested_action,
        data=row_data
    )




def run_single_lookup(result: ValidationResult, table_name: str, session_key: str, existing_data: pd.DataFrame):
    """Run auto-lookup for a single entry"""
    if table_name != 'institution':
        st.info("Auto-lookup is only available for institution table")
        return
    
    primary_field = 'institution_cpi'  
    institution_name = result.data.get(primary_field)
    
    with st.spinner(f"Looking up {institution_name}..."):
        try:
            from services.institution_lookup_service import InstitutionLookupService
            
            # existing_data = get_table_data_cached('institution', limit=None)
            valid_countries = set()
            if not existing_data.empty:
                if 'country_sub' in existing_data.columns:
                    valid_countries.update(existing_data['country_sub'].dropna().unique())
                if 'country_parent' in existing_data.columns:
                    valid_countries.update(existing_data['country_parent'].dropna().unique())
            
            lookup_service = InstitutionLookupService(valid_countries=list(valid_countries))
            lookup_result = lookup_service.lookup_institution(institution_name)
            
            if f'{session_key}_lookup_results' not in st.session_state:
                st.session_state[f'{session_key}_lookup_results'] = {}
            st.session_state[f'{session_key}_lookup_results'][result.row_index] = lookup_result
            
            if lookup_result.confidence_score >= 0.75:
                if f'{session_key}_edited_data' not in st.session_state:
                    st.session_state[f'{session_key}_edited_data'] = {}
                
                edited_data = st.session_state[f'{session_key}_edited_data'].get(result.row_index, result.data.copy())
                
                if lookup_result.institution_type_layer1:
                    edited_data['institution_type_layer1'] = lookup_result.institution_type_layer1
                if lookup_result.institution_type_layer2:
                    edited_data['institution_type_layer2'] = lookup_result.institution_type_layer2
                if lookup_result.institution_type_layer3:
                    edited_data['institution_type_layer3'] = lookup_result.institution_type_layer3
                if lookup_result.subsidiary_country:
                    edited_data['country_sub'] = lookup_result.subsidiary_country
                if lookup_result.parent_country:
                    edited_data['country_parent'] = lookup_result.parent_country
                
                st.session_state[f'{session_key}_edited_data'][result.row_index] = edited_data
            
            st.success(f"Lookup complete (confidence: {lookup_result.confidence_score*100:.0f}%)")
            st.rerun()
            
        except Exception as e:
            st.error(f"Lookup failed: {str(e)}")


def run_batch_lookup(results: List[ValidationResult], table_name: str, session_key: str, existing_data: pd.DataFrame):
    """Run auto-lookup for multiple entries"""
    if table_name != 'institution':
        st.info("Auto-lookup is only available for institution table")
        return
    
    missing_data_results = [
        r for r in results 
        if (not st.session_state.get(f'{session_key}_edited_data', {}).get(r.row_index, {}).get('institution_type_layer1') or 
            not st.session_state.get(f'{session_key}_edited_data', {}).get(r.row_index, {}).get('country_sub'))
        and st.session_state[f'{session_key}_user_decisions'].get(r.row_index) == 'insert'
    ]
    
    if not missing_data_results:
        st.info("No incomplete records found.")
        return
    
    lookup_limit = min(len(missing_data_results), 20)
    
    try:
        from services.institution_lookup_service import InstitutionLookupService
        
        # existing_data = get_table_data_cached('institution', limit=None)
        valid_countries = set()
        if not existing_data.empty:
            if 'country_sub' in existing_data.columns:
                valid_countries.update(existing_data['country_sub'].dropna().unique())
            if 'country_parent' in existing_data.columns:
                valid_countries.update(existing_data['country_parent'].dropna().unique())
        
        lookup_service = InstitutionLookupService(valid_countries=list(valid_countries))
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        if f'{session_key}_lookup_results' not in st.session_state:
            st.session_state[f'{session_key}_lookup_results'] = {}
        if f'{session_key}_edited_data' not in st.session_state:
            st.session_state[f'{session_key}_edited_data'] = {}
        
        for idx, result in enumerate(missing_data_results[:lookup_limit]):
            institution_name = result.data.get('institution_cpi')
            status_text.text(f"Looking up {idx + 1}/{lookup_limit}: {institution_name}")
            
            try:
                lookup_result = lookup_service.lookup_institution(institution_name)
                st.session_state[f'{session_key}_lookup_results'][result.row_index] = lookup_result
                
                if lookup_result.confidence_score >= 0.75:
                    edited_data = st.session_state[f'{session_key}_edited_data'].get(result.row_index, result.data.copy())
                    
                    if lookup_result.institution_type_layer1:
                        edited_data['institution_type_layer1'] = lookup_result.institution_type_layer1
                    if lookup_result.institution_type_layer2:
                        edited_data['institution_type_layer2'] = lookup_result.institution_type_layer2
                    if lookup_result.institution_type_layer3:
                        edited_data['institution_type_layer3'] = lookup_result.institution_type_layer3
                    if lookup_result.subsidiary_country:
                        edited_data['country_sub'] = lookup_result.subsidiary_country
                    if lookup_result.parent_country:
                        edited_data['country_parent'] = lookup_result.parent_country
                    
                    st.session_state[f'{session_key}_edited_data'][result.row_index] = edited_data
                
            except Exception as e:
                print(f"Lookup failed for {institution_name}: {str(e)}")
            
            progress_bar.progress((idx + 1) / lookup_limit)
        
        progress_bar.empty()
        status_text.empty()
        st.success(f"Completed {lookup_limit} lookups")
        st.rerun()
        
    except Exception as e:
        st.error(f"Batch lookup failed: {str(e)}")


def execute_unified_bulk_insert(validation_results: List[ValidationResult], config: TableConfig, session_key: str, table_name: str):
    """Execute bulk insert with deferred standardization mappings"""
    records_to_insert = [
        result for result in validation_results
        if st.session_state[f'{session_key}_user_decisions'].get(result.row_index) == 'insert'
    ]
    
    pending_mappings = st.session_state.get(f'{session_key}_pending_mappings', {})
    
    if not records_to_insert and not pending_mappings:
        st.warning("No records selected for insertion and no mappings to create.")
        return
    
    with st.spinner(f"Processing {len(records_to_insert)} records and {len(pending_mappings)} standardization mappings..."):
        
        mapping_success_count = 0
        mapping_failed_count = 0
        
        if pending_mappings:
            standardization_service = StandardizationService()
            
            for row_index, mapping_info in pending_mappings.items():
                try:
                    user_input = mapping_info['user_input']
                    matched_name = mapping_info['matched_name']
                    table_type = mapping_info['table_type']
                    
                    if table_type == 'institution':
                        result = standardization_service.process_keep_institution(user_input, matched_name)
                    elif table_type == 'geography':
                        result = standardization_service.process_keep_geography(user_input, matched_name)
                    else:
                        result = {'success': False, 'message': 'Keep functionality not available for this table'}
                    
                    if result['success']:
                        mapping_success_count += 1
                    else:
                        mapping_failed_count += 1
                        st.error(f"Mapping failed for {user_input}: {result['message']}")
                        
                except Exception as e:
                    mapping_failed_count += 1
                    st.error(f"Error creating mapping for row {row_index}: {str(e)}")
        
        insert_success_count = 0
        insert_failed_count = 0
        
        if records_to_insert:
            bulk_data = []
            for result in records_to_insert:
                try:
                    data_to_insert = st.session_state.get(f'{session_key}_edited_data', {}).get(result.row_index, result.data)

                    data_to_insert['created_by'] = st.session_state.get('username', 'analyst')
                    data_to_insert['created_at'] = CURRENT_YEAR     
                    
                    from table_configs import get_table_config
                    config = get_table_config(table_name)
                    if config:
                        # year_fields = ['last_verified', 'year', 'year_added']
                        # for year_field in year_fields:
                        #     if any(field.name == year_field for field in config.fields):
                        #         data_to_insert[year_field] = CURRENT_YEAR
                        year_fields = ['last_verified', 'year', 'year_added', 'year_of_analysis'] 
                        for year_field in year_fields:
                            if any(field.name == year_field for field in config.fields):
                                if year_field not in data_to_insert or not data_to_insert[year_field]:
                                    data_to_insert[year_field] = CURRENT_YEAR

                    integer_fields = ['m49_code', 'iso_numeric_code']
                    for field in integer_fields:
                        if field in data_to_insert:
                            value = data_to_insert[field]
                            if value is None or str(value).strip() == '':
                                data_to_insert[field] = None  
                            else:
                                try:
                                    data_to_insert[field] = int(float(str(value).strip()))
                                except (ValueError, TypeError):
                                    data_to_insert[field] = None
                    
                    data_to_insert = {k: v for k, v in data_to_insert.items() if v is not None}
                    
                    bulk_data.append(data_to_insert)
                                        
                except Exception as e:
                    insert_failed_count += 1
            
            if bulk_data:
                try:
                    from database.connection import DatabaseConnection
                    success = DatabaseConnection.bulk_insert(table_name, bulk_data)
                    if success:
                        insert_success_count = len(bulk_data)
                    else:
                        insert_failed_count = len(bulk_data)
                except Exception as e:
                    st.error(f"Bulk insert failed: {str(e)}")
                    insert_failed_count = len(bulk_data)
        
        # Clear cache once at the end
        st.cache_data.clear()
        
        st.success(f"Upload complete!")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Records Uploaded", f"{insert_success_count}/{len(records_to_insert)}")
            if insert_failed_count > 0:
                st.error(f"{insert_failed_count} record uploads failed")
        
        with col2:
            st.metric("Mappings Created", f"{mapping_success_count}/{len(pending_mappings)}")
            if mapping_failed_count > 0:
                st.error(f"{mapping_failed_count} mappings failed")
        
        if mapping_success_count > 0:
            st.session_state[f'{session_key}_pending_mappings'] = {}
        
        if st.button("Start New Upload"):
            for key in ['df', 'validation_results', 'edited_data', 'user_decisions', 'lookup_results', 'upload_complete', 'upload_results', 'pending_mappings']:
                if key == 'pending_mappings':
                    st.session_state[f'{session_key}_{key}'] = {}
                elif key in ['edited_data', 'user_decisions', 'lookup_results']:
                    st.session_state[f'{session_key}_{key}'] = {}
                else:
                    st.session_state[f'{session_key}_{key}'] = None
            st.rerun()           







############################################## New, adding the hierarchy stuff ################################################
# def render_institution_submission_with_hierarchy(
#     form_data: Dict[str, Any], 
#     config: TableConfig, 
#     existing_data: pd.DataFrame,
#     standardization_data: pd.DataFrame,
#     dropdown_options: Dict[str, List[str]]
# ):
#     """Enhanced institution submission with hierarchy support"""
    
#     if st.button(f"Add {config.display_name}", type="primary", key="submit_institution"):
#         if not form_data.get('institution_cpi'):
#             st.error("Institution name is required")
#             return
        
#         username = st.session_state.get('username', 'analyst')
#         institution_name = form_data['institution_cpi']
        
#         # Check for duplicates and fuzzy matches
#         exact_duplicate = check_exact_duplicate(
#             institution_name, existing_data, 'institution_cpi', standardization_data
#         )
        
#         # Initialize hierarchy service
#         hierarchy_service = HierarchyService()
        
#         if exact_duplicate:
#             # Handle exact duplicate with hierarchy option
#             st.error(f"Institution '{institution_name}' already exists as '{exact_duplicate}'")
            
#             # Add hierarchy option for duplicates
#             hierarchy_data = render_hierarchy_options_for_duplicates(
#                 institution_name=institution_name,
#                 duplicate_name=exact_duplicate,
#                 existing_institutions=existing_data,
#                 form_key="duplicate_hierarchy"
#             )
            
#             if hierarchy_data:
#                 if hierarchy_data.get('cancel'):
#                     st.info("Hierarchy creation cancelled")
#                     return
                
#                 # Create hierarchy relationship
#                 result = hierarchy_service.create_hierarchy_entry(
#                     parent_institution=hierarchy_data['parent_institution'],
#                     child_institution=hierarchy_data['child_institution'],
#                     percent_ownership=hierarchy_data.get('percent_ownership'),
#                     relationship_type=hierarchy_data.get('relationship_type'),
#                     user=username,
#                     existing_institutions=existing_data,    
#                     existing_hierarchy=existing_hierarchy_data
#                 )
                
#                 if result['success']:
#                     st.success(f"Hierarchy relationship created successfully!")
#                     st.cache_data.clear()
                    
#                     # Clear hierarchy session state
#                     keys_to_clear = [k for k in st.session_state.keys() if 'hierarchy' in k.lower()]
#                     for key in keys_to_clear:
#                         if key in st.session_state:
#                             del st.session_state[key]
                    
#                     st.rerun()
#                 else:
#                     st.error(f"Failed to create hierarchy: {result['message']}")
            
#             return
        
#         # Check for fuzzy matches
#         fuzzy_matches = check_fuzzy_matches(institution_name, existing_data, 'institution_cpi')
        
#         if fuzzy_matches:
#             # Show fuzzy matches with Keep option and hierarchy option
#             st.warning(f"Found {len(fuzzy_matches)} similar institution(s):")
            
#             best_match = fuzzy_matches[0]
#             match_name, match_score = best_match
            
#             st.info(f"Best match: **{match_name}** ({match_score:.1f}% similarity)")
            
#             col1, col2 = st.columns(2)
            
#             standardization_service = StandardizationService()
            
#             with col1:
#                 if st.button("Keep (Add to Standardization)", key="keep_fuzzy"):
#                     result = standardization_service.process_keep_institution(institution_name, match_name)
#                     if result['success']:
#                         st.success("Added to standardization table!")
#                         st.cache_data.clear()
#                         st.rerun()
#                     else:
#                         st.error(f"Failed: {result['message']}")
            
#             with col2:
#                 if st.button("Create New Institution", key="create_new_fuzzy"):
#                     # Process as new institution (will handle below)
#                     fuzzy_matches = []  # Clear to proceed to new institution logic
            
#             # Add hierarchy option for fuzzy matches 
#             if fuzzy_matches:  # Only show if user hasn't chosen to create new
#                 hierarchy_data = render_hierarchy_options_for_fuzzy_matches(
#                     institution_name=institution_name,
#                     matched_name=match_name,
#                     existing_institutions=existing_data,
#                     form_key="fuzzy_hierarchy"
#                 )
                
#                 if hierarchy_data:
#                     if hierarchy_data.get('cancel'):
#                         st.info("Hierarchy creation cancelled")
#                         return
                    
#                     # Create hierarchy relationship
#                     result = hierarchy_service.create_hierarchy_entry(
#                         parent_institution=hierarchy_data['parent_institution'],
#                         child_institution=hierarchy_data['child_institution'],
#                         percent_ownership=hierarchy_data.get('percent_ownership'),
#                         relationship_type=hierarchy_data.get('relationship_type'),
#                         user=username,
#                         existing_institutions=existing_data,    
#                         existing_hierarchy=existing_hierarchy_data
#                     )
                    
#                     if result['success']:
#                         st.success("Hierarchy relationship created successfully!")
#                         st.cache_data.clear()
                        
#                         # Clear hierarchy session state
#                         keys_to_clear = [k for k in st.session_state.keys() if 'hierarchy' in k.lower()]
#                         for key in keys_to_clear:
#                             if key in st.session_state:
#                                 del st.session_state[key]
                        
#                         st.rerun()
#                     else:
#                         st.error(f"Failed to create hierarchy: {result['message']}")
                
#                 return
        
#         # No duplicates or fuzzy matches - create new institution
#         if not exact_duplicate and not fuzzy_matches:
#             # Auto-populate data
#             enhanced_data = auto_populate_data(form_data, username)
            
#             # Create the institution
#             institution_service = InstitutionService()
#             result = institution_service.create_institution(
#                 institution_name=enhanced_data['institution_cpi'],
#                 institution_type_layer1=enhanced_data.get('institution_type_layer1'),
#                 institution_type_layer2=enhanced_data.get('institution_type_layer2'),
#                 institution_type_layer3=enhanced_data.get('institution_type_layer3'),
#                 country_sub=enhanced_data.get('country_sub'),
#                 country_parent=enhanced_data.get('country_parent'),
#                 double_counting_risk=enhanced_data.get('double_counting_risk'),
#                 institution_cpi_short=enhanced_data.get('institution_cpi_short'),
#                 contact_info=enhanced_data.get('contact_info'),
#                 comments=enhanced_data.get('comments'),
#                 user=username
#             )
            
#             if result['success']:
#                 st.success(f"Institution '{result['institution_name']}' created successfully!")
                
#                 # Optional hierarchy creation for new institutions
#                 hierarchy_data = render_new_institution_hierarchy_option(
#                     institution_name=result['institution_name'],
#                     existing_institutions=existing_data,
#                     form_key="new_institution_hierarchy"
#                 )
                
#                 if hierarchy_data:
#                     if hierarchy_data['mode'] == 'new_as_parent':
#                         # New institution is parent
#                         st.info("Creating hierarchy relationship...")
#                         time.sleep(1)  # Brief delay to ensure institution is created
                        
#                         # Refresh data to get new institution ID
#                         st.cache_data.clear()
                        
#                         hierarchy_result = hierarchy_service.create_hierarchy_entry(
#                             parent_institution=hierarchy_data['parent_institution'],
#                             child_institution=hierarchy_data['child_institution'],
#                             percent_ownership=hierarchy_data.get('percent_ownership'),
#                             relationship_type=hierarchy_data.get('relationship_type'),
#                             user=username,
#                             existing_institutions=existing_data,    
#                             existing_hierarchy=existing_hierarchy_data
#                         )
                    
#                     elif hierarchy_data['mode'] == 'new_as_child':
#                         # New institution is child
#                         hierarchy_result = hierarchy_service.create_hierarchy_entry(
#                             parent_institution=hierarchy_data['parent_institution'],
#                             child_institution=hierarchy_data['child_institution'],
#                             percent_ownership=hierarchy_data.get('percent_ownership'),
#                             relationship_type=hierarchy_data.get('relationship_type'),
#                             user=username,
#                             existing_institutions=existing_data,    
#                             existing_hierarchy=existing_hierarchy_data
#                         )
                    
#                     if 'hierarchy_result' in locals() and hierarchy_result['success']:
#                         st.success("Hierarchy relationship created successfully!")
#                     elif 'hierarchy_result' in locals():
#                         st.error(f"Hierarchy creation failed: {hierarchy_result['message']}")
                
#                 # Clear caches and reset form
#                 st.cache_data.clear()
#                 session_key = f'{config.table_name}_reference_data'
#                 if session_key in st.session_state:
#                     del st.session_state[session_key]
                
#                 st.rerun()
#             else:
#                 st.error(f"Failed to create institution: {result['message']}")


# def render_standard_submission(
#     form_data: Dict[str, Any], 
#     config: TableConfig, 
#     table_name: str, 
#     existing_data: pd.DataFrame
# ):
#     """Standard submission for non-institution tables"""
    
#     if st.button(f"Add {config.display_name}", type="primary", key=f"submit_{table_name}"):
#         missing_fields = []
#         for field_name in config.required_fields:
#             if not form_data.get(field_name):
#                 missing_fields.append(field_name)
        
#         if missing_fields:
#             st.error(f"Required fields missing: {', '.join(missing_fields)}")
#             return
        
#         username = st.session_state.get('username', 'analyst')
        
#         if config.duplicate_check_fields:
#             if len(config.duplicate_check_fields) > 1:
#                 # Use compound duplicate checking for multiple fields
#                 compound_duplicate = check_compound_duplicate(form_data, existing_data, config.duplicate_check_fields)
                
#                 if compound_duplicate:
#                     st.error(f"Entry already exists with these values: {compound_duplicate}")
#                     return
                
#                 # Also check fuzzy matches on the primary field for suggestions
#                 primary_field = config.duplicate_check_fields[0]
#                 input_value = form_data.get(primary_field)
#                 if input_value:
#                     fuzzy_matches = check_fuzzy_matches(input_value, existing_data, primary_field)
#                     if fuzzy_matches:
#                         st.warning(f"Found {len(fuzzy_matches)} similar entries for '{primary_field}':")
#                         for name, score in fuzzy_matches[:3]:
#                             st.write(f"- {name} ({score:.1f}% match)")
                        
#                         if not st.button("Continue Anyway", key="continue_despite_fuzzy"):
#                             return
#             else:
#                 # Use single field duplicate checking (existing logic)
#                 primary_field = config.duplicate_check_fields[0]
#                 input_value = form_data.get(primary_field)
                
#                 if input_value:
#                     exact_duplicate = check_exact_duplicate(input_value, existing_data, primary_field)
                    
#                     if exact_duplicate:
#                         st.error(f"Entry already exists: {exact_duplicate}")
#                         return
                    
#                     fuzzy_matches = check_fuzzy_matches(input_value, existing_data, primary_field)
#                     if fuzzy_matches:
#                         st.warning(f"Found {len(fuzzy_matches)} similar entries:")
#                         for name, score in fuzzy_matches[:3]:
#                             st.write(f"- {name} ({score:.1f}% match)")
                        
#                         # if not st.button("Continue Anyway", key="continue_despite_fuzzy"):
#                         #     return
                            
#             # primary_field = config.duplicate_check_fields[0]
#             # input_value = form_data.get(primary_field)
            
#             # if input_value:
#             #     exact_duplicate = check_exact_duplicate(input_value, existing_data, primary_field)
                
#             #     if exact_duplicate:
#             #         st.error(f"Entry already exists: {exact_duplicate}")
#             #         return
                
#             #     fuzzy_matches = check_fuzzy_matches(input_value, existing_data, primary_field)
#             #     if fuzzy_matches:
#             #         st.warning(f"Found {len(fuzzy_matches)} similar entries:")
#             #         for name, score in fuzzy_matches[:3]:
#             #             st.write(f"- {name} ({score:.1f}% match)")
                    
#             #         if not st.button("Continue Anyway", key="continue_despite_fuzzy"):
#             #             return
        
#         enhanced_data = auto_populate_data(form_data, username)
        
#         query_service = QueryService()
#         success = query_service.execute_insert(table_name, enhanced_data)
        
#         if success:
#             st.success(f"{config.display_name} created successfully!")
#             st.cache_data.clear()
#             session_key = f'{table_name}_reference_data'
#             if session_key in st.session_state:
#                 del st.session_state[session_key]
#             st.rerun()
#         else:
#             st.error(f"Failed to create {config.display_name}")