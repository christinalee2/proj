"""
Updated unified form system with "Keep" functionality for standardization
Integrates the new StandardizationService for both single and bulk entry
"""
import streamlit as st
import pandas as pd
import io
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass

from table_configs import get_table_config, TableConfig
from services.institution_service import InstitutionService
from database.cached_queries import get_table_data_cached
from database.queries import QueryService
from utils.text_processing import TextProcessor
from utils.fuzzy_matching import get_fitted_matcher
from services.standardization_service import StandardizationService


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


def check_exact_duplicate(input_value: str, existing_df: pd.DataFrame, primary_field: str) -> Optional[str]:
    """Check for exact duplicate in any table's primary field"""
    if existing_df.empty or primary_field not in existing_df.columns:
        return None
    
    normalized_input = normalize_name(input_value)
    
    for _, row in existing_df.iterrows():
        existing_value = str(row.get(primary_field, '')).strip()
        if normalize_name(existing_value) == normalized_input:
            return existing_value
    return None


def check_fuzzy_matches(input_value: str, existing_df: pd.DataFrame, primary_field: str) -> List[Tuple[str, float]]:
    """Find fuzzy matches in any table's primary field"""
    if existing_df.empty or primary_field not in existing_df.columns:
        return []
    
    try:
        # Create a temporary DataFrame with the structure fuzzy matcher expects
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


def get_table_dropdown_options(table_name: str, config: TableConfig) -> Dict[str, List[str]]:
    """Get dropdown options for all select fields by reading from the table itself"""
    options = {}
    
    try:
        # Get existing data from the current table only
        existing_data = get_table_data_cached(table_name, limit=None)
        
        if existing_data.empty:
            # If no data, return empty options for all select fields
            for field_config in config.fields:
                if field_config.field_type == 'select':
                    options[field_config.name] = ['']
            return options
        
        # For each select field, get unique values from the table
        for field_config in config.fields:
            if field_config.field_type == 'select':
                field_name = field_config.name
                
                # Special handling for country fields - only try geography if needed and available
                if field_name in ['country_sub', 'country_parent', 'country_cpi']:
                    # First try to get unique values from the current table itself
                    if field_name in existing_data.columns:
                        unique_values = existing_data[field_name].dropna().unique()
                        unique_strings = sorted([str(v) for v in unique_values if str(v).strip()])
                        if unique_strings:  # If we found countries in the current table, use those
                            options[field_name] = [''] + unique_strings
                            continue
                    
                    # Only try geography table if current table has no country data
                    # and we specifically need geography data
                    if table_name != 'geography':  # Don't try to load geography when we're already in geography
                        try:
                            geo_data = get_table_data_cached('geography', limit=None)
                            if not geo_data.empty and 'country_cpi' in geo_data.columns:
                                countries = sorted(geo_data['country_cpi'].dropna().unique())
                                options[field_name] = [''] + [str(c) for c in countries]
                            else:
                                options[field_name] = ['']
                        except Exception as e:
                            # If geography table fails to load, just provide empty options
                            print(f"Could not load geography data for {field_name}: {e}")
                            options[field_name] = ['']
                    else:
                        options[field_name] = ['']
                
                # For all other select fields, get unique values from the current table
                elif field_name in existing_data.columns:
                    unique_values = existing_data[field_name].dropna().unique()
                    # Convert to strings and sort
                    unique_strings = sorted([str(v) for v in unique_values if str(v).strip()])
                    options[field_name] = [''] + unique_strings
                else:
                    options[field_name] = ['']
        
        return options
        
    except Exception as e:
        print(f"Error getting dropdown options for {table_name}: {e}")
        # Return empty options for all select fields
        for field_config in config.fields:
            if field_config.field_type == 'select':
                options[field_config.name] = ['']
        return options


def create_table_entry(table_name: str, data: Dict[str, Any], user: str = "system") -> Dict[str, Any]:
    """Create entry in any table using appropriate service"""
    
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
            double_counting_risk=data.get('double_counting_risk'),
            contact_info=data.get('contact_info'),
            comments=data.get('comments'),
            user=user
        )
    else:
        # For other tables, use direct database insertion
        try:
            # Clean data - remove empty values
            clean_data = {k: v for k, v in data.items() if v is not None and str(v).strip() != ''}
            
            # Use QueryService to insert
            query_service = QueryService()
            success = query_service.execute_insert(table_name, clean_data)
            
            if success:
                return {
                    'success': True,
                    'entry_id': clean_data.get(list(clean_data.keys())[0]),  # Use first field as ID
                    'message': 'Entry created successfully'
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


def render_form_field(field_config, dropdown_options: Dict[str, List[str]], key_suffix: str) -> Any:
    """Render a single form field"""
    field_key = f"{field_config.name}_{key_suffix}"
    
    # Handle prefill for institution fields
    default_value = ''
    if field_config.name == 'institution_type_layer1' and st.session_state.get('prefill_type1'):
        default_value = st.session_state['prefill_type1']
    elif field_config.name == 'institution_type_layer2' and st.session_state.get('prefill_type2'):
        default_value = st.session_state['prefill_type2']
    elif field_config.name == 'institution_type_layer3' and st.session_state.get('prefill_type3'):
        default_value = st.session_state['prefill_type3']
    elif field_config.name == 'country_parent' and st.session_state.get('prefill_parent'):
        default_value = st.session_state['prefill_parent']
    elif field_config.name == 'country_sub' and st.session_state.get('prefill_sub'):
        default_value = st.session_state['prefill_sub']
    
    if field_config.field_type == 'text':
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
        if 'year' in field_config.name.lower():
            return st.number_input(
                field_config.display_name,
                help=field_config.help_text,
                key=field_key,
                step=1,
                value=None
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
        if default_value and default_value in options:
            index = options.index(default_value)
        
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
        # Fallback to text input
        return st.text_input(
            field_config.display_name,
            value=default_value,
            help=field_config.help_text,
            key=field_key
        )


def render_unified_single_entry_form(table_name: str):
    """
    Unified single entry form with duplicate checking and Keep functionality for any table
    """
    config = get_table_config(table_name)
    if not config:
        st.error(f"No configuration found for table: {table_name}")
        return
        
    # Clear cache if needed from previous operation
    if st.session_state.get('_cache_needs_clear', False):
        st.cache_data.clear()
        st.session_state['_cache_needs_clear'] = False
    
    st.subheader(f"Add New {config.display_name}")
    st.markdown(config.description)
    st.markdown("---")
    
    # Load existing data and dropdown options
    existing_data = get_table_data_cached(table_name, limit=None)
    dropdown_options = get_table_dropdown_options(table_name, config)
    
    # Get the primary field (first required field)
    primary_field = config.required_fields[0] if config.required_fields else config.fields[0].name
    primary_field_config = next((f for f in config.fields if f.name == primary_field), None)
    
    # Initialize standardization service
    standardization_service = StandardizationService()
    
    # Primary field input with real-time checking
    if primary_field_config:
        primary_value = st.text_input(
            primary_field_config.display_name,
            placeholder=primary_field_config.placeholder or f"Enter {primary_field_config.display_name.lower()}...",
            help=primary_field_config.help_text,
            key=f"{table_name}_primary"
        )
        
        # Show duplicate checking results if value entered
        if primary_value and len(str(primary_value).strip()) >= 3:
            
            # Exact duplicate warning
            exact = check_exact_duplicate(primary_value, existing_data, primary_field)
            if exact:
                st.warning(f"'{exact}' already exists in the {config.display_name.lower()} table.")
            
            # Fuzzy matches with Keep functionality
            try:
                fuzzy = check_fuzzy_matches(primary_value, existing_data, primary_field)
                if fuzzy:
                    st.info(f"Found {len(fuzzy)} similar {config.display_name.lower()}(s)")
                    st.caption("Similar entries found. Click 'Keep' to use your entry and create a standardization mapping.")
                    
                    # Show fuzzy matches with inline Keep buttons
                    for i, (name, score) in enumerate(fuzzy):
                        col1, col2 = st.columns([4, 1])
                        
                        with col1:
                            # Show match details including country if available
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
                            if st.button("Keep", key=f"keep_{table_name}_{i}", help=f"Use '{primary_value}' and map to '{name}'"):
                                # Process keep action
                                if table_name == 'institution':
                                    result = standardization_service.process_keep_institution(primary_value, name)
                                elif table_name == 'geography':
                                    result = standardization_service.process_keep_geography(primary_value, name)
                                else:
                                    result = {'success': False, 'message': 'Keep functionality not available for this table'}
                                
                                if result['success']:
                                    st.success(result['message'])
                                    st.info(f"Added to standardization table.")
                                else:
                                    st.error(result['message'])
                                
                                st.rerun()
                            
            except Exception as e:
                st.error(f"Error checking for similar entries: {str(e)}")
    
    # Auto-lookup button (only for institution table)
    if table_name == 'institution' and primary_value and len(str(primary_value).strip()) >= 3:
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("Auto-Lookup", key="lookup_btn", help="Automatically find institution details from trusted sources"):
                with st.spinner("Searching trusted sources and extracting data..."):
                    try:
                        from services.institution_lookup_service import InstitutionLookupService
                        
                        # Get valid countries
                        valid_countries = set()
                        if not existing_data.empty:
                            if 'country_sub' in existing_data.columns:
                                valid_countries.update(existing_data['country_sub'].dropna().unique())
                            if 'country_parent' in existing_data.columns:
                                valid_countries.update(existing_data['country_parent'].dropna().unique())
                        
                        lookup_service = InstitutionLookupService(valid_countries=list(valid_countries))
                        result = lookup_service.lookup_institution(primary_value)
                        
                        # Store lookup result in session state for use in form
                        st.session_state['lookup_result'] = result
                        st.session_state['lookup_used'] = False
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Lookup failed: {str(e)}")
    
    # Display lookup results if available (institution only)
    if table_name == 'institution' and st.session_state.get('lookup_result') and not st.session_state.get('lookup_used', False):
        lookup_result = st.session_state['lookup_result']
        confidence = lookup_result.confidence_score
        
        # Color code by confidence
        if confidence >= 0.9:
            st.success(f"High confidence data found ({confidence * 100:.0f}%)")
        elif confidence >= 0.7:
            st.info(f"Moderate confidence data found ({confidence * 100:.0f}%)")
        else:
            st.warning(f"Low confidence data found ({confidence * 100:.0f}%) - Please verify")
        
        # Show found data
        col1, col2 = st.columns(2)
        
        with col1:
            if lookup_result.institution_type_layer1:
                st.write(f"**Type Layer 1:** {lookup_result.institution_type_layer1}")
            if lookup_result.institution_type_layer2:
                st.write(f"**Type Layer 2:** {lookup_result.institution_type_layer2}")
            if lookup_result.institution_type_layer3:
                st.write(f"**Type Layer 3:** {lookup_result.institution_type_layer3}")
        
        with col2:
            if lookup_result.parent_country:
                st.write(f"**Parent Country:** {lookup_result.parent_country}")
            if lookup_result.subsidiary_country:
                st.write(f"**Subsidiary Country:** {lookup_result.subsidiary_country}")
        
        # Show reasoning
        if lookup_result.reasoning:
            with st.expander("Why these values?"):
                st.write(lookup_result.reasoning)
        
        # Show sources
        if lookup_result.sources:
            with st.expander(f"Sources ({len(lookup_result.sources)} sources used)"):
                for source in lookup_result.sources:
                    st.markdown(f"• [{source['title']}]({source['url']})")
        
        # Button to use these values
        col1, col2, col3 = st.columns([2, 1, 2])
        with col2:
            if st.button("Use These Values", key="use_lookup", type="primary"):
                # Set prefill values
                st.session_state['prefill_type1'] = lookup_result.institution_type_layer1
                st.session_state['prefill_type2'] = lookup_result.institution_type_layer2
                st.session_state['prefill_type3'] = lookup_result.institution_type_layer3
                st.session_state['prefill_parent'] = lookup_result.parent_country
                st.session_state['prefill_sub'] = lookup_result.subsidiary_country
                st.session_state['lookup_used'] = True
                st.rerun()
    
    # Form always shows below
    st.markdown("---")
    st.subheader(f"{config.display_name} Details")
    
    # Collect form data
    form_data = {}
    if primary_field_config:
        form_data[primary_field] = primary_value
    
    # Organize fields into categories
    remaining_fields = [f for f in config.fields if f.name != primary_field]
    required_fields = [f for f in remaining_fields if f.required and getattr(f, 'category', 'main') == 'main']
    optional_main_fields = [f for f in remaining_fields if not f.required and getattr(f, 'category', 'main') == 'main']
    advanced_fields = [f for f in remaining_fields if getattr(f, 'category', 'main') == 'advanced']
    
    # Required fields
    if required_fields:
        st.subheader("Required Fields")
        cols = st.columns(2)
        for i, field_config in enumerate(required_fields):
            with cols[i % 2]:
                form_data[field_config.name] = render_form_field(field_config, dropdown_options, f"{table_name}_req_{i}")
    
    # Optional main fields
    if optional_main_fields:
        st.subheader("Optional Fields")
        cols = st.columns(2)
        for i, field_config in enumerate(optional_main_fields):
            with cols[i % 2]:
                form_data[field_config.name] = render_form_field(field_config, dropdown_options, f"{table_name}_opt_{i}")
    
    # Advanced fields in expandable section
    if advanced_fields:
        with st.expander("Additional Information"):
            cols = st.columns(2)
            for i, field_config in enumerate(advanced_fields):
                with cols[i % 2]:
                    form_data[field_config.name] = render_form_field(field_config, dropdown_options, f"{table_name}_adv_{i}")
    
    st.markdown("---")
    
    # Submit buttons
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.button(f"Add {config.display_name}", type="primary", use_container_width=True):
            # Validate required fields
            missing_required = []
            for field_name in config.required_fields:
                if not form_data.get(field_name) or str(form_data[field_name]).strip() == '':
                    field_display = next((f.display_name for f in config.fields if f.name == field_name), field_name)
                    missing_required.append(field_display)
            
            if missing_required:
                st.error(f"Please fill in required fields: {', '.join(missing_required)}")
            else:
                with st.spinner(f"Creating {config.display_name.lower()}..."):
                    result = create_table_entry(
                        table_name,
                        form_data,
                        user=st.session_state.get('username', 'analyst')
                    )
                    
                    if result['success']:
                        st.success(f"{config.display_name} created successfully!")
                        # Defer cache clear to next interaction
                        st.session_state[f'_cache_needs_clear'] = True
                        
                        # Clear lookup data if institution
                        if table_name == 'institution':
                            st.session_state.pop('lookup_result', None)
                            st.session_state.pop('lookup_used', None)
                            for key in ['prefill_type1', 'prefill_type2', 'prefill_type3', 'prefill_parent', 'prefill_sub']:
                                st.session_state.pop(key, None)
                        
                        if st.button("Add Another", key="add_another"):
                            st.rerun()
                    else:
                        st.error(result['message'])
    
    with col2:
        if st.button("Reset Form", use_container_width=True):
            # Clear lookup data if institution
            if table_name == 'institution':
                for key in ['lookup_result', 'lookup_used', 'prefill_type1', 'prefill_type2', 'prefill_type3', 'prefill_parent', 'prefill_sub']:
                    st.session_state.pop(key, None)
            st.rerun()



def render_unified_bulk_upload(table_name: str):
    """
    Enhanced unified bulk upload interface with inline fuzzy matching
    """
    config = get_table_config(table_name)
    if not config:
        st.error(f"No configuration found for table: {table_name}")
        return
    
    st.subheader(f"Bulk Upload to {config.display_name} Table")
    st.markdown(config.description)
    
    # Initialize session state
    session_key = f'bulk_upload_{table_name}'
    init_bulk_upload_session_state(session_key)
    
    # Template download
    render_template_download(table_name, config)
    
    st.markdown("---")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Choose CSV or Excel file",
        type=['csv', 'xlsx', 'xls'],
        key=f"upload_{table_name}"
    )
    
    if uploaded_file is not None:
        # Parse and validate file
        df = process_uploaded_file(uploaded_file, config, session_key)
        
        if df is not None:
            # Run validation with duplicate checking
            validation_results = run_bulk_validation(df, table_name, config, session_key)
            
            if validation_results:
                # Render enhanced bulk upload interface
                render_enhanced_bulk_upload_grid(validation_results, config, session_key, table_name)


def render_enhanced_bulk_upload_grid(validation_results: List[ValidationResult], config: TableConfig, session_key: str, table_name: str):
    """Enhanced bulk upload grid with inline fuzzy matching"""
    # Separate results by status
    valid_results = [r for r in validation_results if r.status == 'valid']
    fuzzy_results = [r for r in validation_results if r.status == 'fuzzy_match']
    duplicate_results = [r for r in validation_results if r.status == 'duplicate']
    error_results = [r for r in validation_results if r.status == 'missing_required']
    
    visible_count = len([r for r in (valid_results + fuzzy_results) 
                        if st.session_state[f'{session_key}_user_decisions'].get(r.row_index) != 'skip'])
    
    # Summary metrics
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
    
    # Collapsed sections for duplicates and errors
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
    
    # Main data grid with fuzzy matches at the top
    st.subheader(f"Review & Edit Records ({visible_count} rows)")
    
    # Batch actions
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Auto-Lookup All Missing Data", use_container_width=True):
            run_batch_lookup(valid_results + fuzzy_results, table_name, session_key)
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
    
    # Enhanced grid header with Match column
    render_enhanced_grid_header()
    
    # Sort results: fuzzy matches first, then valid ones
    rows_to_show_fuzzy = [r for r in fuzzy_results if st.session_state[f'{session_key}_user_decisions'].get(r.row_index) != 'skip']
    rows_to_show_valid = [r for r in valid_results if st.session_state[f'{session_key}_user_decisions'].get(r.row_index) != 'skip']
    
    # Combine: fuzzy matches at top, then valid
    all_to_display = rows_to_show_fuzzy + rows_to_show_valid
    
    # Pagination
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
    
    # Render data rows with enhanced fuzzy match handling
    for result in paginated_rows:
        render_enhanced_grid_row(result, config, session_key, table_name)
    
    # Upload button
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Upload to Database", type="primary", use_container_width=True):
            execute_unified_bulk_insert(validation_results, config, session_key, table_name)


def render_enhanced_grid_header():
    """Enhanced grid header - same as original without Match column"""
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
    
    # Back to original column layout
    cols = st.columns([2, 1.5, 1.5, 1.5, 1.5, 1.5, 0.5, 0.3])
    
    with cols[0]:
        st.markdown("**Institution Name**")
    with cols[1]:
        st.markdown("**Type Layer 1**")
    with cols[2]:
        st.markdown("**Type Layer 2**")
    with cols[3]:
        st.markdown("**Type Layer 3**")
    with cols[4]:
        st.markdown("**Country (Sub)**")
    with cols[5]:
        st.markdown("**Country (Parent)**")
    with cols[6]:
        st.markdown("**Lookup**")
    with cols[7]:
        st.markdown("")


def render_enhanced_grid_row(result: ValidationResult, config: TableConfig, session_key: str, table_name: str):
    """Enhanced grid row with slim blue info box for fuzzy matches"""
    
    # Get edited data for this row
    if f'{session_key}_edited_data' not in st.session_state:
        st.session_state[f'{session_key}_edited_data'] = {}
    
    if result.row_index not in st.session_state[f'{session_key}_edited_data']:
        st.session_state[f'{session_key}_edited_data'][result.row_index] = result.data.copy()
    
    row_data = st.session_state[f'{session_key}_edited_data'][result.row_index]
    
    # Check if this is a fuzzy match row
    is_fuzzy_match = result.status == 'fuzzy_match' and result.fuzzy_matches
    
    container = st.container()
    
    with container:
        # Show fuzzy match warning above the row (like original)
        if is_fuzzy_match:
            col1, col2 = st.columns([4, 1])
            with col1:
                # Create comma-separated match list with percentages
                matches_text = ', '.join([f'{name} ({score*100:.0f}%)' for name, score in result.fuzzy_matches[:3]])
                st.info(f"**Similar institutions found:** {matches_text}")
            with col2:
                if st.button("Match", key=f"match_btn_{result.row_index}", help="Select which institution to map to"):
                    # Show dropdown when Match button is clicked
                    st.session_state[f'show_match_dropdown_{result.row_index}'] = True
                    st.rerun()
        
        # Show dropdown if Match button was clicked
        if st.session_state.get(f'show_match_dropdown_{result.row_index}', False):
            # Create dropdown options from fuzzy matches
            match_options = [f"{name} ({score*100:.0f}%)" for name, score in result.fuzzy_matches]
            
            selected_match = st.selectbox(
                "Select institution to map to:",
                match_options,
                key=f"match_select_{result.row_index}"
            )
            
            col_confirm, col_cancel = st.columns(2)
            
            with col_confirm:
                if st.button("Confirm", key=f"confirm_match_{result.row_index}", type="primary"):
                    # Extract the institution name from the selected option
                    selected_match_name = selected_match.split(' (')[0]  # Remove the percentage part
                    user_input = row_data.get(config.required_fields[0] if config.required_fields else config.fields[0].name, '')
                    
                    # Store mapping for deferred processing instead of processing immediately
                    st.session_state[f'{session_key}_pending_mappings'][result.row_index] = {
                        'user_input': user_input,
                        'matched_name': selected_match_name,
                        'table_type': 'institution' if table_name == 'institution' else 'geography'
                    }
                    
                    # Remove this row from upload (mark as skip)
                    st.session_state[f'{session_key}_user_decisions'][result.row_index] = 'skip'
                    
                    # Clean up dropdown state
                    st.session_state[f'show_match_dropdown_{result.row_index}'] = False
                    
                    st.success(f"Mapping queued: {user_input} → {selected_match_name}")
                    st.rerun()
            
            with col_cancel:
                if st.button("Cancel", key=f"cancel_match_{result.row_index}"):
                    st.session_state[f'show_match_dropdown_{result.row_index}'] = False
                    st.rerun()
        
        # Main row with original column layout
        cols = st.columns([2, 1.5, 1.5, 1.5, 1.5, 1.5, 0.5, 0.3])
        
        # Get dropdown options for this table
        dropdown_options = get_table_dropdown_options(table_name, config)
        
        # Check for lookup data
        lookup_result = st.session_state.get(f'{session_key}_lookup_results', {}).get(result.row_index)
        
        with cols[0]:
            # Institution name (with visual indicator for fuzzy matches)
            name_display = row_data.get(config.required_fields[0] if config.required_fields else config.fields[0].name, '')
            if is_fuzzy_match:
                name_display = f"🔍 {name_display}"
            st.markdown(f"<div class='institution-name'>{name_display}</div>", unsafe_allow_html=True)
        
        # Rest of the columns (Type layers, Countries, etc.) - for institution table
        if table_name == 'institution':
            with cols[1]:
                # Type Layer 1
                default_val = row_data.get('institution_type_layer1', '')
                if not default_val and lookup_result and hasattr(lookup_result, 'institution_type_layer1'):
                    default_val = lookup_result.institution_type_layer1 or ''
                
                options = dropdown_options.get('institution_type_layer1', [''])
                idx = options.index(default_val) if default_val in options else 0
                new_val = st.selectbox(
                    "type1",
                    options,
                    index=idx,
                    key=f"type1_{result.row_index}_{session_key}",
                    label_visibility="collapsed"
                )
                if new_val != row_data.get('institution_type_layer1'):
                    st.session_state[f'{session_key}_edited_data'][result.row_index]['institution_type_layer1'] = new_val
            
            with cols[2]:
                # Type Layer 2
                default_val = row_data.get('institution_type_layer2', '')
                if not default_val and lookup_result and hasattr(lookup_result, 'institution_type_layer2'):
                    default_val = lookup_result.institution_type_layer2 or ''
                
                options = dropdown_options.get('institution_type_layer2', [''])
                idx = options.index(default_val) if default_val in options else 0
                new_val = st.selectbox(
                    "type2",
                    options,
                    index=idx,
                    key=f"type2_{result.row_index}_{session_key}",
                    label_visibility="collapsed"
                )
                if new_val != row_data.get('institution_type_layer2'):
                    st.session_state[f'{session_key}_edited_data'][result.row_index]['institution_type_layer2'] = new_val
            
            with cols[3]:
                # Type Layer 3
                default_val = row_data.get('institution_type_layer3', '')
                if not default_val and lookup_result and hasattr(lookup_result, 'institution_type_layer3'):
                    default_val = lookup_result.institution_type_layer3 or ''
                
                options = dropdown_options.get('institution_type_layer3', [''])
                idx = options.index(default_val) if default_val in options else 0
                new_val = st.selectbox(
                    "type3",
                    options,
                    index=idx,
                    key=f"type3_{result.row_index}_{session_key}",
                    label_visibility="collapsed"
                )
                if new_val != row_data.get('institution_type_layer3'):
                    st.session_state[f'{session_key}_edited_data'][result.row_index]['institution_type_layer3'] = new_val
            
            with cols[4]:
                # Country Sub
                default_val = row_data.get('country_sub', '')
                if not default_val and lookup_result and hasattr(lookup_result, 'subsidiary_country'):
                    default_val = lookup_result.subsidiary_country or ''
                
                options = dropdown_options.get('country_sub', [''])
                idx = options.index(default_val) if default_val in options else 0
                new_val = st.selectbox(
                    "csub",
                    options,
                    index=idx,
                    key=f"csub_{result.row_index}_{session_key}",
                    label_visibility="collapsed"
                )
                if new_val != row_data.get('country_sub'):
                    st.session_state[f'{session_key}_edited_data'][result.row_index]['country_sub'] = new_val
            
            with cols[5]:
                # Country Parent
                default_val = row_data.get('country_parent', '')
                if not default_val and lookup_result and hasattr(lookup_result, 'parent_country'):
                    default_val = lookup_result.parent_country or ''
                
                options = dropdown_options.get('country_parent', [''])
                idx = options.index(default_val) if default_val in options else 0
                new_val = st.selectbox(
                    "cpar",
                    options,
                    index=idx,
                    key=f"cpar_{result.row_index}_{session_key}",
                    label_visibility="collapsed"
                )
                if new_val != row_data.get('country_parent'):
                    st.session_state[f'{session_key}_edited_data'][result.row_index]['country_parent'] = new_val

        
        else:
            # For other tables, show the first few select fields dynamically
            select_fields = [f for f in config.fields if f.field_type == 'select' and f.name != (config.required_fields[0] if config.required_fields else config.fields[0].name)][:5]
            
            for i, field in enumerate(select_fields):
                if i + 1 < len(cols) - 2:  # Make sure we don't exceed column count
                    with cols[i + 1]:
                        current_value = row_data.get(field.name, '')
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
        
        with cols[6]:
            # Lookup button (only for institution table)
            if table_name == 'institution':
                if st.button("🔍", key=f"lookup_btn_{result.row_index}_{session_key}", help="Auto-lookup"):
                    run_single_lookup(result, table_name, session_key)
            else:
                st.markdown("")
        with cols[7]:
            # Discard button (X) - also removes pending mappings
            if st.button("✕", key=f"discard_row_{result.row_index}_{session_key}", help="Remove this row"):
                st.session_state[f'{session_key}_user_decisions'][result.row_index] = 'skip'
                
                # Remove any pending mapping for this row
                if result.row_index in st.session_state.get(f'{session_key}_pending_mappings', {}):
                    del st.session_state[f'{session_key}_pending_mappings'][result.row_index]
                
                # Clean up any dropdown state
                if f'show_match_dropdown_{result.row_index}' in st.session_state:
                    del st.session_state[f'show_match_dropdown_{result.row_index}']
                st.rerun()
        
        st.markdown("---")



        
def init_bulk_upload_session_state(session_key: str):
    """Initialize session state for bulk upload"""
    for key in ['df', 'validation_results', 'edited_data', 'user_decisions', 'upload_complete', 'upload_results', 'pending_mappings']:
        if f'{session_key}_{key}' not in st.session_state:
            if key == 'pending_mappings':
                st.session_state[f'{session_key}_{key}'] = {}
            elif key != 'edited_data' and key != 'user_decisions':
                st.session_state[f'{session_key}_{key}'] = None
            else:
                st.session_state[f'{session_key}_{key}'] = {}


def render_template_download(table_name: str, config: TableConfig):
    """Render template download section"""
    with st.expander("Download Template"):
        # Create sample data
        template_data = {}
        for field_config in config.fields:
            if field_config.field_type == 'number':
                example_value = 2024 if 'year' in field_config.name.lower() else 1.0
            elif field_config.field_type == 'boolean':
                example_value = 'True'
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
                
                # Validate required columns
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


def run_bulk_validation(df: pd.DataFrame, table_name: str, config: TableConfig, session_key: str) -> Optional[List[ValidationResult]]:
    """Run validation with duplicate checking on bulk upload"""
    if st.session_state[f'{session_key}_validation_results'] is None:
        with st.spinner("Validating entries and checking for duplicates..."):
            existing_data = get_table_data_cached(table_name, limit=None)
            primary_field = config.required_fields[0] if config.required_fields else config.fields[0].name
            
            validation_results = []
            
            for idx, row in df.iterrows():
                row_data = row.to_dict()
                
                # Validate this row
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


# def render_excel_grid_header(config: TableConfig):
#     """Render Excel-style grid header"""
#     st.markdown("""
#     <style>
#     .institution-name {
#         font-size: 15px;
#         color: #000000;
#         font-weight: 500;
#         padding: 8px 0;
#     }
#     </style>
#     """, unsafe_allow_html=True)
    
#     # Create columns based on table fields - show first 6 fields + lookup + actions
#     primary_field = config.required_fields[0] if config.required_fields else config.fields[0].name
#     display_fields = [f for f in config.fields if f.name == primary_field][:1]  # Primary field
#     other_fields = [f for f in config.fields if f.field_type == 'select' and f.name != primary_field][:5]  # Up to 5 select fields
#     display_fields.extend(other_fields)
    
#     col_widths = [2] + [1.5] * min(len(other_fields), 5) + [0.5, 0.3]  # Primary + others + lookup + discard
#     cols = st.columns(col_widths)
    
#     with cols[0]:
#         st.markdown(f"**{display_fields[0].display_name}**")
    
#     for i, field in enumerate(other_fields[:5], 1):
#         if i < len(cols) - 2:  # Leave space for lookup and discard
#             with cols[i]:
#                 st.markdown(f"**{field.display_name}**")
    
#     with cols[-2]:
#         st.markdown("**Lookup**")
#     with cols[-1]:
#         st.markdown("")


# def render_excel_grid_row(result: ValidationResult, config: TableConfig, session_key: str, table_name: str):
#     """Render a single row in Excel-style grid with inline editing"""
    
#     # Get edited data for this row
#     if f'{session_key}_edited_data' not in st.session_state:
#         st.session_state[f'{session_key}_edited_data'] = {}
    
#     if result.row_index not in st.session_state[f'{session_key}_edited_data']:
#         st.session_state[f'{session_key}_edited_data'][result.row_index] = result.data.copy()
    
#     row_data = st.session_state[f'{session_key}_edited_data'][result.row_index]
    
#     container = st.container()
#     with container:
#         # Main editable row
#         primary_field = config.required_fields[0] if config.required_fields else config.fields[0].name
#         display_fields = [f for f in config.fields if f.name == primary_field][:1]  # Primary field
#         other_fields = [f for f in config.fields if f.field_type == 'select' and f.name != primary_field][:5]  # Select fields
#         display_fields.extend(other_fields)
        
#         col_widths = [2] + [1.5] * min(len(other_fields), 5) + [0.5, 0.3]
#         cols = st.columns(col_widths)
        
#         # Get dropdown options for this table
#         dropdown_options = get_table_dropdown_options(table_name, config)
        
#         # Check for lookup data
#         lookup_result = st.session_state.get(f'{session_key}_lookup_results', {}).get(result.row_index)
        
#         with cols[0]:
#             # Primary field (non-editable, just display)
#             st.markdown(f"<div class='institution-name'>{row_data.get(primary_field, '')}</div>", unsafe_allow_html=True)
        
#         # Render editable select fields
#         for i, field in enumerate(other_fields[:5], 1):
#             if i < len(cols) - 2:
#                 with cols[i]:
#                     # Get current value
#                     current_value = row_data.get(field.name, '')
                    
#                     # Use lookup result if available and current value is empty
#                     if not current_value and lookup_result:
#                         if field.name == 'institution_type_layer1' and hasattr(lookup_result, 'institution_type_layer1'):
#                             current_value = lookup_result.institution_type_layer1 or ''
#                         elif field.name == 'institution_type_layer2' and hasattr(lookup_result, 'institution_type_layer2'):
#                             current_value = lookup_result.institution_type_layer2 or ''
#                         elif field.name == 'institution_type_layer3' and hasattr(lookup_result, 'institution_type_layer3'):
#                             current_value = lookup_result.institution_type_layer3 or ''
#                         elif field.name == 'country_sub' and hasattr(lookup_result, 'subsidiary_country'):
#                             current_value = lookup_result.subsidiary_country or ''
#                         elif field.name == 'country_parent' and hasattr(lookup_result, 'parent_country'):
#                             current_value = lookup_result.parent_country or ''
                    
#                     # Get options for this field
#                     options = dropdown_options.get(field.name, [''])
#                     idx = options.index(current_value) if current_value in options else 0
                    
#                     # Render selectbox
#                     new_value = st.selectbox(
#                         field.name,
#                         options,
#                         index=idx,
#                         key=f"{field.name}_{result.row_index}_{session_key}",
#                         label_visibility="collapsed"
#                     )
                    
#                     # Update edited data if changed
#                     if new_value != row_data.get(field.name):
#                         st.session_state[f'{session_key}_edited_data'][result.row_index][field.name] = new_value
        
#         with cols[-2]:
#             if st.button("🔍", key=f"lookup_btn_{result.row_index}_{session_key}", help="Auto-lookup"):
#                 run_single_lookup(result, table_name, session_key)
        
#         with cols[-1]:
#             if st.button("✕", key=f"discard_row_{result.row_index}_{session_key}", help="Remove this row"):
#                 st.session_state[f'{session_key}_user_decisions'][result.row_index] = 'skip'
#                 st.rerun()
        
#         st.markdown("---")


def run_single_lookup(result: ValidationResult, table_name: str, session_key: str):
    """Run auto-lookup for a single entry"""
    if table_name != 'institution':
        st.info("Auto-lookup is only available for institution table")
        return
    
    primary_field = 'institution_cpi'  
    institution_name = result.data.get(primary_field)
    
    with st.spinner(f"Looking up {institution_name}..."):
        try:
            from services.institution_lookup_service import InstitutionLookupService
            
            # Get valid countries
            existing_data = get_table_data_cached('institution', limit=None)
            valid_countries = set()
            if not existing_data.empty:
                if 'country_sub' in existing_data.columns:
                    valid_countries.update(existing_data['country_sub'].dropna().unique())
                if 'country_parent' in existing_data.columns:
                    valid_countries.update(existing_data['country_parent'].dropna().unique())
            
            lookup_service = InstitutionLookupService(valid_countries=list(valid_countries))
            lookup_result = lookup_service.lookup_institution(institution_name)
            
            # Store lookup result
            if f'{session_key}_lookup_results' not in st.session_state:
                st.session_state[f'{session_key}_lookup_results'] = {}
            st.session_state[f'{session_key}_lookup_results'][result.row_index] = lookup_result
            
            # Auto-apply if high confidence
            if lookup_result.confidence_score >= 0.75:
                if f'{session_key}_edited_data' not in st.session_state:
                    st.session_state[f'{session_key}_edited_data'] = {}
                
                edited_data = st.session_state[f'{session_key}_edited_data'].get(result.row_index, result.data.copy())
                
                # Update with lookup results
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


def run_batch_lookup(results: List[ValidationResult], table_name: str, session_key: str):
    """Run auto-lookup for multiple entries"""
    if table_name != 'institution':
        st.info("Auto-lookup is only available for institution table")
        return
    
    # Filter for records that need lookup and are set to insert
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
        
        # Initialize lookup service
        existing_data = get_table_data_cached('institution', limit=None)
        valid_countries = set()
        if not existing_data.empty:
            if 'country_sub' in existing_data.columns:
                valid_countries.update(existing_data['country_sub'].dropna().unique())
            if 'country_parent' in existing_data.columns:
                valid_countries.update(existing_data['country_parent'].dropna().unique())
        
        lookup_service = InstitutionLookupService(valid_countries=list(valid_countries))
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Initialize session state
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
                    
                    # Update with lookup results
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
        
        # Step 1: Process standardization mappings first
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
        
        # Step 2: Process regular record insertions IN BULK
        insert_success_count = 0
        insert_failed_count = 0
        
        if records_to_insert:
            # Prepare all data for bulk insert
            bulk_data = []
            for result in records_to_insert:
                try:
                    data_to_insert = st.session_state.get(f'{session_key}_edited_data', {}).get(result.row_index, result.data)
                    bulk_data.append(data_to_insert)
                except Exception as e:
                    insert_failed_count += 1
            
            # Single bulk insert call
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
        
        # Clear cache ONCE at the end
        st.cache_data.clear()
        
        # Show results
        st.success(f"✅ Upload complete!")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Records Uploaded", f"{insert_success_count}/{len(records_to_insert)}")
            if insert_failed_count > 0:
                st.error(f"{insert_failed_count} record uploads failed")
        
        with col2:
            st.metric("Mappings Created", f"{mapping_success_count}/{len(pending_mappings)}")
            if mapping_failed_count > 0:
                st.error(f"{mapping_failed_count} mappings failed")
        
        # Clear pending mappings after successful processing
        if mapping_success_count > 0:
            st.session_state[f'{session_key}_pending_mappings'] = {}
        
        if st.button("Start New Upload"):
            # Reset session state
            for key in ['df', 'validation_results', 'edited_data', 'user_decisions', 'lookup_results', 'upload_complete', 'upload_results', 'pending_mappings']:
                if key == 'pending_mappings':
                    st.session_state[f'{session_key}_{key}'] = {}
                elif key in ['edited_data', 'user_decisions', 'lookup_results']:
                    st.session_state[f'{session_key}_{key}'] = {}
                else:
                    st.session_state[f'{session_key}_{key}'] = None
            st.rerun()