import pandas as pd
import streamlit as st
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from services.cached_services import get_institution_service, get_lookup_service
from database.cached_queries import (
    get_all_institutions_cached, 
    get_dropdown_options,
    get_fitted_matcher_cached
)
from utils.text_processing import TextProcessor
import io


@dataclass
class ValidationResult:
    """Result of validating one row"""
    row_index: int
    status: str
    issues: List[str]
    fuzzy_matches: List[Tuple[str, float]]
    suggested_action: str
    data: Dict


class BulkUploadHandler:
    """Handles bulk uploads with validation and duplicate detection"""
    
    REQUIRED_FIELDS = ['institution_cpi']
    
    OPTIONAL_FIELDS = [
        'institution_type_layer1',
        'institution_type_layer2', 
        'institution_type_layer3',
        'country_sub',
        'country_parent',
        'double_counting_risk',
        'contact_info',
        'comments'
    ]
    
    def __init__(self):
        # Use cached services instead of creating new ones
        self.service = get_institution_service()
        self.lookup_service = None  # Lazy load when needed
        
    def parse_uploaded_file(self, uploaded_file) -> Tuple[pd.DataFrame, List[str]]:
        """Parse CSV or Excel file - OPTIMIZED"""
        errors = []
        
        try:
            # Speed optimization: Use faster engines
            if uploaded_file.name.endswith('.csv'):
                # Use faster C engine for CSV
                df = pd.read_csv(uploaded_file, engine='c', low_memory=False)
            elif uploaded_file.name.endswith(('.xlsx', '.xls')):
                # Use faster openpyxl engine, only read data
                df = pd.read_excel(uploaded_file, engine='openpyxl')
            else:
                errors.append(f"Unsupported file type: {uploaded_file.name}")
                return pd.DataFrame(), errors
            
            # Clean column names (strip whitespace)
            df.columns = df.columns.str.strip()
            
            # Only convert to None where needed (faster than full replacement)
            df = df.where(pd.notna(df), None)
            
            return df, errors
            
        except Exception as e:
            errors.append(f"Error parsing file: {str(e)}")
            return pd.DataFrame(), errors
    
    def validate_columns(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate that required columns exist"""
        missing = []
        for field in self.REQUIRED_FIELDS:
            if field not in df.columns:
                missing.append(field)
        return len(missing) == 0, missing
    
    def validate_row(
        self, 
        row_data: Dict, 
        row_index: int,
        existing_institutions: pd.DataFrame
    ) -> ValidationResult:
        """Validate a single row - OPTIMIZED with cached matcher"""
        issues = []
        status = 'valid'
        fuzzy_matches = []
        
        institution_name = row_data.get('institution_cpi')
        if not institution_name or str(institution_name).strip() == '':
            return ValidationResult(
                row_index=row_index,
                status='missing_required',
                issues=['Missing institution_cpi'],
                fuzzy_matches=[],
                suggested_action='skip',
                data=row_data
            )
        
        normalized_name = TextProcessor.normalize_institution_name(institution_name)
        
        # Check for exact duplicate
        for _, existing_row in existing_institutions.iterrows():
            existing_name = existing_row.get('institution_cpi', '')
            if TextProcessor.normalize_institution_name(existing_name).lower() == normalized_name.lower():
                issues.append(f"Exact match: {existing_name}")
                status = 'duplicate'
                return ValidationResult(
                    row_index=row_index,
                    status=status,
                    issues=issues,
                    fuzzy_matches=[(existing_name, 1.0)],
                    suggested_action='skip',
                    data=row_data
                )
        
        # Check for fuzzy matches using CACHED matcher
        try:
            # Use cached matcher for speed
            institutions_hash = str(len(existing_institutions))
            matcher = get_fitted_matcher_cached(institutions_hash)
            
            fuzzy_matches = matcher.find_similar_institutions(
                query=institution_name,
                institution_df=existing_institutions,
                limit=5,
                tfidf_top_k=50
            )
            
            fuzzy_matches = [(name, score) for name, score in fuzzy_matches if score >= 0.85]
            
            if fuzzy_matches:
                status = 'fuzzy_match'
                    
        except Exception as e:
            print(f"Fuzzy matching error for row {row_index}: {e}")
        
        if status == 'duplicate':
            suggested_action = 'skip'
        elif status == 'fuzzy_match':
            suggested_action = 'insert'
        elif status == 'valid':
            suggested_action = 'insert'
        else:
            suggested_action = 'skip'
        
        return ValidationResult(
            row_index=row_index,
            status=status,
            issues=issues,
            fuzzy_matches=fuzzy_matches,
            suggested_action=suggested_action,
            data=row_data
        )
    
    def validate_bulk_upload(
        self,
        df: pd.DataFrame,
        existing_institutions: pd.DataFrame
    ) -> List[ValidationResult]:
        """Validate entire upload - OPTIMIZED"""
        results = []
        for idx, row in df.iterrows():
            row_data = row.to_dict()
            result = self.validate_row(row_data, idx, existing_institutions)
            results.append(result)
        return results
    
    def get_next_institution_id(self) -> int:
        """Get next available institution_id"""
        try:
            existing = get_all_institutions_cached()
            if existing.empty or 'institution_id' not in existing.columns:
                return 1
            max_id = existing['institution_id'].max()
            return int(max_id) + 1 if pd.notna(max_id) else 1
        except:
            return 1


def render_bulk_upload_interface_enhanced(table: str):
    """Excel-style bulk upload interface with improved fuzzy match handling"""
    
    if table != 'institution':
        st.info(f"Bulk upload for '{table}' table coming soon.")
        return
    
    st.subheader("3. Bulk Upload to Institution Table")
    
    handler = BulkUploadHandler()
    
    # Initialize session state
    if 'bulk_upload_df' not in st.session_state:
        st.session_state['bulk_upload_df'] = None
    if 'validation_results' not in st.session_state:
        st.session_state['validation_results'] = None
    if 'edited_data' not in st.session_state:
        st.session_state['edited_data'] = {}
    if 'user_decisions' not in st.session_state:
        st.session_state['user_decisions'] = {}
    if 'lookup_results' not in st.session_state:
        st.session_state['lookup_results'] = {}
    if 'upload_complete' not in st.session_state:
        st.session_state['upload_complete'] = False
    if 'upload_results' not in st.session_state:
        st.session_state['upload_results'] = None
    if 'rows_per_page' not in st.session_state:
        st.session_state['rows_per_page'] = 50
    if 'current_page' not in st.session_state:
        st.session_state['current_page'] = 1
    if 'selected_matches' not in st.session_state:  # NEW: Track selected fuzzy matches
        st.session_state['selected_matches'] = {}
    
    # Template download section (same as before)
    with st.expander("📄 Download Template"):
        template_data = {
            'institution_cpi': ['Example Institution 1', 'Example Institution 2'],
            'institution_type_layer1': ['Private', 'Public'],
            'institution_type_layer2': ['Funds', 'Corporation'],
            'institution_type_layer3': ['Venture Capital Fund', 'Corporate'],
            'country_sub': ['United States', 'Chile'],
            'country_parent': ['United States', 'Chile'],
        }
        template_df = pd.DataFrame(template_data)
        
        col1, col2 = st.columns(2)
        with col1:
            csv_buffer = io.StringIO()
            template_df.to_csv(csv_buffer, index=False)
            st.download_button(
                "CSV Template",
                csv_buffer.getvalue(),
                "institution_template.csv",
                "text/csv",
                use_container_width=True
            )
        with col2:
            excel_buffer = io.BytesIO()
            template_df.to_excel(excel_buffer, index=False, engine='openpyxl')
            st.download_button(
                "Excel Template",
                excel_buffer.getvalue(),
                "institution_template.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
    st.markdown("---")
    
    # File Upload
    uploaded_file = st.file_uploader(
        "Choose CSV or Excel file",
        type=['csv', 'xlsx', 'xls']
    )
    
    if uploaded_file is not None:
        # Parse file (same logic as before)
        if st.session_state['bulk_upload_df'] is None or uploaded_file.name != st.session_state.get('last_uploaded_file'):
            with st.spinner("Loading file..."):
                df, parse_errors = handler.parse_uploaded_file(uploaded_file)
                
                if parse_errors:
                    for error in parse_errors:
                        st.error(error)
                    return
                
                valid_columns, missing_columns = handler.validate_columns(df)
                
                if not valid_columns:
                    st.error(f"Missing required columns: {', '.join(missing_columns)}")
                    return
                
                st.session_state['bulk_upload_df'] = df
                st.session_state['last_uploaded_file'] = uploaded_file.name
                st.session_state['validation_results'] = None
                st.session_state['current_page'] = 1
                st.session_state['selected_matches'] = {}  # Reset match selections
        
        df = st.session_state['bulk_upload_df']
        
        # Auto-validate using CACHED institutions
        if st.session_state['validation_results'] is None:
            with st.spinner("Validating against existing institutions..."):
                existing_institutions = get_all_institutions_cached()
                validation_results = handler.validate_bulk_upload(df, existing_institutions)
                st.session_state['validation_results'] = validation_results
                st.session_state['user_decisions'] = {
                    result.row_index: result.suggested_action 
                    for result in validation_results
                }
                st.session_state['edited_data'] = {
                    result.row_index: result.data.copy()
                    for result in validation_results
                }
        
        validation_results = st.session_state['validation_results']
        
        # Separate by status
        valid_results = [r for r in validation_results if r.status == 'valid']
        fuzzy_results = [r for r in validation_results if r.status == 'fuzzy_match']
        duplicate_results = [r for r in validation_results if r.status == 'duplicate']
        error_results = [r for r in validation_results if r.status == 'missing_required']
        
        # Count currently visible (not skipped) rows
        visible_new_count = len([r for r in (valid_results + fuzzy_results) if st.session_state['user_decisions'].get(r.row_index) != 'skip'])
        
        # Summary metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total Rows", len(validation_results))
        with col2:
            st.metric("Reviewing", visible_new_count)
        with col3:
            st.metric("Similar Matches", len([r for r in fuzzy_results if st.session_state['user_decisions'].get(r.row_index) != 'skip']))
        with col4:
            st.metric("Duplicates", len(duplicate_results))
        with col5:
            insert_count = sum(1 for d in st.session_state['user_decisions'].values() if d == 'insert')
            st.metric("Will Insert", insert_count)
        
        st.markdown("---")
        
        # Collapsed sections for duplicates and errors
        if duplicate_results:
            with st.expander(f"Duplicates ({len(duplicate_results)}) - Click to review"):
                st.caption("These institutions already exist in the database")
                for result in duplicate_results:
                    render_duplicate_row(result, handler)
        
        if error_results:
            with st.expander(f"Errors ({len(error_results)}) - Missing required data"):
                st.caption("These rows are missing the institution_cpi field")
                for result in error_results:
                    st.error(f"Row {result.row_index + 1}: {result.issues[0]}")
        
        # Main data grid with fuzzy matches at the top
        st.subheader(f"Review & Edit Records ({visible_new_count} rows)")
        
        # Batch actions
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔍 Auto-Lookup All Missing Data", use_container_width=True):
                run_batch_lookup(handler, valid_results + fuzzy_results)
        with col2:
            if st.button("Skip All with Similar Matches", use_container_width=True):
                for result in fuzzy_results:
                    st.session_state['user_decisions'][result.row_index] = 'skip'
                st.rerun()
        with col3:
            if st.button("Reset All Decisions", use_container_width=True):
                st.session_state['user_decisions'] = {
                    r.row_index: r.suggested_action for r in validation_results
                }
                st.session_state['selected_matches'] = {}
                st.rerun()
        
        st.markdown("---")
        
        # Enhanced grid header with Match column
        render_enhanced_grid_header()
        
        # Sort results: fuzzy matches first, then valid ones
        all_to_display = []
        rows_to_show_fuzzy = [r for r in fuzzy_results if st.session_state['user_decisions'].get(r.row_index) != 'skip']
        rows_to_show_valid = [r for r in valid_results if st.session_state['user_decisions'].get(r.row_index) != 'skip']
        
        # Combine: fuzzy matches at top, then valid
        all_to_display = rows_to_show_fuzzy + rows_to_show_valid
        
        # Pagination
        total_rows = len(all_to_display)
        rows_per_page = st.session_state['rows_per_page']
        total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)
        
        if total_rows > rows_per_page:
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                current_page = st.number_input(
                    f"Page (showing {rows_per_page} rows per page)",
                    min_value=1,
                    max_value=max(1, total_pages),
                    value=st.session_state['current_page'],
                    key="page_selector"
                )
                st.session_state['current_page'] = current_page
            
            start_idx = (current_page - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            paginated_rows = all_to_display[start_idx:end_idx]
            
            st.info(f"Showing rows {start_idx + 1}-{min(end_idx, total_rows)} of {total_rows}")
        else:
            paginated_rows = all_to_display
        
        # Render data rows with enhanced fuzzy match handling
        for result in paginated_rows:
            render_enhanced_grid_row(result, handler)
        
        # Upload button
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.button("Upload to Database", type="primary", use_container_width=True, key="final_upload"):
                st.session_state['show_confirm'] = True
                st.rerun()
        
        # Show confirmation dialog if flag is set
        if st.session_state.get('show_confirm', False):
            execute_bulk_insert(handler, validation_results)
        
        # Show results if upload completed
        if st.session_state.get('upload_complete', False):
            show_upload_results()

def render_enhanced_grid_header():
    """Enhanced grid header with Match column for fuzzy matches"""
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
    
    # Added Match column between Institution Name and Type Layer 1
    cols = st.columns([2, 1.2, 1.3, 1.3, 1.3, 1.3, 1.3, 0.5, 0.3])
    
    with cols[0]:
        st.markdown("**Institution Name**")
    with cols[1]:
        st.markdown("**Match**")
    with cols[2]:
        st.markdown("**Type Layer 1**")
    with cols[3]:
        st.markdown("**Type Layer 2**")
    with cols[4]:
        st.markdown("**Type Layer 3**")
    with cols[5]:
        st.markdown("**Country (Sub)**")
    with cols[6]:
        st.markdown("**Country (Parent)**")
    with cols[7]:
        st.markdown("**Lookup**")
    with cols[8]:
        st.markdown("")


def render_enhanced_grid_row(result: ValidationResult, handler: BulkUploadHandler):
    """Enhanced grid row with inline fuzzy match handling"""
    
    row_data = st.session_state['edited_data'].get(result.row_index, result.data)
    
    # Check if this is a fuzzy match row
    is_fuzzy_match = result.status == 'fuzzy_match' and result.fuzzy_matches
    
    container = st.container()
    
    with container:
        # Apply fuzzy match styling if applicable
        if is_fuzzy_match:
            st.markdown('<div class="fuzzy-row">', unsafe_allow_html=True)
        
        # Main row with enhanced column layout
        cols = st.columns([2, 1.2, 1.3, 1.3, 1.3, 1.3, 1.3, 0.5, 0.3])
        
        # Load cached dropdown options
        options = get_dropdown_options()
        
        # Check for lookup data
        lookup_result = st.session_state['lookup_results'].get(result.row_index)
        
        with cols[0]:
            # Institution name (with visual indicator for fuzzy matches)
            name_display = row_data.get('institution_cpi', '')
            if is_fuzzy_match:
                name_display = f"🔍 {name_display}"
            st.markdown(f"<div class='institution-name'>{name_display}</div>", unsafe_allow_html=True)
        
        with cols[1]:
            # Match column - only show for fuzzy matches
            if is_fuzzy_match:
                # Show all fuzzy matches as text
                matches_text = []
                for i, (name, score) in enumerate(result.fuzzy_matches):
                    matches_text.append(f"• {name} ({score*100:.0f}%)")
                
                # Display matches
                for match_text in matches_text:
                    st.caption(match_text)
                
                # Match button with dropdown selection
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
                            user_input = row_data.get('institution_cpi', '')
                            
                            # Process the standardization mapping
                            from services.standardization_service import StandardizationService
                            standardization_service = StandardizationService()
                            
                            with st.spinner(f"Creating mapping for {user_input}..."):
                                mapping_result = standardization_service.process_keep_institution(user_input, selected_match_name)
                                
                                if mapping_result['success']:
                                    st.success(f"Created mapping: {user_input} -> {selected_match_name}")
                                    # Remove this row from upload (mark as skip)
                                    st.session_state['user_decisions'][result.row_index] = 'skip'
                                    # Clean up dropdown state
                                    st.session_state[f'show_match_dropdown_{result.row_index}'] = False
                                    st.rerun()
                                else:
                                    st.error(f"Mapping failed: {mapping_result['message']}")
                    
                    with col_cancel:
                        if st.button("Cancel", key=f"cancel_match_{result.row_index}"):
                            st.session_state[f'show_match_dropdown_{result.row_index}'] = False
                            st.rerun()
            else:
                st.markdown("") # Empty space for non-fuzzy matches
        
        # Rest of the columns (Type layers, Countries, etc.) - same as before
        with cols[2]:
            # Type Layer 1
            default_val = row_data.get('institution_type_layer1', '')
            if not default_val and lookup_result and lookup_result.institution_type_layer1:
                default_val = lookup_result.institution_type_layer1
            
            idx = options['type1'].index(default_val) if default_val in options['type1'] else 0
            new_val = st.selectbox(
                "type1",
                options['type1'],
                index=idx,
                key=f"type1_{result.row_index}",
                label_visibility="collapsed"
            )
            if new_val != row_data.get('institution_type_layer1'):
                st.session_state['edited_data'][result.row_index]['institution_type_layer1'] = new_val
        
        with cols[3]:
            # Type Layer 2
            default_val = row_data.get('institution_type_layer2', '')
            if not default_val and lookup_result and lookup_result.institution_type_layer2:
                default_val = lookup_result.institution_type_layer2
            
            idx = options['type2'].index(default_val) if default_val in options['type2'] else 0
            new_val = st.selectbox(
                "type2",
                options['type2'],
                index=idx,
                key=f"type2_{result.row_index}",
                label_visibility="collapsed"
            )
            if new_val != row_data.get('institution_type_layer2'):
                st.session_state['edited_data'][result.row_index]['institution_type_layer2'] = new_val
        
        with cols[4]:
            # Type Layer 3
            default_val = row_data.get('institution_type_layer3', '')
            if not default_val and lookup_result and lookup_result.institution_type_layer3:
                default_val = lookup_result.institution_type_layer3
            
            idx = options['type3'].index(default_val) if default_val in options['type3'] else 0
            new_val = st.selectbox(
                "type3",
                options['type3'],
                index=idx,
                key=f"type3_{result.row_index}",
                label_visibility="collapsed"
            )
            if new_val != row_data.get('institution_type_layer3'):
                st.session_state['edited_data'][result.row_index]['institution_type_layer3'] = new_val
        
        with cols[5]:
            # Country Sub
            default_val = row_data.get('country_sub', '')
            if not default_val and lookup_result and lookup_result.subsidiary_country:
                default_val = lookup_result.subsidiary_country
            
            idx = options['countries'].index(default_val) if default_val in options['countries'] else 0
            new_val = st.selectbox(
                "csub",
                options['countries'],
                index=idx,
                key=f"csub_{result.row_index}",
                label_visibility="collapsed"
            )
            if new_val != row_data.get('country_sub'):
                st.session_state['edited_data'][result.row_index]['country_sub'] = new_val
        
        with cols[6]:
            # Country Parent
            default_val = row_data.get('country_parent', '')
            if not default_val and lookup_result and lookup_result.parent_country:
                default_val = lookup_result.parent_country
            
            idx = options['countries'].index(default_val) if default_val in options['countries'] else 0
            new_val = st.selectbox(
                "cpar",
                options['countries'],
                index=idx,
                key=f"cpar_{result.row_index}",
                label_visibility="collapsed"
            )
            if new_val != row_data.get('country_parent'):
                st.session_state['edited_data'][result.row_index]['country_parent'] = new_val
        
        with cols[7]:
            # Lookup button
            if st.button("🔍", key=f"lookup_btn_{result.row_index}", help="Auto-lookup"):
                run_single_lookup(handler, result)
        
        with cols[8]:
            # Discard button (X)
            if st.button("✕", key=f"discard_row_{result.row_index}", help="Remove this row"):
                st.session_state['user_decisions'][result.row_index] = 'skip'
                # Clean up selection if it exists
                if result.row_index in st.session_state['selected_matches']:
                    del st.session_state['selected_matches'][result.row_index]
                st.rerun()
        
        if is_fuzzy_match:
            st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown("---")
# def render_grid_header():
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
    
#     cols = st.columns([2, 1.5, 1.5, 1.5, 1.5, 1.5, 0.5, 0.3])
    
#     with cols[0]:
#         st.markdown("**Institution Name**")
#     with cols[1]:
#         st.markdown("**Type Layer 1**")
#     with cols[2]:
#         st.markdown("**Type Layer 2**")
#     with cols[3]:
#         st.markdown("**Type Layer 3**")
#     with cols[4]:
#         st.markdown("**Country (Sub)**")
#     with cols[5]:
#         st.markdown("**Country (Parent)**")
#     with cols[6]:
#         st.markdown("**Lookup**")
#     with cols[7]:
#         st.markdown("")


# def render_grid_row(result: ValidationResult, handler: BulkUploadHandler):
#     """Render a single row in Excel-style grid with inline editing - OPTIMIZED"""
    
#     row_data = st.session_state['edited_data'].get(result.row_index, result.data)
    
#     container = st.container()
    
#     with container:
#         # Show fuzzy match warning if applicable
#         if result.status == 'fuzzy_match' and result.fuzzy_matches:
#             col1, col2 = st.columns([4, 1])
#             with col1:
#                 st.info(f"**Similar institutions found:** {', '.join([f'{name} ({score*100:.0f}%)' for name, score in result.fuzzy_matches[:3]])}")
#             with col2:
#                 if st.button("Discard", key=f"discard_{result.row_index}", help="Too similar - skip this row"):
#                     st.session_state['user_decisions'][result.row_index] = 'skip'
#                     st.rerun()
        
#         # Main row
#         cols = st.columns([2, 1.5, 1.5, 1.5, 1.5, 1.5, 0.5, 0.3])
        
#         # Load cached dropdown options - FAST
#         options = get_dropdown_options()
        
#         # Check for lookup data
#         lookup_result = st.session_state['lookup_results'].get(result.row_index)
        
#         with cols[0]:
#             # Institution name (plain black text)
#             st.markdown(f"<div class='institution-name'>{row_data.get('institution_cpi', '')}</div>", unsafe_allow_html=True)
        
#         with cols[1]:
#             # Type Layer 1
#             default_val = row_data.get('institution_type_layer1', '')
#             if not default_val and lookup_result and lookup_result.institution_type_layer1:
#                 default_val = lookup_result.institution_type_layer1
            
#             idx = options['type1'].index(default_val) if default_val in options['type1'] else 0
#             new_val = st.selectbox(
#                 "type1",
#                 options['type1'],
#                 index=idx,
#                 key=f"type1_{result.row_index}",
#                 label_visibility="collapsed"
#             )
#             if new_val != row_data.get('institution_type_layer1'):
#                 st.session_state['edited_data'][result.row_index]['institution_type_layer1'] = new_val
        
#         with cols[2]:
#             # Type Layer 2
#             default_val = row_data.get('institution_type_layer2', '')
#             if not default_val and lookup_result and lookup_result.institution_type_layer2:
#                 default_val = lookup_result.institution_type_layer2
            
#             idx = options['type2'].index(default_val) if default_val in options['type2'] else 0
#             new_val = st.selectbox(
#                 "type2",
#                 options['type2'],
#                 index=idx,
#                 key=f"type2_{result.row_index}",
#                 label_visibility="collapsed"
#             )
#             if new_val != row_data.get('institution_type_layer2'):
#                 st.session_state['edited_data'][result.row_index]['institution_type_layer2'] = new_val
        
#         with cols[3]:
#             # Type Layer 3
#             default_val = row_data.get('institution_type_layer3', '')
#             if not default_val and lookup_result and lookup_result.institution_type_layer3:
#                 default_val = lookup_result.institution_type_layer3
            
#             idx = options['type3'].index(default_val) if default_val in options['type3'] else 0
#             new_val = st.selectbox(
#                 "type3",
#                 options['type3'],
#                 index=idx,
#                 key=f"type3_{result.row_index}",
#                 label_visibility="collapsed"
#             )
#             if new_val != row_data.get('institution_type_layer3'):
#                 st.session_state['edited_data'][result.row_index]['institution_type_layer3'] = new_val
        
#         with cols[4]:
#             # Country Sub
#             default_val = row_data.get('country_sub', '')
#             if not default_val and lookup_result and lookup_result.subsidiary_country:
#                 default_val = lookup_result.subsidiary_country
            
#             idx = options['countries'].index(default_val) if default_val in options['countries'] else 0
#             new_val = st.selectbox(
#                 "csub",
#                 options['countries'],
#                 index=idx,
#                 key=f"csub_{result.row_index}",
#                 label_visibility="collapsed"
#             )
#             if new_val != row_data.get('country_sub'):
#                 st.session_state['edited_data'][result.row_index]['country_sub'] = new_val
        
#         with cols[5]:
#             # Country Parent
#             default_val = row_data.get('country_parent', '')
#             if not default_val and lookup_result and lookup_result.parent_country:
#                 default_val = lookup_result.parent_country
            
#             idx = options['countries'].index(default_val) if default_val in options['countries'] else 0
#             new_val = st.selectbox(
#                 "cpar",
#                 options['countries'],
#                 index=idx,
#                 key=f"cpar_{result.row_index}",
#                 label_visibility="collapsed"
#             )
#             if new_val != row_data.get('country_parent'):
#                 st.session_state['edited_data'][result.row_index]['country_parent'] = new_val
        
#         with cols[6]:
#             # Lookup button
#             if st.button("🔍", key=f"lookup_btn_{result.row_index}", help="Auto-lookup"):
#                 run_single_lookup(handler, result)
        
#         with cols[7]:
#             # Discard button (X)
#             if st.button("✕", key=f"discard_row_{result.row_index}", help="Remove this row"):
#                 st.session_state['user_decisions'][result.row_index] = 'skip'
#                 st.rerun()
        
#         st.markdown("---")


def render_duplicate_row(result: ValidationResult, handler: BulkUploadHandler):
    """Render a duplicate row with Keep button"""
    
    row_data = result.data
    match_name = result.fuzzy_matches[0][0] if result.fuzzy_matches else "Unknown"
    
    col1, col2, col3 = st.columns([3, 2, 1])
    
    with col1:
        st.text(f"{row_data.get('institution_cpi', 'N/A')}")
    
    with col2:
        st.caption(f"Matches: {match_name}")
    
    with col3:
        if st.button("Keep Anyway", key=f"keep_dup_{result.row_index}"):
            result.status = 'valid'
            st.session_state['user_decisions'][result.row_index] = 'insert'
            st.rerun()


def run_single_lookup(handler: BulkUploadHandler, result: ValidationResult):
    """Run auto-lookup for a single institution - uses CACHED lookup service"""
    institution_name = result.data.get('institution_cpi')
    
    with st.spinner(f"Looking up {institution_name}..."):
        try:
            # Use cached lookup service
            if handler.lookup_service is None:
                handler.lookup_service = get_lookup_service()
            
            lookup_result = handler.lookup_service.lookup_institution(institution_name)
            st.session_state['lookup_results'][result.row_index] = lookup_result
            
            # Auto-apply if high confidence
            if lookup_result.confidence_score >= 0.75:
                st.session_state['edited_data'][result.row_index].update({
                    'institution_type_layer1': lookup_result.institution_type_layer1 or st.session_state['edited_data'][result.row_index].get('institution_type_layer1'),
                    'institution_type_layer2': lookup_result.institution_type_layer2 or st.session_state['edited_data'][result.row_index].get('institution_type_layer2'),
                    'institution_type_layer3': lookup_result.institution_type_layer3 or st.session_state['edited_data'][result.row_index].get('institution_type_layer3'),
                    'country_sub': lookup_result.subsidiary_country or st.session_state['edited_data'][result.row_index].get('country_sub'),
                    'country_parent': lookup_result.parent_country or st.session_state['edited_data'][result.row_index].get('country_parent'),
                })
            
            st.success(f"✅ Lookup complete (confidence: {lookup_result.confidence_score*100:.0f}%)")
            st.rerun()
            
        except Exception as e:
            st.error(f"Lookup failed: {str(e)}")


def run_batch_lookup(handler: BulkUploadHandler, results: List[ValidationResult]):
    """Run auto-lookup for multiple institutions - uses CACHED lookup service"""
    
    missing_data_results = [
        r for r in results 
        if (not st.session_state['edited_data'].get(r.row_index, {}).get('institution_type_layer1') or 
            not st.session_state['edited_data'].get(r.row_index, {}).get('country_sub'))
        and st.session_state['user_decisions'].get(r.row_index) == 'insert'
    ]
    
    if not missing_data_results:
        st.info("No incomplete records found.")
        return
    
    lookup_limit = min(len(missing_data_results), 20)
    
    # Use cached lookup service
    if handler.lookup_service is None:
        handler.lookup_service = get_lookup_service()
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for idx, result in enumerate(missing_data_results[:lookup_limit]):
        institution_name = result.data.get('institution_cpi')
        status_text.text(f"Looking up {idx + 1}/{lookup_limit}: {institution_name}")
        
        try:
            lookup_result = handler.lookup_service.lookup_institution(institution_name)
            st.session_state['lookup_results'][result.row_index] = lookup_result
            
            if lookup_result.confidence_score >= 0.75:
                st.session_state['edited_data'][result.row_index].update({
                    'institution_type_layer1': lookup_result.institution_type_layer1 or st.session_state['edited_data'][result.row_index].get('institution_type_layer1'),
                    'institution_type_layer2': lookup_result.institution_type_layer2 or st.session_state['edited_data'][result.row_index].get('institution_type_layer2'),
                    'institution_type_layer3': lookup_result.institution_type_layer3 or st.session_state['edited_data'][result.row_index].get('institution_type_layer3'),
                    'country_sub': lookup_result.subsidiary_country or st.session_state['edited_data'][result.row_index].get('country_sub'),
                    'country_parent': lookup_result.parent_country or st.session_state['edited_data'][result.row_index].get('country_parent'),
                })
            
        except Exception as e:
            print(f"Lookup failed for {institution_name}: {str(e)}")
        
        progress_bar.progress((idx + 1) / lookup_limit)
    
    progress_bar.empty()
    status_text.empty()
    st.success(f"Completed {lookup_limit} lookups")
    st.rerun()


def execute_bulk_insert(handler: BulkUploadHandler, validation_results: List[ValidationResult]):
    """Execute the bulk insert operation"""
    
    records_to_insert = [
        result for result in validation_results
        if st.session_state['user_decisions'].get(result.row_index) == 'insert'
    ]
    
    if not records_to_insert:
        st.warning("No records selected for insertion.")
        st.session_state['show_confirm'] = False
        return
    
    # Confirmation dialog
    st.warning(f"You are about to insert {len(records_to_insert)} institutions into the database.")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("✅ Confirm Upload", type="primary", use_container_width=True):
            with st.spinner("Uploading to database..."):
                success_count = 0
                failed_count = 0
                results_details = []
                
                for result in records_to_insert:
                    row_data = st.session_state['edited_data'].get(result.row_index, result.data)
                    
                    try:
                        creation_result = handler.service.create_institution(
                            institution_name=row_data.get('institution_cpi', ''),
                            institution_type_layer1=row_data.get('institution_type_layer1'),
                            institution_type_layer2=row_data.get('institution_type_layer2'),
                            institution_type_layer3=row_data.get('institution_type_layer3'),
                            country_sub=row_data.get('country_sub'),
                            country_parent=row_data.get('country_parent'),
                            double_counting_risk=row_data.get('double_counting_risk'),
                            contact_info=row_data.get('contact_info'),
                            comments=row_data.get('comments'),
                            user=st.session_state.get('username', 'analyst')
                        )
                        
                        if creation_result['success']:
                            success_count += 1
                            results_details.append({
                                'name': row_data.get('institution_cpi'),
                                'status': 'success'
                            })
                        else:
                            failed_count += 1
                            results_details.append({
                                'name': row_data.get('institution_cpi'),
                                'status': 'failed',
                                'message': creation_result['message']
                            })
                    
                    except Exception as e:
                        failed_count += 1
                        results_details.append({
                            'name': row_data.get('institution_cpi'),
                            'status': 'failed',
                            'message': str(e)
                        })
                
                # Clear cache to pick up new data
                st.cache_data.clear()
                
                st.session_state['upload_complete'] = True
                st.session_state['upload_results'] = {
                    'success_count': success_count,
                    'failed_count': failed_count,
                    'details': results_details
                }
                st.session_state['show_confirm'] = False
                st.rerun()
    
    with col2:
        if st.button("Cancel", use_container_width=True):
            st.session_state['show_confirm'] = False
            st.rerun()


def show_upload_results():
    """Display upload results"""
    results = st.session_state['upload_results']
    
    st.success(f"Upload Complete!")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Successfully Inserted", results['success_count'])
    with col2:
        st.metric("Failed", results['failed_count'])
    
    if results['failed_count'] > 0:
        with st.expander("View Failed Records"):
            failed_records = [r for r in results['details'] if r['status'] == 'failed']
            for record in failed_records:
                st.error(f"{record['name']}: {record.get('message', 'Unknown error')}")
    
    if st.button("Start New Upload"):
        # Reset all session state
        st.session_state['bulk_upload_df'] = None
        st.session_state['validation_results'] = None
        st.session_state['edited_data'] = {}
        st.session_state['user_decisions'] = {}
        st.session_state['lookup_results'] = {}
        st.session_state['upload_complete'] = False
        st.session_state['upload_results'] = None
        st.rerun()