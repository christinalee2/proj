"""
NZFT Institution Matching Page
Handles bulk uploads for institution matching with exact and fuzzy matching capabilities
"""
import streamlit as st
import pandas as pd
import io
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import re

from database.cached_queries import get_table_data_cached
from utils.fuzzy_matching import get_fitted_matcher
from utils.text_processing import TextProcessor


@dataclass
class MatchResult:
    """Result of matching process for one institution"""
    original_name: str
    row_index: int
    match_type: str  # 'exact', 'fuzzy', 'none'
    exact_match: Optional[str] = None
    fuzzy_matches: List[Tuple[str, float, str]] = None  # (name, score, country)
    selected_match: Optional[str] = None


class NZFTMatcher:
    """Handles NZFT institution matching logic"""
    
    def __init__(self):
        self.target_column = 'institution_cpi'  # Make this configurable
        self.suffix_patterns = [
            r'\s+(llc|ltd|limited|inc|incorporated|corp|corporation)\.?$',
            r'\s+(gmbh|sarl|srl|pvt|pty|pte|bv|nv|ag|sa|sas|ab)\.?$',
            r'\s+(plc|public\s+limited\s+company|se|oyj|spa)\.?$'
        ]
    
    def normalize_for_matching(self, name: str) -> str:
        """Normalize name for exact matching (includes suffix removal)"""
        if not name:
            return ""
        
        normalized = TextProcessor.normalize_institution_name(name).lower().strip()
        
        # Try removing common suffixes for matching
        for pattern in self.suffix_patterns:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE).strip()
        
        return normalized
    
    def find_exact_matches(self, input_names: List[str], institution_df: pd.DataFrame) -> Dict[int, str]:
        """Find exact matches (including with suffix variations)"""
        exact_matches = {}
        
        if institution_df.empty or 'institution_cpi' not in institution_df.columns:
            return exact_matches
        
        # Create lookup dict for institution names
        institution_lookup = {}
        for _, row in institution_df.iterrows():
            inst_name = str(row['institution_cpi']).strip()
            normalized = self.normalize_for_matching(inst_name)
            if normalized:
                institution_lookup[normalized] = inst_name
        
        # Check each input name
        for idx, input_name in enumerate(input_names):
            if not input_name:
                continue
            
            input_normalized = self.normalize_for_matching(input_name)
            
            # Check for exact match (including suffix variations)
            if input_normalized in institution_lookup:
                exact_matches[idx] = institution_lookup[input_normalized]
        
        return exact_matches
    
    def find_fuzzy_matches(self, input_names: List[str], institution_df: pd.DataFrame, 
                          exclude_exact: Dict[int, str]) -> Dict[int, List[Tuple[str, float, str]]]:
        """Find fuzzy matches for non-exact entries"""
        fuzzy_matches = {}
        
        if institution_df.empty:
            return fuzzy_matches
        
        try:
            # Get fitted matcher
            matcher = get_fitted_matcher(institution_df, threshold=0.70)  # Lower threshold for more matches
            
            for idx, input_name in enumerate(input_names):
                if idx in exclude_exact or not input_name:
                    continue
                
                # Get fuzzy matches
                matches = matcher.find_similar_institutions(
                    query=input_name,
                    institution_df=institution_df,
                    limit=5,
                    tfidf_top_k=50
                )
                
                if matches:
                    # Add country information
                    enhanced_matches = []
                    for name, score in matches:
                        # Find country info for this institution
                        country = ""
                        matching_row = institution_df[institution_df['institution_cpi'] == name]
                        if not matching_row.empty:
                            country = str(matching_row.iloc[0].get('country_sub', '')).strip()
                        
                        enhanced_matches.append((name, score, country))
                    
                    fuzzy_matches[idx] = enhanced_matches
            
        except Exception as e:
            st.error(f"Error in fuzzy matching: {str(e)}")
        
        return fuzzy_matches
    
    def process_upload(self, df: pd.DataFrame, institution_df: pd.DataFrame) -> List[MatchResult]:
        """Process uploaded file and find all matches"""
        if self.target_column not in df.columns:
            raise ValueError(f"Required column '{self.target_column}' not found in uploaded file")
        
        input_names = df[self.target_column].fillna('').astype(str).tolist()
        
        # Find exact matches
        exact_matches = self.find_exact_matches(input_names, institution_df)
        
        # Find fuzzy matches for non-exact entries
        fuzzy_matches = self.find_fuzzy_matches(input_names, institution_df, exact_matches)
        
        # Create match results
        results = []
        for idx, name in enumerate(input_names):
            if idx in exact_matches:
                result = MatchResult(
                    original_name=name,
                    row_index=idx,
                    match_type='exact',
                    exact_match=exact_matches[idx]
                )
            elif idx in fuzzy_matches:
                result = MatchResult(
                    original_name=name,
                    row_index=idx,
                    match_type='fuzzy',
                    fuzzy_matches=fuzzy_matches[idx]
                )
            else:
                result = MatchResult(
                    original_name=name,
                    row_index=idx,
                    match_type='none'
                )
            
            results.append(result)
        
        return results


def render_nzft_page():
    """Render the NZFT institution matching page"""
    st.header("NZFT Institution Matching")
    st.markdown("Upload a file with institutions to match against the database")
    st.markdown("---")
    
    # Initialize session state
    if 'nzft_uploaded_df' not in st.session_state:
        st.session_state['nzft_uploaded_df'] = None
    if 'nzft_match_results' not in st.session_state:
        st.session_state['nzft_match_results'] = None
    if 'nzft_user_selections' not in st.session_state:
        st.session_state['nzft_user_selections'] = {}
    if 'nzft_exact_confirmations' not in st.session_state:
        st.session_state['nzft_exact_confirmations'] = {}
    
    matcher = NZFTMatcher()
    
    # Configuration section
    with st.expander("Configuration"):
        target_column = st.text_input(
            "Target Column Name",
            value=matcher.target_column,
            help="The column name in your upload file that contains institution names"
        )
        matcher.target_column = target_column
    
    # File upload
    st.subheader("1. Upload File")
    uploaded_file = st.file_uploader(
        "Choose CSV or Excel file with institutions to match",
        type=['csv', 'xlsx', 'xls'],
        key="nzft_upload"
    )
    
    if uploaded_file is not None:
        # Parse file
        if st.session_state['nzft_uploaded_df'] is None or uploaded_file.name != st.session_state.get('nzft_last_file'):
            with st.spinner("Loading file..."):
                try:
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file, engine='c')
                    else:
                        df = pd.read_excel(uploaded_file, engine='openpyxl')
                    
                    df.columns = df.columns.str.strip()
                    
                    # Validate target column exists
                    if matcher.target_column not in df.columns:
                        st.error(f"Column '{matcher.target_column}' not found. Available columns: {', '.join(df.columns)}")
                        return
                    
                    st.session_state['nzft_uploaded_df'] = df
                    st.session_state['nzft_last_file'] = uploaded_file.name
                    st.session_state['nzft_match_results'] = None
                    st.session_state['nzft_user_selections'] = {}
                    st.session_state['nzft_exact_confirmations'] = {}
                    
                except Exception as e:
                    st.error(f"Error parsing file: {str(e)}")
                    return
        
        df = st.session_state['nzft_uploaded_df']
        
        # Show file preview
        st.subheader("File Preview")
        st.dataframe(df.head(10), use_container_width=True)
        st.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        
        # Process matches
        if st.session_state['nzft_match_results'] is None:
            with st.spinner("Finding matches in institution database..."):
                try:
                    institution_df = get_table_data_cached('institution', limit=None)
                    match_results = matcher.process_upload(df, institution_df)
                    st.session_state['nzft_match_results'] = match_results
                    
                    # Initialize exact confirmations for all exact matches
                    for result in match_results:
                        if result.match_type == 'exact':
                            st.session_state['nzft_exact_confirmations'][result.row_index] = result.exact_match
                    
                except Exception as e:
                    st.error(f"Error processing matches: {str(e)}")
                    return
        
        match_results = st.session_state['nzft_match_results']
        
        # Separate results by type
        exact_results = [r for r in match_results if r.match_type == 'exact']
        fuzzy_results = [r for r in match_results if r.match_type == 'fuzzy']
        no_match_results = [r for r in match_results if r.match_type == 'none']
        
        # Summary
        st.markdown("---")
        st.subheader("2. Matching Results")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Institutions", len(match_results))
        with col2:
            st.metric("Exact Matches", len(exact_results))
        with col3:
            st.metric("Fuzzy Matches", len(fuzzy_results))
        with col4:
            st.metric("No Matches", len(no_match_results))
        
        # Exact matches section
        if exact_results:
            st.markdown("---")
            with st.expander(f"Exact Matches ({len(exact_results)}) - Automatically kept"):
                st.info("These institutions were found as exact matches (including suffix variations) and will be automatically included in the results")
                
                # Create a dataframe for exact matches
                exact_data = []
                for result in exact_results:
                    exact_data.append({
                        'Original Name': result.original_name,
                        'Database Match': result.exact_match
                    })
                
                exact_df = pd.DataFrame(exact_data)
                st.dataframe(exact_df, use_container_width=True, hide_index=True)
        
        # Fuzzy matches section
        if fuzzy_results:
            st.markdown("---")
            st.subheader("Fuzzy Matches")
            st.info("Review these potential matches and select the correct ones")
            
            for result in fuzzy_results:
                with st.container():
                    st.markdown(f"**Original:** {result.original_name}")
                    
                    if result.fuzzy_matches:
                        st.markdown("**Potential matches:**")
                        
                        # Create radio button options
                        options = ["No match"]
                        option_values = [None]
                        
                        for name, score, country in result.fuzzy_matches:
                            country_str = f" ({country})" if country else ""
                            option_text = f"{name}{country_str} - {score*100:.1f}% match"
                            options.append(option_text)
                            option_values.append(name)
                        
                        # Get current selection
                        current_selection = st.session_state['nzft_user_selections'].get(result.row_index)
                        try:
                            default_index = option_values.index(current_selection) if current_selection in option_values else 0
                        except ValueError:
                            default_index = 0
                        
                        selected_option = st.radio(
                            f"Select match for '{result.original_name}':",
                            options=options,
                            index=default_index,
                            key=f"fuzzy_match_{result.row_index}",
                            label_visibility="collapsed"
                        )
                        
                        # Store selection
                        selected_value = option_values[options.index(selected_option)]
                        st.session_state['nzft_user_selections'][result.row_index] = selected_value
                    
                    else:
                        st.warning("No fuzzy matches found")
                    
                    st.markdown("---")
        
        # No matches section
        if no_match_results:
            with st.expander(f"No Matches Found ({len(no_match_results)})"):
                for result in no_match_results:
                    st.text(f"• {result.original_name}")
        
        # Finish and download section
        st.markdown("---")
        st.subheader("3. Generate Results")
        
        if st.button("Finish Matching", type="primary", use_container_width=True):
            # Generate final results
            final_df = generate_final_results(df, match_results, matcher.target_column)
            st.session_state['nzft_final_df'] = final_df
            
            # Show preview
            st.subheader("Final Results Preview")
            st.dataframe(final_df, use_container_width=True)
            
            # Summary of matches
            matched_count = (final_df['match'] != '').sum()
            st.success(f"Processing complete! {matched_count} out of {len(final_df)} institutions matched.")
            
            # Download button
            csv_buffer = io.StringIO()
            final_df.to_csv(csv_buffer, index=False)
            
            st.download_button(
                label="Download Results as CSV",
                data=csv_buffer.getvalue(),
                file_name=f"nzft_matched_institutions_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )


def generate_final_results(original_df: pd.DataFrame, match_results: List[MatchResult], target_column: str) -> pd.DataFrame:
    """Generate final DataFrame with match column"""
    # Create a copy of the original DataFrame
    final_df = original_df.copy()
    
    # Find the position to insert the match column (right after target column)
    target_col_index = final_df.columns.get_loc(target_column)
    
    # Create match values
    match_values = [''] * len(final_df)
    
    for result in match_results:
        if result.match_type == 'exact' and result.exact_match:
            # Use exact match from session state confirmations
            confirmed_match = st.session_state['nzft_exact_confirmations'].get(result.row_index)
            if confirmed_match:
                match_values[result.row_index] = confirmed_match
        
        elif result.match_type == 'fuzzy':
            # Use user selection from session state
            selected_match = st.session_state['nzft_user_selections'].get(result.row_index)
            if selected_match:
                match_values[result.row_index] = selected_match
    
    # Insert the match column right after the target column
    final_df.insert(target_col_index + 1, 'match', match_values)
    
    return final_df


def reset_nzft_session():
    """Reset NZFT session state"""
    for key in ['nzft_uploaded_df', 'nzft_match_results', 'nzft_user_selections', 
                'nzft_exact_confirmations', 'nzft_final_df', 'nzft_last_file']:
        if key in st.session_state:
            del st.session_state[key]