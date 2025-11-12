import streamlit as st
import pandas as pd
import io
import os
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import re
import boto3
from botocore.exceptions import ClientError

from utils.fuzzy_matching import get_fitted_matcher
from utils.text_processing import TextProcessor
from config import AWS_REGION, S3_BUCKET


@dataclass
class MatchResult:
    original_name: str
    row_index: int
    match_type: str  # 'exact', 'fuzzy', 'none'
    exact_match: Optional[Dict[str, str]] = None  # {nzft_id, entity, entity_clean, country_cpi}
    fuzzy_matches: List[Tuple[str, float, Dict[str, str]]] = None  # (display_name, score, {nzft_id, entity, entity_clean, country_cpi})
    selected_match: Optional[Dict[str, str]] = None




@st.cache_data(ttl=14400)  # Cache for 4 hours, could honestly be longer
def load_nzft_data_cached() -> Optional[pd.DataFrame]:
    """Loads NZFT data from AWS as a csv file, can change the location of this directly 
    """
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        s3_key = 'auxiliary-data/reference-data/reference-db-2/nzft.csv'
        
        st.info(f"Attempting to load NZFT data from s3://{S3_BUCKET}/{s3_key}")
        
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        nzft_df = pd.read_csv(obj['Body'])
        
        st.success(f"Successfully loaded NZFT data with {len(nzft_df)} rows")
        
        required_columns = ['nzft_id', 'entity', 'entity_clean', 'country_cpi']
        missing_columns = [col for col in required_columns if col not in nzft_df.columns]
        
        if missing_columns:
            st.error(f"NZFT data missing required columns: {missing_columns}")
            st.info(f"Available columns: {list(nzft_df.columns)}")
            return None
        
        nzft_df['entity'] = nzft_df['entity'].fillna('').astype(str)
        nzft_df['entity_clean'] = nzft_df['entity_clean'].fillna('').astype(str)
        nzft_df['country_cpi'] = nzft_df['country_cpi'].fillna('').astype(str)
        nzft_df['nzft_id'] = nzft_df['nzft_id'].fillna('').astype(str)
        
        return nzft_df
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            st.error(f"NZFT file not found at s3://{S3_BUCKET}/{s3_key}")
        elif error_code == 'NoSuchBucket':
            st.error(f"S3 bucket '{S3_BUCKET}' not found")
        elif error_code in ['AccessDenied', 'AuthorizationHeaderMalformed', 'InvalidAccessKeyId']:
            st.error(f"AWS authentication error: {str(e)}")
            st.info("This suggests an issue with AWS credentials or permissions. Since other tables work, try checking if you have specific permissions for this file.")
        else:
            st.error(f"S3 error ({error_code}): {str(e)}")
        return None
    except Exception as e:
        st.error(f"Error loading NZFT data: {str(e)}")
        st.exception(e)
        return None


class NZFTMatcher:
    """Runs exact matching and fuzzy matching on normalized versions of hte text"""
    
    def __init__(self):
        self.target_column = 'institution'  # Default column name in the uploaded file, can change if there's something else that's commonly used like entity or whatever
        self.nzft_df = None
        self.suffix_patterns = [
            r'\s+(llc|ltd|limited|inc|incorporated|corp|corporation)\.?$',
            r'\s+(gmbh|sarl|srl|pvt|pty|pte|bv|nv|ag|sa|sas|ab)\.?$',
            r'\s+(plc|public\s+limited\s+company|se|oyj|spa)\.?$'
        ]


        
    
    def load_nzft_data(self) -> bool:
        """Load NZFT reference data from fixed S3 location"""
        self.nzft_df = load_nzft_data_cached()
        return self.nzft_df is not None


        
    
    def normalize_for_matching(self, name: str) -> str:
        """Normalize name for exact matching (includes suffix removal)"""
        if not name:
            return ""
        
        normalized = TextProcessor.normalize_institution_name(name).lower().strip()
        
        for pattern in self.suffix_patterns:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE).strip()
        
        return normalized


        
    
    def find_exact_matches(self, input_names: List[str]) -> Dict[int, Dict[str, str]]:
        """Find exact matches against both entity and entity_clean columns"""
        exact_matches = {}
        
        if self.nzft_df is None or self.nzft_df.empty:
            return exact_matches
        
        # Create lookup dictionaries for both entity and entity_clean
        entity_lookup = {}
        entity_clean_lookup = {}
        
        for _, row in self.nzft_df.iterrows():
            match_data = {
                'nzft_id': str(row['nzft_id']),
                'entity': str(row['entity']),
                'entity_clean': str(row['entity_clean']),
                'country_cpi': str(row['country_cpi'])
            }
            
            entity_normalized = self.normalize_for_matching(str(row['entity']))
            if entity_normalized:
                entity_lookup[entity_normalized] = match_data
            
            entity_clean_normalized = self.normalize_for_matching(str(row['entity_clean']))
            if entity_clean_normalized:
                entity_clean_lookup[entity_clean_normalized] = match_data
        
        for idx, input_name in enumerate(input_names):
            if not input_name:
                continue
            
            input_normalized = self.normalize_for_matching(input_name)
            
            if input_normalized in entity_lookup:
                exact_matches[idx] = entity_lookup[input_normalized]
            elif input_normalized in entity_clean_lookup:
                exact_matches[idx] = entity_clean_lookup[input_normalized]
        
        return exact_matches



        
    
    def find_fuzzy_matches(self, input_names: List[str], exclude_exact: Dict[int, Dict[str, str]]) -> Dict[int, List[Tuple[str, float, Dict[str, str]]]]:
        """Find fuzzy matches for non-exact entries"""
        fuzzy_matches = {}
        
        if self.nzft_df is None or self.nzft_df.empty:
            return fuzzy_matches
        
        try:
            matcher_df = pd.DataFrame()
            matcher_df['institution_cpi'] = self.nzft_df['entity'].fillna('') + ' | ' + self.nzft_df['entity_clean'].fillna('')
            matcher_df['country_sub'] = self.nzft_df['country_cpi']
            
            matcher = get_fitted_matcher(matcher_df, threshold=0.70)
            
            for idx, input_name in enumerate(input_names):
                if idx in exclude_exact or not input_name:
                    continue
                
                matches = matcher.find_similar_institutions(
                    query=input_name,
                    institution_df=matcher_df,
                    limit=5,
                    tfidf_top_k=50
                )
                
                if matches:
                    enhanced_matches = []
                    for combined_name, score in matches:
                        # Find the original NZFT row that corresponds to this match
                        for _, nzft_row in self.nzft_df.iterrows():
                            expected_combined = str(nzft_row['entity']) + ' | ' + str(nzft_row['entity_clean'])
                            if expected_combined == combined_name:
                                match_data = {
                                    'nzft_id': str(nzft_row['nzft_id']),
                                    'entity': str(nzft_row['entity']),
                                    'entity_clean': str(nzft_row['entity_clean']),
                                    'country_cpi': str(nzft_row['country_cpi'])
                                }
                                
                                # Create display name for UI
                                display_name = str(nzft_row['entity'])
                                if str(nzft_row['entity_clean']) and str(nzft_row['entity_clean']) != str(nzft_row['entity']):
                                    display_name += f" / {nzft_row['entity_clean']}"
                                
                                enhanced_matches.append((display_name, score, match_data))
                                break
                    
                    if enhanced_matches:
                        fuzzy_matches[idx] = enhanced_matches
        
        except Exception as e:
            st.error(f"Error in fuzzy matching: {str(e)}")
        
        return fuzzy_matches


        
    
    def process_upload(self, df: pd.DataFrame) -> List[MatchResult]:
        """Process an uploaded DataFrame and return matching results"""
        results = []
        
        if self.target_column not in df.columns:
            available_columns = list(df.columns)
            st.error(f"Column '{self.target_column}' not found. Available columns: {available_columns}")
            
            # Try to find a suitable column
            possible_columns = ['institution', 'entity', 'name', 'company', 'organization']
            found_column = None
            for col in possible_columns:
                if col in available_columns:
                    found_column = col
                    break
            
            if found_column:
                st.warning(f"Using column '{found_column}' instead of '{self.target_column}'")
                self.target_column = found_column
            else:
                st.error("No suitable column found for matching. Please ensure your file has a column named 'institution', 'entity', 'name', 'company', or 'organization'")
                return results
        
        input_names = df[self.target_column].fillna('').astype(str).tolist()
        
        if not self.load_nzft_data():
            st.error("Failed to load NZFT reference data. Please check your S3 configuration and file location.")
            st.info(f"Expected location: s3://{S3_BUCKET}/auxiliary-data/reference-data/reference-db-2/nzft.csv")
            return results
        
        # Find exact matches first
        exact_matches = self.find_exact_matches(input_names)
        
        # Find fuzzy matches for non-exact entries
        fuzzy_matches = self.find_fuzzy_matches(input_names, exact_matches)
        
        # Create MatchResult objects
        for idx, name in enumerate(input_names):
            if idx in exact_matches:
                results.append(MatchResult(
                    original_name=name,
                    row_index=idx,
                    match_type='exact',
                    exact_match=exact_matches[idx]
                ))
            elif idx in fuzzy_matches:
                results.append(MatchResult(
                    original_name=name,
                    row_index=idx,
                    match_type='fuzzy',
                    fuzzy_matches=fuzzy_matches[idx]
                ))
            else:
                results.append(MatchResult(
                    original_name=name,
                    row_index=idx,
                    match_type='none'
                ))
        
        return results


def render_nzft_page():
    """Render the NZFT matching page"""
    st.header("NZFT Institution Matching")
    st.markdown("Upload a file with institution names to match against the NZFT database")
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
    if 'nzft_final_df' not in st.session_state:
        st.session_state['nzft_final_df'] = None
    if 'nzft_last_file' not in st.session_state:
        st.session_state['nzft_last_file'] = None
    
    matcher = NZFTMatcher()
    
    # File upload
    st.subheader("1. Upload Institution Data")
    uploaded_file = st.file_uploader(
        "Choose a CSV or Excel file",
        type=['csv', 'xlsx'],
        help="File should contain a column named 'institution' with institution names to match"
    )
    
    if uploaded_file is not None:
        file_key = f"{uploaded_file.name}_{uploaded_file.size}"
        
        # Reset if new file uploaded
        if st.session_state['nzft_last_file'] != file_key:
            reset_nzft_session()
            st.session_state['nzft_last_file'] = file_key
        
        if st.session_state['nzft_uploaded_df'] is None:
            with st.spinner("Processing file..."):
                try:
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)
                    
                    st.session_state['nzft_uploaded_df'] = df
                    
                except Exception as e:
                    st.error(f"Error parsing file: {str(e)}")
                    return
        
        df = st.session_state['nzft_uploaded_df']
        
        st.subheader("File Preview")
        st.dataframe(df.head(10), use_container_width=True)
        st.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        
        # Process Matches
        if st.session_state['nzft_match_results'] is None:
            with st.spinner("Finding matches in NZFT database..."):
                try:
                    match_results = matcher.process_upload(df)
                    st.session_state['nzft_match_results'] = match_results
                    
                    # Auto-confirm exact matches
                    for result in match_results:
                        if result.match_type == 'exact':
                            st.session_state['nzft_exact_confirmations'][result.row_index] = result.exact_match
                    
                except Exception as e:
                    st.error(f"Error processing matches: {str(e)}")
                    return
        
        match_results = st.session_state['nzft_match_results']
        
        exact_results = [r for r in match_results if r.match_type == 'exact']
        fuzzy_results = [r for r in match_results if r.match_type == 'fuzzy']
        no_match_results = [r for r in match_results if r.match_type == 'none']
        
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
        
        # Show exact matches
        if exact_results:
            st.markdown("---")
            with st.expander(f"Exact Matches ({len(exact_results)}) - Automatically kept"):
                st.info("These institutions were found as exact matches and will be automatically included in the results")
                
                exact_data = []
                for result in exact_results:
                    match_data = result.exact_match
                    exact_data.append({
                        'Original Name': result.original_name,
                        'NZFT Entity': match_data['entity'],
                        'NZFT Entity Clean': match_data['entity_clean'],
                        'Country': match_data['country_cpi'],
                        'NZFT ID': match_data['nzft_id']
                    })
                
                exact_df = pd.DataFrame(exact_data)
                st.dataframe(exact_df, use_container_width=True, hide_index=True)
        
        # Show fuzzy matches for user review
        if fuzzy_results:
            st.markdown("---")
            st.subheader("Fuzzy Matches")
            st.info("Review these potential matches and select the correct ones")
            
            for result in fuzzy_results:
                with st.container():
                    st.markdown(f"**Original:** {result.original_name}")
                    
                    if result.fuzzy_matches:
                        st.markdown("**Potential matches:**")
                        
                        options = ["No match"]
                        option_values = [None]
                        
                        for display_name, score, match_data in result.fuzzy_matches:
                            country_str = f" ({match_data['country_cpi']})" if match_data['country_cpi'] else ""
                            option_text = f"{display_name}{country_str} - {score*100:.1f}% match"
                            options.append(option_text)
                            option_values.append(match_data)
                        
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
                        
                        selected_value = option_values[options.index(selected_option)]
                        st.session_state['nzft_user_selections'][result.row_index] = selected_value
                    
                    else:
                        st.warning("No fuzzy matches found")
                    
                    st.markdown("---")
        
        if no_match_results:
            with st.expander(f"No Matches Found ({len(no_match_results)})"):
                for result in no_match_results:
                    st.text(f"• {result.original_name}")
        
        st.markdown("---")
        st.subheader("3. Generate Results")
        
        if st.button("Finish Matching", type="primary", use_container_width=True):
            final_df = generate_final_results(df, match_results, matcher.target_column)
            st.session_state['nzft_final_df'] = final_df
            
            st.subheader("Final Results Preview")
            st.dataframe(final_df, use_container_width=True)
            
            matched_count = (final_df['nzft_id'] != '').sum()
            st.success(f"Processing complete! {matched_count} out of {len(final_df)} institutions matched.")
            
            csv_buffer = io.StringIO()
            final_df.to_csv(csv_buffer, index=False)

            #downloads as csv, can be changed if it would be more useful to uplaod to aws or something
            st.download_button(
                label="Download Results as CSV",
                data=csv_buffer.getvalue(),
                file_name=f"nzft_matched_institutions_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )




    
def generate_final_results(original_df: pd.DataFrame, match_results: List[MatchResult], target_column: str) -> pd.DataFrame:
    """Generate final DataFrame with separate nzft_id, entity, and entity_clean columns to denote the match, right now it keeps the other current values from the uploaded df, can be switched to keep the values from the nzft file instead"""
    final_df = original_df.copy()
    
    target_col_index = final_df.columns.get_loc(target_column)
    
    # Initialize the three new columns
    nzft_ids = [''] * len(final_df)
    entities = [''] * len(final_df)
    entity_cleans = [''] * len(final_df)
    
    for result in match_results:
        match_data = None
        
        if result.match_type == 'exact' and result.exact_match:
            confirmed_match = st.session_state['nzft_exact_confirmations'].get(result.row_index)
            if confirmed_match:
                match_data = confirmed_match
        
        elif result.match_type == 'fuzzy':
            selected_match = st.session_state['nzft_user_selections'].get(result.row_index)
            if selected_match:
                match_data = selected_match
        
        if match_data:
            nzft_ids[result.row_index] = match_data.get('nzft_id', '')
            entities[result.row_index] = match_data.get('entity', '')
            entity_cleans[result.row_index] = match_data.get('entity_clean', '')
    
    # Insert the three new columns after the target column
    final_df.insert(target_col_index + 1, 'nzft_id', nzft_ids)
    final_df.insert(target_col_index + 2, 'entity', entities)
    final_df.insert(target_col_index + 3, 'entity_clean', entity_cleans)
    
    return final_df






def reset_nzft_session():
    for key in ['nzft_uploaded_df', 'nzft_match_results', 'nzft_user_selections', 
                'nzft_exact_confirmations', 'nzft_final_df', 'nzft_last_file']:
        if key in st.session_state:
            del st.session_state[key]