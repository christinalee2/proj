"""
Simple institution entry form - FORM ALWAYS VISIBLE
"""
import streamlit as st
from typing import List, Optional, Tuple
from services.institution_service import InstitutionService
from services.institution_lookup_service import InstitutionLookupService
from utils.text_processing import TextProcessor
from utils.fuzzy_matching import get_fitted_matcher
import pandas as pd


def normalize_name(name: str) -> str:
    """Normalize name for comparison"""
    if not name:
        return ""
    return TextProcessor.normalize_institution_name(name).lower().strip()


def check_exact_duplicate(input_name: str, existing_df: pd.DataFrame) -> Optional[str]:
    """Check for exact duplicate"""
    normalized_input = normalize_name(input_name)
    
    for _, row in existing_df.iterrows():
        existing_name = row.get('institution_cpi', '')
        if normalize_name(existing_name) == normalized_input:
            return existing_name
    return None


def check_fuzzy_matches(input_name: str, existing_df: pd.DataFrame) -> List[Tuple[str, float]]:
    """Find fuzzy matches"""
    matcher = get_fitted_matcher(existing_df, threshold=0.85)
    matches = matcher.find_similar_institutions(
        query=input_name,
        institution_df=existing_df,
        limit=5,
        tfidf_top_k=50
    )
    # Filter out exact matches
    return [(name, score) for name, score in matches 
            if normalize_name(name) != normalize_name(input_name)]


@st.cache_data(ttl=300)
def load_institutions() -> pd.DataFrame:
    """Load institutions (cached)"""
    service = InstitutionService()
    return service.query_service.get_all_institutions()


@st.cache_data(ttl=300)
def load_countries() -> pd.DataFrame:
    """Load countries (cached)"""
    service = InstitutionService()
    return service.query_service.get_countries()


@st.cache_data(ttl=300)
def load_types() -> dict:
    """Load institution types (cached)"""
    service = InstitutionService()
    return service.query_service.get_institution_types()


def render_simple_institution_form():
    """Render institution form - FORM IS ALWAYS VISIBLE"""
    
    # Initialize session state for lookup
    if 'lookup_result' not in st.session_state:
        st.session_state['lookup_result'] = None
    if 'lookup_used' not in st.session_state:
        st.session_state['lookup_used'] = False
    
    # Load data
    existing_institutions = load_institutions()
    service = InstitutionService()
    
    # Institution name input
    institution_name = st.text_input(
        "Institution Name",
        placeholder="Type institution name...",
        help="Enter institution name and press Enter",
        key="inst_name"
    )
    
    # Show warnings (ALL informational, ALL visible, NONE blocking)
    if institution_name and len(institution_name) >= 3:
        
        # Exact duplicate warning
        exact = check_exact_duplicate(institution_name, existing_institutions)
        if exact:
            st.warning(f"'{exact}' already exists in the database.")
        
        # Fuzzy matches - always expanded, no emoji
        try:
            fuzzy = check_fuzzy_matches(institution_name, existing_institutions)
            if fuzzy:
                st.info(f"Found {len(fuzzy)} similar institution(s)")
                st.caption("Already in database. If yours is different, continue below.")
                
                # Get full details for matched institutions
                for name, score in fuzzy:
                    try:
                        # Find the matching row to get type and country
                        match_row = existing_institutions[existing_institutions['institution_cpi'] == name]
                        
                        if not match_row.empty:
                            inst_type = match_row.iloc[0].get('institution_type_layer1', 'N/A')
                            country = match_row.iloc[0].get('country_sub', 'N/A')
                            st.text(f"• {name} ({inst_type}, {country}) - {score * 100:.2f}% match")
                        else:
                            st.text(f"• {name} - {score * 100:.2f}% match")
                    except Exception as e:
                        st.text(f"• {name} - {score * 100:.2f}% match")
        except Exception as e:
            st.error(f"Error checking for similar institutions: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
    
    # FORM ALWAYS SHOWS BELOW THIS LINE - NO CONDITIONS
    st.markdown("---")
    
    # Auto-lookup button
    if institution_name and len(institution_name) >= 3:
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("Auto-Lookup", key="lookup_btn", help="Automatically find institution details from trusted sources"):
                with st.spinner("Searching trusted sources and extracting data..."):
                    try:
                        # Get valid countries from existing institutions
                        valid_countries = set()
                        if not existing_institutions.empty:
                            if 'country_sub' in existing_institutions.columns:
                                valid_countries.update(existing_institutions['country_sub'].dropna().unique())
                            if 'country_parent' in existing_institutions.columns:
                                valid_countries.update(existing_institutions['country_parent'].dropna().unique())
                        
                        lookup_service = InstitutionLookupService(valid_countries=list(valid_countries))
                        result = lookup_service.lookup_institution(institution_name)
                        st.session_state['lookup_result'] = result
                        st.session_state['lookup_used'] = False  # Reset flag
                        st.rerun()
                    except Exception as e:
                        st.error(f"Lookup failed: {str(e)}")
    
    # Display lookup results if available
    lookup_result = st.session_state.get('lookup_result')
    if lookup_result and not st.session_state.get('lookup_used', False):
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
                st.session_state['lookup_used'] = True  # Mark as used
                st.rerun()
        
        st.markdown("---")
    
    st.subheader("Institution Details")
    
    # Form fields
    col1, col2 = st.columns(2)
    
    with col1:
        types = load_types()
        
        # Type Layer 1 - with prefill
        type1_options = [''] + ['Public', 'Private'] + types.get('layer1', [])
        type1_index = 0
        if st.session_state.get('prefill_type1') and st.session_state['prefill_type1'] in type1_options:
            type1_index = type1_options.index(st.session_state['prefill_type1'])
        
        type1 = st.selectbox(
            "Institution Type - Layer 1",
            type1_options,
            index=type1_index,
            key="t1"
        )
        
        # Type Layer 2 - with prefill
        type2_options = [''] + ['Funds', 'Corporation', 'Commercial FI', 'Insurance'] + types.get('layer2', [])
        type2_index = 0
        if st.session_state.get('prefill_type2') and st.session_state['prefill_type2'] in type2_options:
            type2_index = type2_options.index(st.session_state['prefill_type2'])
        
        type2 = st.selectbox(
            "Institution Type - Layer 2",
            type2_options,
            index=type2_index,
            key="t2"
        )
        
        # Type Layer 3 - with prefill
        type3_options = [''] + ['Asset Manager', 'Bank', 'Venture Capital Fund', 'Private Equity Fund', 
                   'Insurance Company', 'Corporation', 'Pension Fund'] + types.get('layer3', [])
        type3_index = 0
        if st.session_state.get('prefill_type3') and st.session_state['prefill_type3'] in type3_options:
            type3_index = type3_options.index(st.session_state['prefill_type3'])
        
        type3 = st.selectbox(
            "Institution Type - Layer 3",
            type3_options,
            index=type3_index,
            key="t3"
        )
    
    with col2:
        countries = load_countries()
        country_list = countries['country_cpi'].tolist() if not countries.empty else []
        country_options = [''] + sorted(country_list)
        
        # Subsidiary Country - with prefill
        csub_index = 0
        if st.session_state.get('prefill_sub') and st.session_state['prefill_sub'] in country_options:
            csub_index = country_options.index(st.session_state['prefill_sub'])
        
        country_sub = st.selectbox(
            "Subsidiary Country",
            country_options,
            index=csub_index,
            key="csub",
            help="Country where institution primarily operates"
        )
        
        # Parent Country - with prefill
        cpar_index = 0
        if st.session_state.get('prefill_parent') and st.session_state['prefill_parent'] in country_options:
            cpar_index = country_options.index(st.session_state['prefill_parent'])
        
        country_parent = st.selectbox(
            "Parent Country",
            country_options,
            index=cpar_index,
            key="cpar",
            help="Country where headquarters is located"
        )
        
        dc_risk = st.selectbox(
            "Double Counting Risk",
            ['', 'Low', 'Medium', 'High'],
            key="dc"
        )
    
    with st.expander("Additional Information"):
        contact = st.text_area("Contact Information", key="contact")
        comments = st.text_area("Comments", key="comments")
    
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.button("Add Institution", type="primary", use_container_width=True, key="add"):
            if not institution_name:
                st.error("Please enter institution name")
            elif not type1:
                st.error("Please select Type - Layer 1")
            else:
                with st.spinner("Creating..."):
                    result = service.create_institution(
                        institution_name=institution_name,
                        institution_type_layer1=type1 or None,
                        institution_type_layer2=type2 or None,
                        institution_type_layer3=type3 or None,
                        country_sub=country_sub or None,
                        country_parent=country_parent or None,
                        double_counting_risk=dc_risk or None,
                        contact_info=contact or None,
                        comments=comments or None,
                        user=st.session_state.get('username', 'unknown')
                    )
                    
                    if result['success']:
                        st.success(f"'{result['institution_name']}' created successfully!")
                        load_institutions.clear()
                        
                        # Clear lookup data after successful creation
                        st.session_state['lookup_result'] = None
                        st.session_state['lookup_used'] = False
                        st.session_state.pop('prefill_type1', None)
                        st.session_state.pop('prefill_type2', None)
                        st.session_state.pop('prefill_type3', None)
                        st.session_state.pop('prefill_parent', None)
                        st.session_state.pop('prefill_sub', None)
                        
                        if st.button("Add Another", key="another"):
                            st.rerun()
                    else:
                        st.error(result['message'])
    
    with col2:
        if st.button("Reset", use_container_width=True, key="reset"):
            # Clear all form data including lookup results
            keys_to_clear = ['lookup_result', 'lookup_used', 'prefill_type1', 'prefill_type2', 
                           'prefill_type3', 'prefill_parent', 'prefill_sub']
            for key in keys_to_clear:
                st.session_state.pop(key, None)
            st.rerun()