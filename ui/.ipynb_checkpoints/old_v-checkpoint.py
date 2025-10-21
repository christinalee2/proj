import streamlit as st
from typing import List, Optional, Tuple
from services.institution_service import InstitutionService
from utils.text_processing import TextProcessor
from utils.fuzzy_matching import get_fitted_matcher  # Simplified import
import pandas as pd


def normalize_for_comparison(name: str) -> str:
    """Normalize name for duplicate checking"""
    if not name:
        return ""
    normalized = TextProcessor.normalize_institution_name(name)
    return normalized.lower().strip()


def extract_suffix_variants(name: str) -> List[str]:
    """Generate variants of name with common suffixes removed"""
    suffixes = [
        'inc', 'incorporated', 'corp', 'corporation', 'company', 'co',
        'ltd', 'limited', 'llc', 'l.l.c.', 'plc', 'p.l.c.',
        'sa', 's.a.', 'sas', 's.a.s.', 'gmbh', 'ag', 'nv', 'bv',
        'llp', 'lp', 'pa', 'pc'
    ]
    
    name_lower = name.lower().strip()
    variants = [name_lower]
    
    for suffix in suffixes:
        if name_lower.endswith(f' {suffix}'):
            variants.append(name_lower[:-len(suffix)-1].strip())
        if name_lower.endswith(f', {suffix}'):
            variants.append(name_lower[:-len(suffix)-2].strip())
        if name_lower.endswith(f' {suffix}.'):
            variants.append(name_lower[:-len(suffix)-2].strip())
    
    return list(set(variants))


def check_exact_duplicate(input_name: str, existing_institutions: pd.DataFrame) -> Optional[str]:
    """Check for exact duplicate"""
    normalized_input = normalize_for_comparison(input_name)
    
    for _, row in existing_institutions.iterrows():
        existing_name = row.get('institution_cpi', '')
        normalized_existing = normalize_for_comparison(existing_name)
        
        if normalized_input == normalized_existing:
            return existing_name
    
    return None


def check_suffix_variants(input_name: str, existing_institutions: pd.DataFrame) -> List[str]:
    """Check if input matches existing names with different suffixes"""
    input_variants = extract_suffix_variants(input_name)
    matches = []
    
    for _, row in existing_institutions.iterrows():
        existing_name = row.get('institution_cpi', '')
        existing_variants = extract_suffix_variants(existing_name)
        
        for input_var in input_variants:
            for existing_var in existing_variants:
                if input_var == existing_var and normalize_for_comparison(input_name) != normalize_for_comparison(existing_name):
                    matches.append(existing_name)
                    break
    
    return list(set(matches))


def check_fuzzy_matches(input_name: str, existing_institutions: pd.DataFrame, threshold: float = 0.85) -> List[Tuple[str, float]]:
    """Find fuzzy matches using hybrid TF-IDF + Jaro-Winkler approach (FAST!)"""
    
    # Get cached fitted matcher (vectorization done once)
    fuzzy_matcher = get_fitted_matcher(existing_institutions, threshold=threshold)
    
    # Fast hybrid search: TF-IDF filters to top 50, then Jaro-Winkler ranks top 5
    matches = fuzzy_matcher.find_similar_institutions(
        query=input_name,
        institution_df=existing_institutions,
        limit=5,
        tfidf_top_k=50  # Only run expensive Jaro-Winkler on top 50 TF-IDF matches
    )
    
    # Filter out exact matches (already handled)
    filtered_matches = [
        (name, score) for name, score in matches
        if normalize_for_comparison(name) != normalize_for_comparison(input_name)
    ]
    
    return filtered_matches


@st.cache_data(ttl=300)
def load_institution_names() -> pd.DataFrame:
    """Load all institution names (cached)"""
    institution_service = InstitutionService()
    return institution_service.query_service.get_all_institutions()


@st.cache_data(ttl=300)
def load_countries() -> pd.DataFrame:
    """Load all countries (cached)"""
    institution_service = InstitutionService()
    return institution_service.query_service.get_countries()


@st.cache_data(ttl=300)
def load_institution_types() -> dict:
    """Load institution types (cached)"""
    institution_service = InstitutionService()
    return institution_service.query_service.get_institution_types()


def render_live_institution_form():
    """Render institution form with live autocomplete"""
    
    # CRITICAL: Prevent double rendering
    # Check if this form has already been rendered in this script run
    import streamlit.runtime.scriptrunner as sr
    ctx = sr.get_script_run_ctx()
    if ctx:
        form_id = f"inst_form_{id(render_live_institution_form)}"
        if hasattr(ctx, '_rendered_forms'):
            if form_id in ctx._rendered_forms:
                return  # Already rendered, skip
        else:
            ctx._rendered_forms = set()
        ctx._rendered_forms.add(form_id)
    
    # Initialize session state
    if 'duplicate_override' not in st.session_state:
        st.session_state['duplicate_override'] = False
    
    # Load existing institutions
    with st.spinner("Loading institution database..."):
        existing_institutions = load_institution_names()
    
    institution_service = InstitutionService()
    
    # Simple text input with callback that forces rerun
    institution_name = st.text_input(
        "Type institution name",
        value="",
        placeholder="Start typing institution name...",
        help="As you type, matching institutions will appear below. Press Tab or click away to see suggestions.",
        label_visibility="collapsed",
        key="institution_name_input",
        on_change=None  # Streamlit will rerun on blur (when you tab/click away)
    )
    
    # Show live autocomplete results as user types
    if institution_name and len(institution_name) >= 1:
        normalized_input = institution_name.lower()
        
        # Filter: prioritize starts with, then contains
        starts_with = existing_institutions[
            existing_institutions['institution_cpi'].str.lower().str.startswith(normalized_input, na=False)
        ]
        
        contains = existing_institutions[
            existing_institutions['institution_cpi'].str.lower().str.contains(normalized_input, na=False, regex=False) &
            ~existing_institutions['institution_cpi'].str.lower().str.startswith(normalized_input, na=False)
        ]
        
        matches = pd.concat([starts_with, contains]).head(10)
        
        if not matches.empty:
            st.info(f"Found {len(matches)} matching institutions")
            
            for idx, row in matches.iterrows():
                inst_name = row['institution_cpi']
                inst_type = row.get('institution_type_layer1', 'N/A')
                country = row.get('country_sub', 'N/A')
                
                col1, col2 = st.columns([4, 1])
                
                with col1:
                    st.text(f"{inst_name} ({inst_type}, {country})")
                
                with col2:
                    if st.button("Select", key=f"sel_{idx}_{hash(inst_name)}"):
                        st.error(f"**Institution already added!** '{inst_name}' is already in the database.")
                        st.stop()
            
            st.markdown("---")
    
    # Duplicate checking
    duplicate_found = False
    
    if institution_name and len(institution_name) >= 3 and not st.session_state.get('duplicate_override'):
        
        # Check exact duplicate
        exact_match = check_exact_duplicate(institution_name, existing_institutions)
        if exact_match:
            st.error("**Institution already added!**")
            st.write(f"'{exact_match}' is already in the database.")
            duplicate_found = True
        
        # Check suffix variants
        if not duplicate_found:
            suffix_matches = check_suffix_variants(institution_name, existing_institutions)
            if suffix_matches:
                st.warning("**Possible duplicate found!**")
                for match in suffix_matches:
                    st.write(f"**{match}** already added. Is this the same company?")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Yes, same company", key="same_co"):
                        st.info(f"Please use: **{suffix_matches[0]}**")
                        st.stop()
                with col2:
                    if st.button("No, different company", key="diff_co"):
                        st.session_state['duplicate_override'] = True
                        st.rerun()
                
                duplicate_found = True
        
        # Check fuzzy matches
        if not duplicate_found:
            fuzzy_matches = check_fuzzy_matches(institution_name, existing_institutions, threshold=85)
            if fuzzy_matches:
                st.info("**Similar institutions found:**")
                st.write("Are any of these the same company?")
                
                for match_name, score in fuzzy_matches:
                    col1, col2, col3 = st.columns([3, 1, 1])
                    
                    with col1:
                        st.write(f"**{match_name}** ({score}% match)")
                    
                    with col2:
                        if st.button("Same", key=f"sm_{hash(match_name)}"):
                            st.info(f"Please use: **{match_name}**")
                            st.stop()
                    
                    with col3:
                        if st.button("Different", key=f"df_{hash(match_name)}"):
                            st.session_state['duplicate_override'] = True
                            st.rerun()
                
                duplicate_found = True
    
    if duplicate_found:
        return
    
    # Full form
    st.markdown("---")
    
    # Get suggestions
    col1, col2 = st.columns([3, 1])
    with col2:
        if institution_name and len(institution_name) >= 3:
            if st.button("Get Suggestions", use_container_width=True, key="get_sugg"):
                with st.spinner("Analyzing..."):
                    result = institution_service.get_institution_suggestions(institution_name)
                    st.session_state['suggestions'] = result['suggestions']
                    st.session_state['research_links'] = result['research_links']
    
    suggestions = st.session_state.get('suggestions', {})
    research_links = st.session_state.get('research_links', [])
    
    if research_links:
        with st.expander("Research Links"):
            cols = st.columns(len(research_links))
            for idx, link in enumerate(research_links):
                with cols[idx]:
                    st.markdown(f"[{link['title']}]({link['url']})")
    
    st.subheader("Institution Details")
    
    col1, col2 = st.columns(2)
    
    with col1:
        existing_types = load_institution_types()  # Use cached version
        
        institution_type_layer1 = st.selectbox(
            "Institution Type - Layer 1",
            options=[''] + ['Public', 'Private'] + existing_types.get('layer1', []),
            help="Public or Private entity",
            key="type1"
        )
        
        institution_type_layer2 = st.selectbox(
            "Institution Type - Layer 2",
            options=[''] + ['Funds', 'Corporation', 'Commercial FI', 'Insurance'] + existing_types.get('layer2', []),
            help="General category",
            key="type2"
        )
        
        institution_type_layer3 = st.selectbox(
            "Institution Type - Layer 3",
            options=[''] + ['Asset Manager', 'Bank', 'Venture Capital Fund', 'Private Equity Fund', 
                           'Insurance Company', 'Corporation', 'Pension Fund'] + existing_types.get('layer3', []),
            help="Specific classification",
            key="type3"
        )
    
    with col2:
        countries_df = load_countries()  # Use cached version
        country_list = countries_df['country_cpi'].tolist() if not countries_df.empty else []
        
        country_sub = st.selectbox(
            "Primary Operating Country",
            options=[''] + sorted(country_list),
            key="country_sub"
        )
        
        country_parent = st.selectbox(
            "Headquarters Country",
            options=[''] + sorted(country_list),
            key="country_parent"
        )
        
        double_counting_risk = st.selectbox(
            "Double Counting Risk",
            options=['', 'Low', 'Medium', 'High'],
            key="dc_risk"
        )
    
    with st.expander("Additional Information"):
        contact_info = st.text_area("Contact Information", key="contact")
        comments = st.text_area("Comments", key="comments")
    
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.button("Add Institution", type="primary", use_container_width=True, key="add_inst"):
            if not institution_name:
                st.error("Please enter an institution name")
            elif not institution_type_layer1:
                st.error("Please select Institution Type - Layer 1")
            else:
                with st.spinner("Creating institution..."):
                    result = institution_service.create_institution(
                        institution_name=institution_name,
                        institution_type_layer1=institution_type_layer1 or None,
                        institution_type_layer2=institution_type_layer2 or None,
                        institution_type_layer3=institution_type_layer3 or None,
                        country_sub=country_sub or None,
                        country_parent=country_parent or None,
                        double_counting_risk=double_counting_risk or None,
                        contact_info=contact_info or None,
                        comments=comments or None,
                        user=st.session_state.get('username', 'unknown')
                    )
                    
                    if result['success']:
                        st.success(f"Institution '{result['institution_name']}' created successfully!")
                        load_institution_names.clear()
                        
                        if st.button("Add Another", key="add_another"):
                            st.session_state.clear()
                            st.rerun()
                    else:
                        st.error(result['message'])
    
    with col2:
        if st.button("Reset Form", use_container_width=True, key="reset"):
            st.session_state.clear()
            st.rerun()