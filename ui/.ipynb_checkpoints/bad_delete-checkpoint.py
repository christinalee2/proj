import streamlit as st
from typing import Optional
from services.institution_service import InstitutionService
from ui.components import (
    show_validation_results,
    show_suggestions_panel,
    show_research_links,
    show_success_message,
    show_error_message
)


def render_institution_form():
    """Render the single institution entry form"""
    
    st.header("Add New Institution")
    st.markdown("Enter institution details below. The system will check for duplicates and suggest metadata automatically.")
    
    # Initialize service
    institution_service = InstitutionService()
    
    # Institution name input with live search
    institution_name = st.text_input(
        "Institution Name *",
        key="inst_name",
        help="Enter the full institution name. Accents will be automatically removed.",
        placeholder="e.g., BlackRock Inc."
    )
    
    # Live search for similar names
    if institution_name and len(institution_name) > 2:
        with st.spinner("Checking for existing institutions..."):
            search_results = institution_service.search_institutions(institution_name, limit=10)
            
            if not search_results.empty:
                with st.expander(f"📋 Found {len(search_results)} institutions starting with '{institution_name[:10]}...'"):
                    st.dataframe(search_results, use_container_width=True)
    
    # Divider
    st.markdown("---")
    
    # Get suggestions if name is entered
    suggestions = None
    research_links = None
    
    if institution_name and len(institution_name) > 2:
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🔍 Get Suggestions", use_container_width=True):
                with st.spinner("Analyzing institution..."):
                    result = institution_service.get_institution_suggestions(institution_name)
                    suggestions = result['suggestions']
                    research_links = result['research_links']
                    st.session_state['suggestions'] = suggestions
                    st.session_state['research_links'] = research_links
    
    # Retrieve from session state if available
    if 'suggestions' in st.session_state:
        suggestions = st.session_state['suggestions']
        research_links = st.session_state['research_links']
    
    # Show suggestions
    if suggestions:
        show_suggestions_panel(suggestions)
        st.markdown("---")
    
    # Show research links
    if research_links:
        show_research_links(research_links)
        st.markdown("---")
    
    # Main form fields
    st.subheader("Institution Details")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Get existing types for dropdowns
        existing_types = institution_service.query_service.get_institution_types()
        
        institution_type_layer1 = st.selectbox(
            "Institution Type - Layer 1 *",
            options=[''] + ['Public', 'Private'] + existing_types.get('layer1', []),
            index=0 if not suggestions else (
                ['', 'Public', 'Private'].index(suggestions['suggestions']['institution_type_layer1'])
                if suggestions['suggestions'].get('institution_type_layer1') in ['Public', 'Private']
                else 0
            ),
            help="Public or Private entity"
        )
        
        institution_type_layer2 = st.selectbox(
            "Institution Type - Layer 2",
            options=[''] + ['Funds', 'Corporation', 'Commercial FI', 'Insurance'] + existing_types.get('layer2', []),
            help="General category (e.g., Funds, Corporation, Commercial FI)"
        )
        
        institution_type_layer3 = st.selectbox(
            "Institution Type - Layer 3",
            options=[''] + ['Asset Manager', 'Bank', 'Venture Capital Fund', 'Private Equity Fund', 
                           'Insurance Company', 'Corporation', 'Pension Fund'] + existing_types.get('layer3', []),
            help="Specific classification (e.g., Asset Manager, Bank, Venture Capital Fund)"
        )
    
    with col2:
        # Get countries for dropdowns
        countries_df = institution_service.query_service.get_countries()
        country_list = countries_df['country_cpi'].tolist() if not countries_df.empty else []
        
        country_sub = st.selectbox(
            "Primary Operating Country",
            options=[''] + sorted(country_list),
            help="Country where the institution primarily operates"
        )
        
        country_parent = st.selectbox(
            "Headquarters Country",
            options=[''] + sorted(country_list),
            help="Country where the institution's headquarters is located"
        )
        
        double_counting_risk = st.selectbox(
            "Double Counting Risk",
            options=['', 'Low', 'Medium', 'High'],
            help="Risk of double counting in climate finance tracking"
        )
    
    # Additional fields
    with st.expander("Additional Information (Optional)"):
        contact_info = st.text_area(
            "Contact Information",
            help="Email, phone, or other contact details"
        )
        
        comments = st.text_area(
            "Comments",
            help="Any additional notes or comments"
        )
    
    # Submit button
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        submit_button = st.button("✅ Add Institution", type="primary", use_container_width=True)
    
    with col2:
        if st.button("🔄 Reset Form", use_container_width=True):
            st.session_state.clear()
            st.rerun()
    
    # Handle form submission
    if submit_button:
        if not institution_name:
            show_error_message("Please enter an institution name")
        elif not institution_type_layer1:
            show_error_message("Please select Institution Type - Layer 1 (Public/Private)")
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
                    show_success_message(
                        f"✅ Institution '{result['institution_name']}' created successfully!",
                        details={
                            'Institution ID': result['institution_id'],
                            'Institution Name': result['institution_name']
                        }
                    )
                    
                    # Clear form
                    if st.button("Add Another Institution"):
                        st.session_state.clear()
                        st.rerun()
                else:
                    show_error_message(result['message'])
                    
                    if result.get('validation'):
                        show_validation_results(result['validation'])