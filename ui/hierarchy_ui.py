import streamlit as st
import pandas as pd
from typing import List, Tuple, Optional, Dict, Any
from services.hierarchy_service import HierarchyService
from database.cached_queries import get_table_data_cached


def render_institution_search_widget(
    key: str, 
    label: str, 
    existing_institutions: pd.DataFrame,
    help_text: str = None,
    placeholder: str = "Start typing to search institutions..."
) -> Tuple[Optional[str], Optional[str]]:
    """
    Render search to find institutions by exact/fuzzy match
    
    Args:
        key: Unique key for the widget
        label: Display label
        existing_institutions: df of existing institutions
        help_text: Optional help text
        placeholder: Placeholder text
        
    Returns:
        Tuple of (selected_institution_name, selected_institution_id)
    """
    hierarchy_service = HierarchyService()
    
    # Initialize session state for search
    search_key = f"{key}_search"
    results_key = f"{key}_results"
    selected_key = f"{key}_selected"
    
    if search_key not in st.session_state:
        st.session_state[search_key] = ""
    if results_key not in st.session_state:
        st.session_state[results_key] = []
    if selected_key not in st.session_state:
        st.session_state[selected_key] = None
    
    # Search input
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input(
            label,
            value=st.session_state[search_key],
            placeholder=placeholder,
            help=help_text,
            key=f"{key}_input"
        )
    
    with col2:
        clear_search = st.button("Clear", key=f"{key}_clear", type="secondary")
        if clear_search:
            st.session_state[search_key] = ""
            st.session_state[results_key] = []
            st.session_state[selected_key] = None
            st.rerun()
    
    # Update search term in session state
    if search_term != st.session_state[search_key]:
        st.session_state[search_key] = search_term
        
        if len(search_term) >= 2:
            # Perform search
            search_results = hierarchy_service.search_institution_for_hierarchy(
                search_term, existing_institutions, limit=10
            )
            st.session_state[results_key] = search_results
        else:
            st.session_state[results_key] = []
    
    # Display search results
    if st.session_state[results_key]:
        st.write("Search Results:")
        for i, (name, inst_id, score) in enumerate(st.session_state[results_key]):
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                st.write(f"**{name}**")
                if score < 100:
                    st.caption(f"Match: {score:.1f}%")
            
            with col2:
                if st.button("Select", key=f"{key}_select_{i}"):
                    st.session_state[selected_key] = {'name': name, 'id': inst_id}
                    st.session_state[search_key] = name
                    st.session_state[results_key] = []
                    st.rerun()
    
    # Show selected institution
    if st.session_state[selected_key]:
        selected = st.session_state[selected_key]
        st.success(f"Selected: {selected['name']} (ID: {selected['id']})")
        return selected['name'], selected['id']
    
    return None, None


def render_hierarchy_form(
    institution_name: str,
    institution_id: str,
    relationship_type: str,
    existing_institutions: pd.DataFrame,
    form_key: str
) -> Optional[Dict[str, Any]]:
    """
    Render hierarchy relationship form
    
    Args:
        institution_name: Name of the known institution
        institution_id: ID of the known institution
        relationship_type: Either "parent" or "child"
        existing_institutions: DataFrame of existing institutions
        form_key: Unique form key
        
    Returns:
        Dictionary with hierarchy data if form is submitted, None otherwise
    """
    hierarchy_service = HierarchyService()
    
    st.subheader(f"Add Hierarchy Relationship")
    
    if relationship_type == "parent":
        st.info(f"**{institution_name}** will be the PARENT institution")
        search_label = "Select Child Institution"
        search_help = "Institution that will be owned/controlled by the parent"
        parent_name, parent_id = institution_name, institution_id
        child_name, child_id = render_institution_search_widget(
            key=f"{form_key}_child",
            label=search_label,
            existing_institutions=existing_institutions,
            help_text=search_help
        )
    else:
        st.info(f"**{institution_name}** will be the CHILD institution")
        search_label = "Select Parent Institution"
        search_help = "Institution that owns/controls the child"
        parent_name, parent_id = render_institution_search_widget(
            key=f"{form_key}_parent",
            label=search_label,
            existing_institutions=existing_institutions,
            help_text=search_help
        )
        child_name, child_id = institution_name, institution_id
    
    # Additional form fields
    col1, col2 = st.columns(2)
    
    with col1:
        percent_ownership = st.number_input(
            "Ownership Percentage",
            min_value=0.0,
            max_value=1.0,
            value=1.0,
            step=0.01,
            format="%.2f",
            help="Enter as decimal (e.g., 0.51 for 51%)",
            key=f"{form_key}_ownership"
        )
    
    with col2:
        is_controlling = st.checkbox(
            "Is Controlling Institution",
            value=percent_ownership > 0.5,
            help="Check if ownership percentage > 50%",
            key=f"{form_key}_controlling"
        )
    
    relationship_type_text = st.text_input(
        "Relationship Type",
        placeholder="e.g., subsidiary, division, branch",
        help="Describe the type of relationship",
        key=f"{form_key}_rel_type"
    )
    
    # Validation and submission
    can_submit = parent_name and child_name and parent_id and child_id
    
    if not can_submit:
        if relationship_type == "parent":
            st.warning("Please select a child institution to continue")
        else:
            st.warning("Please select a parent institution to continue")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Add Relationship", disabled=not can_submit, key=f"{form_key}_submit"):
            # Validate the entry
            validation = hierarchy_service.validate_hierarchy_entry(
                parent_name, child_name, percent_ownership, existing_institutions
            )
            
            if validation['is_valid']:
                return {
                    'parent_institution': parent_name,
                    'child_institution': child_name,
                    'percent_ownership': percent_ownership,
                    'is_controlling_institution': is_controlling,
                    'relationship_type': relationship_type_text,
                    'validation': validation
                }
            else:
                for error in validation['errors']:
                    st.error(error)
                for warning in validation['warnings']:
                    st.warning(warning)
    
    with col2:
        if st.button("Cancel", key=f"{form_key}_cancel"):
            return {"cancel": True}
    
    return None


def render_hierarchy_options_for_duplicates(
    institution_name: str,
    duplicate_name: str,
    existing_institutions: pd.DataFrame,
    form_key: str
) -> Optional[Dict[str, Any]]:
    """
    Render hierarchy options when there's a duplicate institution
    
    Args:
        institution_name: Name of institution user tried to enter
        duplicate_name: Name of existing institution that matches
        existing_institutions: DataFrame of existing institutions
        form_key: Unique form key
        
    Returns:
        Dictionary with hierarchy data if form is submitted, None otherwise
    """
    st.info("💡 Since this institution already exists, you can create a hierarchy relationship:")
    
    existing_inst = existing_institutions[
        existing_institutions['institution_cpi'].str.lower() == duplicate_name.lower()
    ]
    
    if existing_inst.empty:
        st.error("Could not find existing institution details")
        return None
    
    existing_id = str(existing_inst.iloc[0].get('id_institution_cpi', ''))
    
    relationship_choice = st.radio(
        "What type of relationship do you want to create?",
        ["Parent Institution", "Child Institution"],
        key=f"{form_key}_relationship_choice",
        help="Choose whether the existing institution should be parent or child"
    )
    
    if relationship_choice == "Parent Institution":
        # Existing institution is parent, user's input becomes child
        return render_hierarchy_form(
            institution_name=duplicate_name,
            institution_id=existing_id,
            relationship_type="parent",
            existing_institutions=existing_institutions,
            form_key=f"{form_key}_as_parent"
        )
    else:
        # Existing institution is child, user's input becomes parent  
        return render_hierarchy_form(
            institution_name=duplicate_name,
            institution_id=existing_id,
            relationship_type="child",
            existing_institutions=existing_institutions,
            form_key=f"{form_key}_as_child"
        )


def render_hierarchy_options_for_fuzzy_matches(
    institution_name: str,
    matched_name: str,
    existing_institutions: pd.DataFrame,
    form_key: str
) -> Optional[Dict[str, Any]]:
    """
    Render hierarchy options when there's a fuzzy match
    
    Args:
        institution_name: Name of institution user tried to enter
        matched_name: Name of fuzzy matched institution
        existing_institutions: DataFrame of existing institutions
        form_key: Unique form key
        
    Returns:
        Dictionary with hierarchy data if form is submitted, None otherwise
    """
    st.info("💡 You can also create a hierarchy relationship with the matched institution:")
    
    # Find the matched institution ID
    matched_inst = existing_institutions[
        existing_institutions['institution_cpi'].str.lower() == matched_name.lower()
    ]
    
    if matched_inst.empty:
        st.error("Could not find matched institution details")
        return None
    
    matched_id = str(matched_inst.iloc[0].get('id_institution_cpi', ''))
    
    # Radio button for relationship type
    relationship_choice = st.radio(
        "What type of relationship do you want to create?",
        ["Parent Institution", "Child Institution"],
        key=f"{form_key}_fuzzy_relationship_choice",
        help="Choose whether the matched institution should be parent or child"
    )
    
    if relationship_choice == "Parent Institution":
        # Matched institution is parent
        return render_hierarchy_form(
            institution_name=matched_name,
            institution_id=matched_id,
            relationship_type="parent",
            existing_institutions=existing_institutions,
            form_key=f"{form_key}_fuzzy_as_parent"
        )
    else:
        # Matched institution is child
        return render_hierarchy_form(
            institution_name=matched_name,
            institution_id=matched_id,
            relationship_type="child",
            existing_institutions=existing_institutions,
            form_key=f"{form_key}_fuzzy_as_child"
        )


def render_new_institution_hierarchy_option(
    institution_name: str,
    existing_institutions: pd.DataFrame,
    form_key: str
) -> Optional[Dict[str, Any]]:
    """
    Render optional hierarchy form for completely new institutions
    
    Args:
        institution_name: Name of the new institution
        existing_institutions: DataFrame of existing institutions
        form_key: Unique form key
        
    Returns:
        Dictionary with hierarchy data if form is submitted, None otherwise
    """
    # Expandable section for hierarchy
    with st.expander("🔗 Add Hierarchy Relationship (Optional)", expanded=False):
        st.write("Create a parent-child relationship for this new institution")
        
        relationship_choice = st.radio(
            "How should this new institution be related?",
            ["As Parent Institution", "As Child Institution", "No Relationship"],
            key=f"{form_key}_new_relationship_choice",
            help="Choose the role of this new institution in the hierarchy"
        )
        
        if relationship_choice == "No Relationship":
            return None
        elif relationship_choice == "As Parent Institution":
            # New institution will be parent
            st.write(f"**{institution_name}** will be the PARENT institution")
            
            child_name, child_id = render_institution_search_widget(
                key=f"{form_key}_new_child",
                label="Select Child Institution",
                existing_institutions=existing_institutions,
                help_text="Institution that will be owned/controlled by this new parent"
            )
            
            if child_name and child_id:
                col1, col2 = st.columns(2)
                
                with col1:
                    percent_ownership = st.number_input(
                        "Ownership Percentage",
                        min_value=0.0,
                        max_value=1.0,
                        value=1.0,
                        step=0.01,
                        format="%.2f",
                        key=f"{form_key}_new_ownership"
                    )
                
                with col2:
                    is_controlling = st.checkbox(
                        "Is Controlling",
                        value=percent_ownership > 0.5,
                        key=f"{form_key}_new_controlling"
                    )
                
                relationship_type_text = st.text_input(
                    "Relationship Type",
                    placeholder="e.g., subsidiary, division",
                    key=f"{form_key}_new_rel_type"
                )
                
                if st.button("Create Relationship", key=f"{form_key}_new_submit"):
                    return {
                        'parent_institution': institution_name,
                        'child_institution': child_name,
                        'child_id': child_id,
                        'percent_ownership': percent_ownership,
                        'is_controlling_institution': is_controlling,
                        'relationship_type': relationship_type_text,
                        'mode': 'new_as_parent'
                    }
        
        else:  # As Child Institution
            # New institution will be child
            st.write(f"**{institution_name}** will be the CHILD institution")
            
            # Search for parent institution
            parent_name, parent_id = render_institution_search_widget(
                key=f"{form_key}_new_parent",
                label="Select Parent Institution",
                existing_institutions=existing_institutions,
                help_text="Institution that owns/controls this new child"
            )
            
            if parent_name and parent_id:
                col1, col2 = st.columns(2)
                
                with col1:
                    percent_ownership = st.number_input(
                        "Ownership Percentage",
                        min_value=0.0,
                        max_value=1.0,
                        value=1.0,
                        step=0.01,
                        format="%.2f",
                        key=f"{form_key}_new_child_ownership"
                    )
                
                with col2:
                    is_controlling = st.checkbox(
                        "Is Controlling",
                        value=percent_ownership > 0.5,
                        key=f"{form_key}_new_child_controlling"
                    )
                
                relationship_type_text = st.text_input(
                    "Relationship Type",
                    placeholder="e.g., subsidiary, division",
                    key=f"{form_key}_new_child_rel_type"
                )
                
                if st.button("Create Relationship", key=f"{form_key}_new_child_submit"):
                    return {
                        'parent_institution': parent_name,
                        'parent_id': parent_id,
                        'child_institution': institution_name,
                        'percent_ownership': percent_ownership,
                        'is_controlling_institution': is_controlling,
                        'relationship_type': relationship_type_text,
                        'mode': 'new_as_child'
                    }
    
    return None