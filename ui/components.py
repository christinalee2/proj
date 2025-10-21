"""
Reusable UI components for the Streamlit app
"""
import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional


def show_validation_results(validation: Dict[str, Any]):
    """
    Display validation results with appropriate styling
    
    Args:
        validation: Validation result dictionary
    """
    if validation['has_exact_duplicate']:
        st.error("**Duplicate Found**")
        st.write(f"This institution already exists in the database:")
        st.json(validation['exact_match'])
    elif validation['has_fuzzy_duplicate']:
        st.warning("**Similar Institutions Found**")
        st.write("The following similar institutions were found:")
        for match in validation['fuzzy_matches']:
            st.write(f"- **{match['name']}** (Match: {match['score']}%)")
    
    if validation['warnings']:
        for warning in validation['warnings']:
            st.warning(warning)
    
    if validation['errors']:
        for error in validation['errors']:
            st.error(error)
    
    if validation['is_valid'] and not validation['has_exact_duplicate']:
        st.success("Ready to insert")


def show_suggestions_panel(suggestions: Dict[str, Any]):
    """
    Display enrichment suggestions in an organized panel
    
    Args:
        suggestions: Dictionary with suggested metadata
    """
    st.subheader("Suggested Metadata")
    
    confidence = suggestions.get('confidence', 'low')
    if confidence == 'high':
        st.success("High confidence suggestions (from AI analysis)")
    elif confidence == 'medium':
        st.info("Medium confidence suggestions (from keyword matching)")
    else:
        st.warning("Low confidence suggestions")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if suggestions.get('institution_type_layer1'):
            st.write(f"**Type (Layer 1):** {suggestions['institution_type_layer1']}")
        if suggestions.get('institution_type_layer2'):
            st.write(f"**Type (Layer 2):** {suggestions['institution_type_layer2']}")
        if suggestions.get('institution_type_layer3'):
            st.write(f"**Type (Layer 3):** {suggestions['institution_type_layer3']}")
    
    with col2:
        if suggestions.get('country_sub'):
            st.write(f"**Primary Country:** {suggestions['country_sub']}")
        if suggestions.get('country_parent'):
            st.write(f"**HQ Country:** {suggestions['country_parent']}")
    
    if suggestions.get('sources'):
        with st.expander("Information Sources"):
            for source in suggestions['sources']:
                st.write(f"- {source}")


def show_research_links(links: List[Dict[str, str]]):
    """
    Display research links in a compact format
    
    Args:
        links: List of link dictionaries with 'title' and 'url'
    """
    st.subheader("Research Links")
    cols = st.columns(len(links))
    
    for idx, link in enumerate(links):
        with cols[idx]:
            st.markdown(f"[{link['title']}]({link['url']})", unsafe_allow_html=True)


def show_bulk_upload_preview(df: pd.DataFrame, validation_df: Optional[pd.DataFrame] = None):
    """
    Display a preview of bulk upload data with validation status
    
    Args:
        df: Original DataFrame
        validation_df: DataFrame with validation results
    """
    st.subheader("Upload Preview")
    
    if validation_df is not None:
        # Show summary statistics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total = len(validation_df)
            st.metric("Total Rows", total)
        
        with col2:
            valid = len(validation_df[validation_df['status'] == 'OK'])
            st.metric("Valid", valid, delta=None if valid == 0 else "✓")
        
        with col3:
            warnings = len(validation_df[validation_df['status'] == 'WARNING'])
            st.metric("Warnings", warnings, delta=None if warnings == 0 else "⚠")
        
        with col4:
            errors = len(validation_df[validation_df['status'] == 'ERROR']) + \
                    len(validation_df[validation_df['status'] == 'DUPLICATE'])
            st.metric("Errors", errors, delta=None if errors == 0 else "✗")
        
        # Show detailed table with color coding
        def highlight_status(row):
            if row['status'] == 'OK':
                return ['background-color: #d4edda'] * len(row)
            elif row['status'] == 'WARNING':
                return ['background-color: #fff3cd'] * len(row)
            elif row['status'] in ['ERROR', 'DUPLICATE']:
                return ['background-color: #f8d7da'] * len(row)
            return [''] * len(row)
        
        styled_df = validation_df.style.apply(highlight_status, axis=1)
        st.dataframe(styled_df, use_container_width=True, height=400)
    else:
        st.dataframe(df, use_container_width=True, height=400)


def show_audit_history(audit_df: pd.DataFrame):
    """
    Display audit history in a formatted table
    
    Args:
        audit_df: DataFrame with audit log entries
    """
    if audit_df.empty:
        st.info("No audit history available")
        return
    
    st.subheader("📝 Change History")
    
    # Format the DataFrame for display
    display_df = audit_df[[
        'changed_at', 'operation', 'table_name', 
        'field_name', 'old_value', 'new_value', 'changed_by'
    ]].copy()
    
    display_df['changed_at'] = pd.to_datetime(display_df['changed_at']).dt.strftime('%Y-%m-%d %H:%M')
    
    st.dataframe(display_df, use_container_width=True, height=400)


def create_autocomplete_input(
    label: str,
    options: List[str],
    key: str,
    help_text: Optional[str] = None
) -> str:
    """
    Create an input with autocomplete functionality
    
    Args:
        label: Label for the input
        options: List of options for autocomplete
        key: Unique key for the widget
        help_text: Optional help text
        
    Returns:
        Selected or typed value
    """
    # Use selectbox with option to type new value
    choice = st.radio(
        f"{label} - Input Method",
        ["Select from existing", "Enter new"],
        key=f"{key}_method",
        horizontal=True,
        help=help_text
    )
    
    if choice == "Select from existing":
        value = st.selectbox(
            label,
            options=[''] + sorted(options),
            key=f"{key}_select"
        )
    else:
        value = st.text_input(
            label,
            key=f"{key}_text",
            help=help_text
        )
    
    return value


def show_success_message(message: str, details: Optional[Dict[str, Any]] = None):
    """
    Show a success message with optional details
    
    Args:
        message: Success message
        details: Optional dictionary with additional details
    """
    st.success(message)
    
    if details:
        with st.expander("View Details"):
            st.json(details)


def show_error_message(message: str, error: Optional[Exception] = None):
    """
    Show an error message with optional exception details
    
    Args:
        message: Error message
        error: Optional exception object
    """
    st.error(message)
    
    if error:
        with st.expander("Error Details"):
            st.code(str(error))


def create_confirmation_dialog(message: str, key: str) -> bool:
    """
    Create a confirmation dialog
    
    Args:
        message: Confirmation message
        key: Unique key for the widget
        
    Returns:
        True if confirmed, False otherwise
    """
    st.warning(message)
    col1, col2 = st.columns(2)
    
    with col1:
        confirm = st.button("✓ Confirm", key=f"{key}_confirm", type="primary")
    with col2:
        cancel = st.button("✗ Cancel", key=f"{key}_cancel")
    
    return confirm