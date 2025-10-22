import streamlit as st
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from table_configs import get_available_tables, get_table_display_names, get_table_config

from database.cached_queries import get_table_data_cached
from services.nzft_matching import render_nzft_page, reset_nzft_session


load_dotenv()

st.set_page_config(
    page_title="Reference Data Management",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    .stButton>button {
        width: 100%;
    }
    h1 {
        color: #1f77b4;
    }
    .dataframe {
        font-size: 14px;
    }
    </style>
    """, unsafe_allow_html=True)


def initialize_session_state():
    """Initialize session state variables"""
    if 'current_page' not in st.session_state:
        st.session_state['current_page'] = 'Upload New Data'
    if 'username' not in st.session_state:
        st.session_state['username'] = 'analyst'
    if 'selected_table' not in st.session_state:
        st.session_state['selected_table'] = 'institution'


def render_sidebar():
    """Render the sidebar with navigation and controls"""
    with st.sidebar:
        st.title("Reference Data Manager")
        st.markdown("---")
        
        st.subheader("User")
        username = st.text_input(
            "Username",
            value=st.session_state.get('username', 'analyst'),
            key="username_input"
        )
        st.session_state['username'] = username
        
        st.markdown("---")
        
        st.subheader("Menu")
        page = st.radio(
            "",
            [
                "Upload New Data",
                "View Current Tables",
                "NZFT"  
            ],
            key="navigation",
            index=0 if st.session_state['current_page'] == 'Upload New Data' 
                  else 1 if st.session_state['current_page'] == 'View Current Tables'
                  else 2  
        )
        
        st.session_state['current_page'] = page
        
        st.markdown("---")
        
        st.subheader("Cache Management")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Clear Data Cache", help="Clear cached table data", use_container_width=True):
                st.cache_data.clear()
                st.success("Data cache cleared!")
                st.rerun()
        
        with col2:
            if st.button("🔄 Clear All", help="Clear all caches", use_container_width=True):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.success("All caches cleared!")
                st.rerun()
        
        # Add NZFT reset button
        if st.session_state.get('current_page') == 'NZFT':
            st.markdown("---")
            if st.button("🔄 Reset NZFT Session", help="Clear NZFT matching data", use_container_width=True):
                reset_nzft_session()
                st.success("NZFT session cleared!")
                st.rerun()
        
        st.caption("Data cached for 2-4 hours")
        st.caption("Services cached indefinitely")
        
        st.markdown("---")
        
        st.caption(f"Version 2.0.0")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d')}")
    
    return st.session_state['current_page']


def render_upload_page():
    """Render the upload page with table selection and forms"""
    st.header("Upload New Data")
    st.markdown("Select a table and upload your data")
    st.markdown("---")
    
    # Table selection
    st.subheader("1. Select Table")
    
    available_tables = get_available_tables()
    table_display_names = get_table_display_names()
    
    display_options = [f"{table_display_names[table]} ({table})" for table in available_tables]
    
    selected_display = st.selectbox(
        "Which table do you want to upload to?",
        options=display_options,
        index=available_tables.index(st.session_state.get('selected_table', 'institution')),
        help="Choose the table where you want to add data",
        key="upload_table_selector"
    )
    
    selected_table = selected_display.split(' (')[-1].rstrip(')')
    st.session_state['selected_table'] = selected_table
    
    
    st.markdown("---")
    
    st.subheader("2. Upload Method")
    upload_method = st.radio(
        "How would you like to upload data?",
        options=["Single Entry Form", "Bulk Upload (CSV or Excel)"],
        horizontal=True,
        key=f"upload_method_{selected_table}"
    )
    
    st.markdown("---")
    
    if upload_method == "Single Entry Form":
        from ui.unified_table_forms import render_unified_single_entry_form
        render_unified_single_entry_form(selected_table)
    else:
        from ui.unified_table_forms import render_unified_bulk_upload
        render_unified_bulk_upload(selected_table)


def render_table_view(table_name: str):
    """
    Render table data view for any table
    
    Args:
        table_name: Name of the table
    """
    config = get_table_config(table_name)
    if not config:
        st.error(f"No configuration found for table: {table_name}")
        return
    
    st.header(f"View {config.display_name} Data")
    st.markdown(config.description)
    st.markdown("---")
    
    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown("")  
    with col2:
        if st.button("🔄 Refresh Data", help="Force reload from database", use_container_width=True):
            st.cache_data.clear()
            st.success("Cache cleared! Data will reload fresh.")
            st.rerun()
    
    st.markdown("---")
    
    with st.spinner(f"Loading {config.display_name} data..."):
        try:
            df = get_table_data_cached(table_name, limit=None)
            st.info(f"Loaded all {len(df):,} rows from {config.display_name}")
            
            if df.empty:
                st.info(f"No data found in {config.display_name} table")
                return
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", f"{len(df):,}")
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                # Show primary field stats if available
                primary_field = config.required_fields[0] if config.required_fields else None
                if primary_field and primary_field in df.columns:
                    unique_count = df[primary_field].nunique()
                    st.metric(f"Unique {primary_field}", f"{unique_count:,}")
                else:
                    st.metric("Data Size", f"{len(df) * len(df.columns):,} cells")
            
            st.markdown("---")
            
            col1, col2 = st.columns([3, 1])
            
            with col1:
                search_term = st.text_input(
                    "Search",
                    placeholder="Type to filter results...",
                    help="Search across all columns"
                )
            
            with col2:
                show_rows = st.number_input(
                    "Rows to display",
                    min_value=10,
                    max_value=1000,
                    value=100,
                    step=10
                )
            
            if search_term:
                mask = df.astype(str).apply(
                    lambda x: x.str.contains(search_term, case=False, na=False)
                ).any(axis=1)
                filtered_df = df[mask].head(show_rows)
                st.info(f"Found {len(df[mask])} matching rows (showing {len(filtered_df)})")
            else:
                filtered_df = df.head(show_rows)
            
            st.subheader(f"Data from {config.display_name} Table")
            st.dataframe(
                filtered_df,
                use_container_width=True,
                height=600,
                hide_index=False
            )
            
            # Export functionality
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 1, 1])
            
            with col2:
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"{table_name}_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            st.exception(e)


def render_view_tables_page():
    """Render the view tables page"""
    st.header("View Current Tables")
    st.markdown("Browse your reference data")
    st.markdown("---")
    
    st.subheader("Select Table")
    
    available_tables = get_available_tables()
    table_display_names = get_table_display_names()
    
    display_options = [f"{table_display_names[table]} ({table})" for table in available_tables]
    
    selected_display = st.selectbox(
        "Which table would you like to view?",
        options=display_options,
        index=available_tables.index(st.session_state.get('selected_table', 'institution')),
        help="Choose a table to view its contents",
        key="view_table_selector"
    )
    
    selected_table = selected_display.split(' (')[-1].rstrip(')')
    st.session_state['selected_table'] = selected_table
    
    st.markdown("---")
    
    render_table_view(selected_table)


def main():
    """Main application entry point"""
    
    # Initialize
    initialize_session_state()
    
    # Render sidebar and get selected page
    page = render_sidebar()
    
    # Render selected page
    if page == "Upload New Data":
        render_upload_page()
    elif page == "View Current Tables":
        render_view_tables_page()
    elif page == "NZFT":  # Add this condition
        render_nzft_page()


if __name__ == "__main__":
    main()

