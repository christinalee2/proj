import streamlit as st
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from functools import lru_cache
import os

from table_configs import get_available_tables, get_table_display_names, get_table_config
from database.cached_queries import preload_critical_data, clear_all_data_cache
from services.cached_services import clear_all_services
from services.nzft_matching import render_nzft_page, reset_nzft_session

load_dotenv()

def load_auth_config():
    from streamlit.runtime.secrets import secrets_singleton

    auth_secrets = {
        "auth": {
            "redirect_uri": os.getenv("REDIRECT_URI"),
            "cookie_secret": os.getenv("COOKIE_SECRET"),
            "oidc": {
                "client_id":     os.getenv("CLIENT_ID"),
                "client_secret": os.getenv("CLIENT_SECRET"),
                "server_metadata_url": os.getenv("SERVER_METADATA_URL"),
                "client_kwargs": {"scope": "openid"}
            }
        }
    }
    secrets_singleton._secrets = auth_secrets
    for k, v in auth_secrets.items():
        secrets_singleton._maybe_set_environment_variable(k, v)

load_auth_config()

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

# Handle authentication
query_params = st.query_params if hasattr(st, 'query_params') else {}
is_callback = 'code' in query_params or 'state' in query_params

if 'auth_attempted' not in st.session_state:
    st.session_state.auth_attempted = False

user_logged_in = False
user = None

try:
    if hasattr(st, 'user') and st.user and hasattr(st.user, 'is_logged_in'):
        user_logged_in = st.user.is_logged_in
        if user_logged_in:
            user = st.user
except:
    user_logged_in = False

if not user_logged_in:
    if is_callback:
        st.info("Processing authentication...")
        st.stop()
    elif not st.session_state.auth_attempted:
        st.info("Please log in to access this application")
        if st.button("Login with OIDC", type="primary"):
            st.session_state.auth_attempted = True
            st.login("oidc")
            st.stop()  
        st.stop()
    else:
        st.error("Authentication failed or still in progress")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Try Again", type="primary"):
                st.session_state.auth_attempted = False
                st.rerun()
        with col2:
            if st.button("Login Again", type="secondary"):
                st.login("oidc")
                st.stop()
        st.stop()

user = st.user 

def initialize_app():
    """Simple app initialization"""
    # Initialize basic session state
    if 'app_ready' not in st.session_state:
        # Cache user info once
        if 'user_info' not in st.session_state:
            user_email = user.get('email', 'No email available')
            authenticated_username = (
                user.get('email', '').split('@')[0] if user.get('email') 
                else user.get('preferred_username', user.get('name', 'authenticated_user'))
            )
            st.session_state.user_info = {
                'email': user_email,
                'username': authenticated_username
            }
        
        # Set default page
        if 'current_page' not in st.session_state:
            st.session_state.current_page = 'Upload New Data'
        
        # Preload critical data ONCE
        with st.spinner("Loading application data..."):
            preload_critical_data()
        
        st.session_state.app_ready = True

def render_sidebar():
    """Simple sidebar without problematic callbacks"""
    with st.sidebar:
        st.title("Reference Data Manager")
        st.markdown("---")
        
        st.subheader("Authentication")
        st.success(f"Logged in as: **{st.session_state.user_info['email']}**")
        
        st.markdown("---")
        
        st.subheader("Current User")
        st.text_input(
            "Active User",
            value=st.session_state.user_info['username'],
            disabled=True,
            help="Authenticated user from Cognito"
        )
        
        st.markdown("---")
        
        # Simple navigation without callbacks
        st.subheader("Menu")
        page_options = ["Upload New Data", "View Current Tables", "NZFT"]
        
        # Use simple selectbox instead of radio with callbacks
        current_page = st.selectbox(
            "Select Page:",
            page_options,
            index=page_options.index(st.session_state.current_page),
            key="page_selector"
        )
        
        # Update session state directly
        if current_page != st.session_state.current_page:
            st.session_state.current_page = current_page
            st.rerun()
        
        st.markdown("---")
        
        # Simple cache controls
        st.subheader("Cache Management")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Clear Data Cache", use_container_width=True):
                clear_all_data_cache()
                clear_all_services()
                st.session_state.app_ready = False
                st.success("Cache cleared")
                st.rerun()
        
        with col2:
            if st.button("Restart App", use_container_width=True):
                # Keep auth info, clear everything else
                keys_to_keep = ['auth_attempted', 'user_info']
                keys_to_remove = [k for k in st.session_state.keys() if k not in keys_to_keep]
                for key in keys_to_remove:
                    del st.session_state[key]
                st.cache_data.clear()
                st.cache_resource.clear()
                st.success("App restarted")
                st.rerun()
        
        if st.session_state.current_page == 'NZFT':
            st.markdown("---")
            if st.button("Reset NZFT Session", use_container_width=True):
                reset_nzft_session()
                st.success("NZFT session reset")
        
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d')}")
    
    return st.session_state.current_page

def render_table_selection():
    """Simple table selection"""
    st.subheader("1. Table Selection")
    
    available_tables = get_available_tables()
    table_display_names = get_table_display_names()
    display_options = [f"{table_display_names[table]} ({table})" for table in available_tables]
    
    if 'selected_table_display' not in st.session_state:
        st.session_state.selected_table_display = display_options[0]
    
    try:
        current_index = display_options.index(st.session_state.selected_table_display)
    except ValueError:
        current_index = 0
        st.session_state.selected_table_display = display_options[0]
    
    selected_display = st.selectbox(
        "Which table do you want to upload to?",
        options=display_options,
        index=current_index,
        key="upload_table_selector"
    )
    
    selected_table = selected_display.split(' (')[-1].rstrip(')')
    st.session_state.selected_table = selected_table
    st.session_state.selected_table_display = selected_display
    
    return selected_table

def render_upload_method_selection(selected_table):
    """Simple upload method selection"""
    st.subheader("2. Upload Method")
    upload_method = st.radio(
        "How would you like to upload data?",
        options=["Single Entry Form", "Bulk Upload (CSV or Excel)"],
        horizontal=True,
        key=f"upload_method_{selected_table}"
    )
    return upload_method

def render_upload_page():
    """Simple upload page"""
    st.header("Upload New Data")
    st.markdown("This tool lets you add new data to the CPI reference tables.")
    st.markdown("---")
    
    selected_table = render_table_selection()
    config = get_table_config(selected_table)
    if config and hasattr(config, 'general_description') and config.general_description:
        st.markdown(f"{config.general_description}")
    
    st.markdown("---")
    upload_method = render_upload_method_selection(selected_table)
    st.markdown("---")
    
    if upload_method == "Single Entry Form":
        from ui.unified_table_forms import render_unified_single_entry_form
        render_unified_single_entry_form(selected_table)
    else:
        from ui.unified_table_forms import render_unified_bulk_upload
        render_unified_bulk_upload(selected_table)

def render_table_view(table_name: str):
    """Simple table view"""
    config = get_table_config(table_name)
    if not config:
        st.error(f"No configuration found for table: {table_name}")
        return
    
    st.header(f"View {config.display_name} Data")
    st.markdown(config.description)
    st.markdown("---")
    
    col1, col2 = st.columns([4, 1])
    with col2:
        if st.button("Refresh Data", help="Force reload from database", use_container_width=True):
            # Clear specific table cache
            for key in list(st.session_state.keys()):
                if f"table_{table_name}" in key:
                    del st.session_state[key]
            st.success("Data will reload fresh")
            st.rerun()
    
    st.markdown("---")
    
    with st.spinner(f"Loading {config.display_name} data..."):
        try:
            from database.cached_queries import get_table_data_cached
            df = get_table_data_cached(table_name, limit=None)
            
            if df.empty:
                st.info(f"No data found in {config.display_name} table")
                return
            
            st.info(f"Loaded {len(df):,} rows from {config.display_name}")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", f"{len(df):,}")
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                primary_field = config.required_fields[0] if config.required_fields else None
                if primary_field and primary_field in df.columns:
                    unique_count = df[primary_field].nunique()
                    st.metric(f"Unique {primary_field}", f"{unique_count:,}")
                else:
                    st.metric("Data Size", f"{len(df) * len(df.columns):,} cells")
            
            st.markdown("---")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                search_term = st.text_input("Search", placeholder="Type to filter results...")
            with col2:
                show_rows = st.number_input("Rows to display", min_value=10, max_value=1000, value=100, step=10)
            
            if search_term:
                mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                filtered_df = df[mask].head(show_rows)
                st.info(f"Found {len(df[mask])} matching rows (showing {len(filtered_df)})")
            else:
                filtered_df = df.head(show_rows)
            
            st.subheader(f"Data from {config.display_name} Table")
            st.dataframe(filtered_df, use_container_width=True, height=600, hide_index=False)
            
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=f"{table_name}_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")

def render_view_tables_page():
    """Simple view tables page"""
    st.header("View Current Tables")
    st.markdown("Browse your reference data")
    st.markdown("---")
    
    st.subheader("Select Table")
    
    available_tables = get_available_tables()
    table_display_names = get_table_display_names()
    display_options = [f"{table_display_names[table]} ({table})" for table in available_tables]
    
    try:
        current_index = available_tables.index(st.session_state.get('selected_table', 'institution'))
    except ValueError:
        current_index = 0
    
    selected_display = st.selectbox(
        "Which table would you like to view?",
        options=display_options,
        index=current_index,
        key="view_table_selector"
    )
    
    selected_table = selected_display.split(' (')[-1].rstrip(')')
    st.session_state.selected_table = selected_table

    config = get_table_config(selected_table)
    if config and hasattr(config, 'general_description') and config.general_description:
        st.markdown(f"{config.general_description}")
    
    st.markdown("---")
    render_table_view(selected_table)

def main():
    """Simple main function"""
    # Initialize app once
    initialize_app()
    
    # Render sidebar and get current page
    current_page = render_sidebar()
    
    # Render current page
    if current_page == "Upload New Data":
        render_upload_page()
    elif current_page == "View Current Tables":
        render_view_tables_page()
    elif current_page == "NZFT":
        render_nzft_page()

if __name__ == "__main__":
    main()