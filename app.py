import streamlit as st
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

from table_configs import get_available_tables, get_table_display_names, get_table_config

from database.cached_queries import get_table_data_cached
from services.nzft_matching import render_nzft_page, reset_nzft_session


load_dotenv()


@st.cache_data(ttl=14400)
def get_table_configs_cached():
    """Caching table config loading"""
    return {
        'available_tables': get_available_tables(),
        'display_names': get_table_display_names()
    }

    
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



query_params = st.query_params if hasattr(st, 'query_params') else {}
is_callback = 'code' in query_params or 'state' in query_params

# Initialize authentication session state
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
            st.stop()  # Add this to prevent further execution
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

        
def initialize_session_state():
    """Initialize all session state at once to reduce overhead"""
    defaults = {
        'current_page': 'Upload New Data',
        'username': 'analyst',
        'selected_table': 'institution',
        'auth_attempted': False
    }
    
    # Batch update session state - only set if not already present
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value



def render_sidebar():
    """Render the sidebar with navigation and controls"""
    with st.sidebar:
        st.title("Reference Data Manager")
        st.markdown("---")
        
        # Cache user info in session state to avoid repeated user object access
        if 'user_info' not in st.session_state:
            user = st.user
            user_email = user.get('email', 'No email available')
            authenticated_username = (
                user.get('email', '').split('@')[0] if user.get('email') 
                else user.get('preferred_username', 
                user.get('name', 'authenticated_user'))
            )
            st.session_state.user_info = {
                'email': user_email,
                'username': authenticated_username
            }
        
        st.subheader("Authentication")
        st.success(f"Logged in as: **{st.session_state.user_info['email']}**")
        
        # Update session state with cached username
        st.session_state['username'] = st.session_state.user_info['username']
        
        st.markdown("---")
        
        st.subheader("Current User")
        st.text_input(
            "Active User",
            value=st.session_state.user_info['username'],
            disabled=True,
            help="Authenticated user from Cognito"
        )
        
        st.markdown("---")
        
        # Handle navigation directly without fragment for now
        st.subheader("Menu")
        
        page_options = ["Upload New Data", "View Current Tables", "NZFT"]
        
        if 'current_page' not in st.session_state:
            st.session_state['current_page'] = 'Upload New Data'
            
        try:
            current_index = page_options.index(st.session_state['current_page'])
        except ValueError:
            current_index = 0
            st.session_state['current_page'] = page_options[0]
        
        # Use on_change callback to update session state properly
        def update_page():
            st.session_state['current_page'] = st.session_state['navigation']
        
        page = st.radio(
            "",
            page_options,
            key="navigation",
            index=current_index,
            on_change=update_page  # This ensures proper state update
        )
        
        # Return the current page from session state
        current_page = st.session_state['current_page']
        
        st.markdown("---")
        
        # Call the cache controls fragment INSIDE the sidebar context
        render_cache_controls_fragment()
        
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d')}")
    
    return current_page

@st.fragment  
def render_cache_controls_fragment():
    """Fragment for cache controls - called inside sidebar context"""
    st.subheader("Cache Management")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Clear Data Cache", help="Clear cached table data", use_container_width=True):
            st.cache_data.clear()
            keys_to_clear = [key for key in st.session_state.keys() if key.endswith('_reference_data')]
            for key in keys_to_clear:
                del st.session_state[key]
            st.success("Data cache cleared!")
    
    with col2:
        if st.button("🔄 Clear All", help="Clear all caches", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            keys_to_clear = [key for key in st.session_state.keys() if key.endswith('_reference_data')]
            for key in keys_to_clear:
                del st.session_state[key]
            st.success("All caches cleared!")
    
    # Add NZFT reset button
    if st.session_state.get('current_page') == 'NZFT':
        st.markdown("---")
        if st.button("🔄 Reset NZFT Session", help="Clear NZFT matching data", use_container_width=True):
            from services.nzft_matching import reset_nzft_session
            reset_nzft_session()
            st.success("NZFT session reset!")
    
    st.caption("Data cached for 2-4 hours")
    st.caption("Services cached indefinitely")

    

@st.fragment
def render_table_selection():
    """Fragment table selection to avoid full rerun"""
    st.subheader("1. Select Table")
    
    configs = get_table_configs_cached()
    available_tables = configs['available_tables']
    table_display_names = configs['display_names']
    
    display_options = [f"{table_display_names[table]} ({table})" for table in available_tables]
    
    # Use session state to maintain selection
    if 'selected_table_display' not in st.session_state:
        default_table = st.session_state.get('selected_table', 'institution')
        try:
            default_index = available_tables.index(default_table)
        except ValueError:
            default_index = 0
        st.session_state.selected_table_display = display_options[default_index]
    
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
    st.session_state['selected_table'] = selected_table
    st.session_state.selected_table_display = selected_display
    
    return selected_table

@st.fragment
def render_upload_method_selection(selected_table):
    """Fragment upload method selection"""
    st.subheader("2. Upload Method")
    upload_method = st.radio(
        "How would you like to upload data?",
        options=["Single Entry Form", "Bulk Upload (CSV or Excel)"],
        horizontal=True,
        key=f"upload_method_{selected_table}"
    )
    return upload_method




def render_upload_page():
    """Render the upload page with fragments"""
    st.header("Upload New Data")
    st.markdown("This tool lets you add new data to the CPI reference tables. You can upload a single entry or import multiple records at once, depending on your needs. All submissions are reviewed by the research team before being finalized. To help ensure accuracy, please review the [reference table documentation](https://www.notion.so/cpi-all/Reference-Tables-1f3efb28632b80b4b53dec019a97d70a) before uploading.")
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
    """
    Render table view for tables -- not showing up currently, may just delete if we dont wnat this

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
        #To refresh session state data, gets out of cache
        if st.button("🔄 Refresh Data", help="Force reload from database", use_container_width=True):
            st.cache_data.clear()
            
            session_key = f'{table_name}_reference_data'
            if session_key in st.session_state:
                del st.session_state[session_key]
            
            keys_to_clear = [key for key in st.session_state.keys() if key.endswith('_reference_data')]
            for key in keys_to_clear:
                del st.session_state[key]
            
            st.success("Cache cleared! Data will reload fresh.")
            st.rerun()
    
    st.markdown("---")
    
    with st.spinner(f"Loading {config.display_name} data..."):
        try:
            session_key = f'{table_name}_reference_data'
            if session_key in st.session_state:
                df = st.session_state[session_key]['existing_data']
                st.info(f"Loaded {len(df):,} rows from session state")
            else:
                df = get_table_data_cached(table_name, limit=None)
                st.info(f"Loaded {len(df):,} rows from database")
                
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
    
    # Use cached configs
    configs = get_table_configs_cached()
    available_tables = configs['available_tables']
    table_display_names = configs['display_names']
    
    display_options = [f"{table_display_names[table]} ({table})" for table in available_tables]
    
    try:
        current_index = available_tables.index(st.session_state.get('selected_table', 'institution'))
    except ValueError:
        current_index = 0
    
    selected_display = st.selectbox(
        "Which table would you like to view?",
        options=display_options,
        index=current_index,
        help="Choose a table to view its contents",
        key="view_table_selector"
    )
    
    selected_table = selected_display.split(' (')[-1].rstrip(')')
    st.session_state['selected_table'] = selected_table

    config = get_table_config(selected_table)
    if config and hasattr(config, 'general_description') and config.general_description:
        st.markdown(f"{config.general_description}")
    
    st.markdown("---")
    
    render_table_view(selected_table)


def main():
    """Main application entry point with optimizations"""
    
    # Initialize session state only once
    if 'app_initialized' not in st.session_state:
        initialize_session_state()
        st.session_state.app_initialized = True
    
    page = render_sidebar()
    
    # Lazy load page modules only when needed
    if page == "Upload New Data":
        render_upload_page()
    elif page == "View Current Tables":
        render_view_tables_page()
    elif page == "NZFT":
        from services.nzft_matching import render_nzft_page
        render_nzft_page()

if __name__ == "__main__":
    main()

