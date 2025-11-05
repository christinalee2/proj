import streamlit as st
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

from table_configs import get_available_tables, get_table_display_names, get_table_config

from database.cached_queries import get_table_data_cached
from services.nzft_matching import render_nzft_page, reset_nzft_session


load_dotenv()



def load_auth_config():
    load_dotenv()               # Load variables from .env
    from streamlit.runtime.secrets import secrets_singleton

    auth_secrets = {
        "auth": {
            "redirect_uri": os.getenv("redirect_uri"),
            "cookie_secret": os.getenv("cookie_secret"),
            "oidc": {
                "client_id":     os.getenv("client_id"),
                "client_secret": os.getenv("client_secret"),
                "server_metadata_url": os.getenv("server_metadata_url"),
                "client_kwargs": { "prompt": "login",
                                 "scope": "openid"}
            }
        }
    }

    # Inject settings into Streamlit’s secret store
    secrets_singleton._secrets = auth_secrets
    for k, v in auth_secrets.items():
        secrets_singleton._maybe_set_environment_variable(k, v)

load_auth_config()



# # Handle authentication
# if not st.user:
#     st.login("oidc")
#     st.stop()

# # Check if user actually has authentication data
# user = st.user
# user_dict = dict(user) if user else {}

# # If no email/authentication data, force login
# if not user_dict or not user_dict.get('email'):
#     st.error("Authentication required to access this application")
#     st.info("Please complete the login process")
#     if st.button("Login with AWS Cognito"):
#         st.login("oidc")
#     st.stop()
    
# current_username = (
#     user.get('email', '').split('@')[0] if user.get('email') 
#     else user.get('preferred_username', 
#     user.get('name', 'authenticated_user'))
# )

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



# if not st.user.is_logged_in:
#     st.login("oidc")
#     st.stop()

# user = st.user
# st.sidebar.markdown(f"**👋 Hello {user.email}!**")
# st.button("Logout", on_click=st.logout)

# try:
#     if not hasattr(st, 'user') or not st.user or not st.user.is_logged_in:
#         st.info("Please log in to access this application")
#         st.login("oidc")
#         st.stop()
# except Exception as e:
#     st.error("Authentication required")
#     st.login("oidc") 
#     st.stop()

# # Only access user info if authenticated
# user = st.user

# Check if we're in callback processing
query_params = st.query_params if hasattr(st, 'query_params') else {}
is_callback = 'code' in query_params or 'state' in query_params

# Initialize authentication session state
if 'auth_attempted' not in st.session_state:
    st.session_state.auth_attempted = False

# Check authentication status
user_logged_in = False
user = None

try:
    if hasattr(st, 'user') and st.user and hasattr(st.user, 'is_logged_in'):
        user_logged_in = st.user.is_logged_in
        if user_logged_in:
            user = st.user
except:
    user_logged_in = False

# Handle authentication
if not user_logged_in:
    if 'force_clean_auth' not in st.session_state:
        st.session_state.force_clean_auth = True
        for key in list(st.session_state.keys()):
            if 'auth' in key.lower():
                del st.session_state[key]
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
        # Auth was attempted but we're still not logged in
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

# If we get here, user is authenticated
user = st.user  # Now it's safe to assign this

        
def initialize_session_state():
    """Initializes default page variables"""
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
        
        # Authentication status and user info  
        st.subheader("Authentication")
        user = st.user
        user_email = user.get('email', 'No email available')
        st.success(f"Logged in as: **{user_email}**")
        
        if st.button("Logout", type="secondary", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            
            # Reset auth state
            st.session_state.auth_attempted = False
            
            # Logout
            st.logout()
            st.rerun()
        
        # Update session state with authenticated username
        authenticated_username = (
            user.get('email', '').split('@')[0] if user.get('email') 
            else user.get('preferred_username', 
            user.get('name', 'authenticated_user'))
        )
        st.session_state['username'] = authenticated_username
        
        st.markdown("---")
        
        st.subheader("Current User")
        st.text_input(
            "Active User",
            value=authenticated_username,
            disabled=True,
            help="Authenticated user from Cognito"
        )
        
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
                  # else 1
                #else 2
        )
        
        st.session_state['current_page'] = page
        
        st.markdown("---")
        

        st.subheader("Cache Management")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Clear Data Cache", help="Clear cached table data", use_container_width=True):
                st.cache_data.clear()
                # Clear all table reference data from session state, useful when there's a lot of new data etc.
                keys_to_clear = [key for key in st.session_state.keys() if key.endswith('_reference_data')]
                for key in keys_to_clear:
                    del st.session_state[key]
                st.success("Data cache cleared!")
                st.rerun()
        
        with col2:
            if st.button("🔄 Clear All", help="Clear all caches", use_container_width=True):
                st.cache_data.clear()
                st.cache_resource.clear()
                keys_to_clear = [key for key in st.session_state.keys() if key.endswith('_reference_data')]
                for key in keys_to_clear:
                    del st.session_state[key]
                st.success("All caches cleared!")
                st.rerun()
        
        # Add NZFT reset button
        if st.session_state.get('current_page') == 'NZFT':
            st.markdown("---")
            if st.button("🔄 Reset NZFT Session", help="Clear NZFT matching data", use_container_width=True):
                reset_nzft_session()
                st.success("NZFT session reset!")
                st.rerun()
        
        st.caption("Data cached for 2-4 hours")
        st.caption("Services cached indefinitely")
        
        st.markdown("---")
        
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d')}")
    
    return st.session_state['current_page']


def render_upload_page():
    """Render the upload page, lets you select table and single/bulk entry"""
    st.header("Upload New Data")
    st.markdown("This tool lets you add new data to the CPI reference tables. You can upload a single entry or import multiple records at once, depending on your needs. All submissions are reviewed by the research team before being finalized. To help ensure accuracy, please review the [reference table documentation](https://www.notion.so/cpi-all/Reference-Tables-1f3efb28632b80b4b53dec019a97d70a) before uploading.")
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

    config = get_table_config(selected_table)
    if config and hasattr(config, 'general_description') and config.general_description:
        st.markdown(f"{config.general_description}")
    
    
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
    """Render the view tables page -- also may delete"""
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

    config = get_table_config(selected_table)
    if config and hasattr(config, 'general_description') and config.general_description:
        st.markdown(f"{config.general_description}")
    
    st.markdown("---")
    
    render_table_view(selected_table)


def main():
    """Main application entry point"""
    
    # Initialize
    initialize_session_state()
    
    page = render_sidebar()
    
    # Render selected page
    if page == "Upload New Data":
        render_upload_page()
    elif page == "View Current Tables":
        render_view_tables_page()
    elif page == "NZFT": 
        render_nzft_page()


if __name__ == "__main__":
    main()

