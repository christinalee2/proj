"""
Generic form components that work with any table configuration
Reusable UI components for single entry and bulk upload forms
"""
import streamlit as st
import pandas as pd
import io
from typing import Dict, List, Any, Optional
from table_configs import TableConfig, FieldConfig, get_table_config
from services.generic_table_service import GenericTableServiceFactory
from database.cached_queries import get_table_data_cached


def render_field(field_config: FieldConfig, value: Any = None, key_suffix: str = "") -> Any:
    """
    Render a single form field based on its configuration
    
    Args:
        field_config: Field configuration
        value: Current value (for editing)
        key_suffix: Suffix for Streamlit key to avoid conflicts
        
    Returns:
        The value entered/selected by user
    """
    field_key = f"{field_config.name}_{key_suffix}" if key_suffix else field_config.name
    
    # Determine default value
    default_value = value if value is not None else ''
    
    if field_config.field_type == 'text':
        return st.text_input(
            field_config.display_name,
            value=default_value,
            placeholder=field_config.placeholder or f"Enter {field_config.display_name.lower()}...",
            help=field_config.help_text,
            key=field_key
        )
    
    elif field_config.field_type == 'textarea':
        return st.text_area(
            field_config.display_name,
            value=default_value,
            placeholder=field_config.placeholder or f"Enter {field_config.display_name.lower()}...",
            help=field_config.help_text,
            key=field_key
        )
    
    elif field_config.field_type == 'number':
        # Handle integer vs float
        if 'year' in field_config.name.lower() or 'id' in field_config.name.lower():
            return st.number_input(
                field_config.display_name,
                value=int(default_value) if default_value else None,
                help=field_config.help_text,
                key=field_key,
                step=1
            )
        else:
            return st.number_input(
                field_config.display_name,
                value=float(default_value) if default_value else None,
                help=field_config.help_text,
                key=field_key,
                format="%.6f"
            )
    
    elif field_config.field_type == 'select':
        options = field_config.options or ['']
        
        # For country fields, get options from service
        if field_config.name in ['country_sub', 'country_parent', 'country_cpi'] and not options:
            service = GenericTableServiceFactory.get_service('geography')
            dropdown_options = service.get_dropdown_options()
            options = dropdown_options.get(field_config.name, [''])
        
        # Find index of current value
        index = 0
        if default_value and default_value in options:
            index = options.index(default_value)
        
        return st.selectbox(
            field_config.display_name,
            options=options,
            index=index,
            help=field_config.help_text,
            key=field_key
        )
    
    elif field_config.field_type == 'boolean':
        # Convert various boolean representations
        bool_value = False
        if default_value:
            bool_value = str(default_value).lower() in ['true', '1', 'yes']
        
        return st.checkbox(
            field_config.display_name,
            value=bool_value,
            help=field_config.help_text,
            key=field_key
        )
    
    else:
        # Fallback to text input
        return st.text_input(
            field_config.display_name,
            value=default_value,
            help=field_config.help_text,
            key=field_key
        )


def render_single_entry_form(table_name: str):
    """
    Render single entry form for any table
    
    Args:
        table_name: Name of the table
    """
    config = get_table_config(table_name)
    if not config:
        st.error(f"No configuration found for table: {table_name}")
        return
    
    service = GenericTableServiceFactory.get_service(table_name)
    
    st.subheader(f"Add New {config.display_name}")
    st.markdown(config.description)
    st.markdown("---")
    
    # Check for duplicates on primary field input
    primary_field_config = None
    for field in config.fields:
        if field.name == config.required_fields[0]:
            primary_field_config = field
            break
    
    if primary_field_config:
        # Render primary field separately for duplicate checking
        primary_value = render_field(primary_field_config, key_suffix="primary")
        
        # Check for duplicates if value entered
        if primary_value and len(str(primary_value).strip()) >= 3:
            existing_data = get_table_data_cached(table_name)
            duplicate_check = service.check_duplicates({primary_field_config.name: primary_value}, existing_data)
            
            if duplicate_check['has_duplicates']:
                st.warning(f"Entry with this {primary_field_config.display_name} already exists")
                for match in duplicate_check['exact_matches']:
                    st.write(f"• {match.get(primary_field_config.name, 'Unknown')}")
    
    st.markdown("---")
    
    # Collect form data
    form_data = {}
    if primary_field_config:
        form_data[primary_field_config.name] = primary_value
    
    # Organize fields into columns
    required_fields = [f for f in config.fields if f.required and f.name != config.required_fields[0]]
    optional_fields = [f for f in config.fields if not f.required and f.name != config.required_fields[0]]
    
    # Required fields
    if required_fields:
        st.subheader("Required Fields")
        cols = st.columns(2)
        for i, field_config in enumerate(required_fields):
            with cols[i % 2]:
                form_data[field_config.name] = render_field(field_config, key_suffix="req")
    
    # Optional fields
    if optional_fields:
        st.subheader("Optional Fields")
        cols = st.columns(2)
        for i, field_config in enumerate(optional_fields):
            with cols[i % 2]:
                form_data[field_config.name] = render_field(field_config, key_suffix="opt")
    
    st.markdown("---")
    
    # Submit button
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.button(f"Add {config.display_name}", type="primary", use_container_width=True):
            # Validate required fields
            missing_required = []
            for field_name in config.required_fields:
                if not form_data.get(field_name) or str(form_data[field_name]).strip() == '':
                    missing_required.append(field_name)
            
            if missing_required:
                st.error(f"Please fill in required fields: {', '.join(missing_required)}")
            else:
                with st.spinner("Creating entry..."):
                    result = service.create_entry(
                        form_data,
                        user=st.session_state.get('username', 'analyst')
                    )
                    
                    if result['success']:
                        st.success(f"{config.display_name} created successfully!")
                        # Clear cache to pick up new data
                        st.cache_data.clear()
                        
                        # Option to add another
                        if st.button("Add Another", key="add_another"):
                            st.rerun()
                    else:
                        st.error(result['message'])
                        if result.get('validation_errors'):
                            for error in result['validation_errors']:
                                st.error(f"• {error}")
    
    with col2:
        if st.button("Reset Form", use_container_width=True):
            st.rerun()


def render_bulk_upload_form(table_name: str):
    """
    Render bulk upload interface for any table
    
    Args:
        table_name: Name of the table
    """
    config = get_table_config(table_name)
    if not config:
        st.error(f"No configuration found for table: {table_name}")
        return
    
    service = GenericTableServiceFactory.get_service(table_name)
    
    st.subheader(f"Bulk Upload to {config.display_name} Table")
    st.markdown(config.description)
    
    # Initialize session state
    session_key = f'bulk_upload_{table_name}'
    if f'{session_key}_df' not in st.session_state:
        st.session_state[f'{session_key}_df'] = None
    if f'{session_key}_results' not in st.session_state:
        st.session_state[f'{session_key}_results'] = None
    
    # Download template
    with st.expander("📄 Download Template"):
        template_data = create_template_data(config)
        template_df = pd.DataFrame(template_data)
        
        col1, col2 = st.columns(2)
        with col1:
            csv_buffer = io.StringIO()
            template_df.to_csv(csv_buffer, index=False)
            st.download_button(
                "CSV Template",
                csv_buffer.getvalue(),
                f"{table_name}_template.csv",
                "text/csv",
                use_container_width=True
            )
        with col2:
            excel_buffer = io.BytesIO()
            template_df.to_excel(excel_buffer, index=False, engine='openpyxl')
            st.download_button(
                "Excel Template",
                excel_buffer.getvalue(),
                f"{table_name}_template.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
    st.markdown("---")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Choose CSV or Excel file",
        type=['csv', 'xlsx', 'xls'],
        key=f"upload_{table_name}"
    )
    
    if uploaded_file is not None:
        # Parse file
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file, engine='c')
            else:
                df = pd.read_excel(uploaded_file, engine='openpyxl')
            
            # Clean column names
            df.columns = df.columns.str.strip()
            df = df.where(pd.notna(df), None)
            
            st.session_state[f'{session_key}_df'] = df
            
        except Exception as e:
            st.error(f"Error parsing file: {str(e)}")
            return
        
        df = st.session_state[f'{session_key}_df']
        
        # Validate columns
        missing_required = []
        for field_name in config.required_fields:
            if field_name not in df.columns:
                missing_required.append(field_name)
        
        if missing_required:
            st.error(f"Missing required columns: {', '.join(missing_required)}")
            st.info("Please download the template and ensure your file has all required columns.")
            return
        
        # Show preview
        st.subheader("Data Preview")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Rows", len(df))
        with col2:
            st.metric("Columns", len(df.columns))
        with col3:
            valid_rows = len(df.dropna(subset=config.required_fields))
            st.metric("Valid Rows", valid_rows)
        
        # Show data preview
        st.dataframe(df.head(10), use_container_width=True)
        
        if len(df) > 10:
            st.caption(f"Showing first 10 rows of {len(df)} total")
        
        st.markdown("---")
        
        # Process upload
        col1, col2 = st.columns([1, 1])
        
        with col1:
            if st.button(f"Upload {len(df)} rows to {config.display_name}", 
                        type="primary", use_container_width=True):
                with st.spinner("Processing upload..."):
                    result = service.bulk_create_entries(
                        df, 
                        user=st.session_state.get('username', 'analyst')
                    )
                    st.session_state[f'{session_key}_results'] = result
                    
                    # Clear cache to pick up new data
                    st.cache_data.clear()
                    st.rerun()
        
        with col2:
            if st.button("Reset Upload", use_container_width=True):
                st.session_state[f'{session_key}_df'] = None
                st.session_state[f'{session_key}_results'] = None
                st.rerun()
    
    # Show results if available
    if st.session_state[f'{session_key}_results']:
        show_bulk_upload_results(st.session_state[f'{session_key}_results'], config)


def create_template_data(config: TableConfig) -> Dict[str, List]:
    """
    Create template data for download
    
    Args:
        config: Table configuration
        
    Returns:
        Dictionary with sample data
    """
    template_data = {}
    
    for field_config in config.fields:
        if field_config.field_type == 'select' and field_config.options:
            # Use first non-empty option as example
            options = [opt for opt in field_config.options if opt]
            example_value = options[0] if options else 'Example'
        elif field_config.field_type == 'number':
            example_value = 2024 if 'year' in field_config.name.lower() else 1.0
        elif field_config.field_type == 'boolean':
            example_value = 'True'
        else:
            example_value = f'Example {field_config.display_name}'
        
        template_data[field_config.name] = [example_value, '']
    
    return template_data


def show_bulk_upload_results(results: Dict[str, Any], config: TableConfig):
    """
    Display bulk upload results
    
    Args:
        results: Upload results
        config: Table configuration
    """
    st.success("Upload Complete!")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Successfully Created", results['successful'])
    with col2:
        st.metric("Failed", results['failed'])
    with col3:
        st.metric("Total Processed", results['total_rows'])
    
    st.markdown(f"**Summary:** {results['summary']}")
    
    # Show failed entries if any
    if results['failed'] > 0:
        with st.expander("View Failed Entries"):
            failed_details = [d for d in results['details'] if d['status'] == 'failed']
            for detail in failed_details:
                st.error(f"Row {detail['row_number']} ({detail['identifier']}): {detail['message']}")
    
    # Show successful entries
    if results['successful'] > 0:
        with st.expander("View Successful Entries"):
            success_details = [d for d in results['details'] if d['status'] == 'success']
            for detail in success_details[:10]:  # Show first 10
                st.success(f"Row {detail['row_number']} ({detail['identifier']}): Created successfully")
            
            if len(success_details) > 10:
                st.caption(f"... and {len(success_details) - 10} more successful entries")


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
    
    # Load controls
    col1, col2 = st.columns([3, 1])
    
    with col1:
        load_all = st.checkbox(
            "Load all rows",
            value=False,
            help="Load entire table (may take time for large tables)"
        )
    
    with col2:
        if st.button("🔄 Refresh Data", help="Force reload from database", use_container_width=True):
            st.cache_data.clear()
            st.success("Cache cleared! Data will reload fresh.")
            st.rerun()
    
    st.markdown("---")
    
    # Load and display data
    with st.spinner(f"Loading {config.display_name} data..."):
        try:
            if load_all:
                df = get_table_data_cached(table_name, limit=None)
                st.info(f"✅ Loaded all {len(df):,} rows from {config.display_name}")
            else:
                df = get_table_data_cached(table_name, limit=1000)
                st.info(f"✅ Showing first 1,000 rows. Check 'Load all rows' to see complete table.")
            
            if df.empty:
                st.info(f"No data found in {config.display_name} table")
                return
            
            # Show metrics
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
            
            # Search functionality
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
            
            # Filter data
            if search_term:
                mask = df.astype(str).apply(
                    lambda x: x.str.contains(search_term, case=False, na=False)
                ).any(axis=1)
                filtered_df = df[mask].head(show_rows)
                st.info(f"Found {len(df[mask])} matching rows (showing {len(filtered_df)})")
            else:
                filtered_df = df.head(show_rows)
            
            # Display data
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