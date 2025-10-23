import streamlit as st
import pandas as pd
import io
from services.institution_service import InstitutionService
from ui.components import (
    show_bulk_upload_preview,
    show_success_message,
    show_error_message,
    create_confirmation_dialog
)
from config import MAX_BULK_UPLOAD_ROWS


def render_bulk_upload():
    """Render the bulk CSV upload interface"""
    
    st.header("Bulk Institution Upload")
    st.markdown("Upload a CSV file with multiple institutions. The system will validate and process all entries.")
    
    # Initialize service
    institution_service = InstitutionService()
    
    # File upload
    st.subheader("1️⃣ Upload CSV File")
    
    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type=['csv'],
        help=f"Maximum {MAX_BULK_UPLOAD_ROWS} rows per upload"
    )
    
    # Show template download
    with st.expander("📥 Download CSV Template"):
        template_df = pd.DataFrame({
            'institution_cpi': ['Example Corp', 'Sample LLC'],
            'institution_type_layer1': ['Public', 'Private'],
            'institution_type_layer2': ['Corporation', 'Funds'],
            'institution_type_layer3': ['Corporation', 'Asset Manager'],
            'country_sub': ['USA', 'GBR'],
            'country_parent': ['USA', 'GBR'],
            'double_counting_risk': ['Low', 'Medium'],
            'contact_info': ['contact@example.com', ''],
            'comments': ['', 'Investment fund']
        })
        
        csv_buffer = io.StringIO()
        template_df.to_csv(csv_buffer, index=False)
        csv_str = csv_buffer.getvalue()
        
        st.download_button(
            label="Download Template",
            data=csv_str,
            file_name="institution_upload_template.csv",
            mime="text/csv"
        )
        
        st.markdown("**Required columns:**")
        st.markdown("- `institution_cpi` or `institution_name`: Institution name (required)")
        st.markdown("- `institution_type_layer1`: Public or Private (recommended)")
        
        st.markdown("**Optional columns:**")
        st.markdown("- `institution_type_layer2`, `institution_type_layer3`")
        st.markdown("- `country_sub`, `country_parent`")
        st.markdown("- `double_counting_risk`, `contact_info`, `comments`")
    
    # Process uploaded file
    if uploaded_file is not None:
        try:
            # Read CSV
            df = pd.read_csv(uploaded_file)
            
            # Check row limit
            if len(df) > MAX_BULK_UPLOAD_ROWS:
                show_error_message(
                    f"File contains {len(df)} rows, but maximum is {MAX_BULK_UPLOAD_ROWS}. "
                    f"Please split into multiple files."
                )
                return
            
            st.success(f"✅ Loaded {len(df)} rows from CSV")
            
            # Show column mapping help
            st.subheader("2️⃣ Verify Column Mapping")
            
            detected_cols = df.columns.tolist()
            st.write("**Detected columns:**", ", ".join(detected_cols))
            
            # Check for required columns
            has_name_col = 'institution_cpi' in detected_cols or 'institution_name' in detected_cols
            
            if not has_name_col:
                show_error_message(
                    "No institution name column found. Please include either 'institution_cpi' or 'institution_name'"
                )
                return
            
            # Standardize column names
            if 'institution_name' in df.columns and 'institution_cpi' not in df.columns:
                df = df.rename(columns={'institution_name': 'institution_cpi'})
            
            # Validation
            st.subheader("3️⃣ Validate Data")
            
            if st.button("🔍 Validate All Entries", type="primary"):
                with st.spinner("Validating entries..."):
                    # Get existing institutions
                    existing_institutions = institution_service.query_service.get_all_institutions()
                    
                    # Validate
                    validated_df = institution_service.validation_service.validate_bulk_entries(
                        df,
                        existing_institutions
                    )
                    
                    # Store in session state
                    st.session_state['validated_df'] = validated_df
                    st.session_state['original_df'] = df
                    
                    st.success("✅ Validation complete!")
            
            # Show validation results
            if 'validated_df' in st.session_state:
                validated_df = st.session_state['validated_df']
                original_df = st.session_state['original_df']
                
                show_bulk_upload_preview(original_df, validated_df)
                
                # Count valid entries
                valid_count = len(validated_df[validated_df['status'] == 'OK'])
                warning_count = len(validated_df[validated_df['status'] == 'WARNING'])
                error_count = len(validated_df[validated_df['status'].isin(['ERROR', 'DUPLICATE'])])
                
                # Upload options
                st.subheader("4️⃣ Upload Options")
                
                upload_option = st.radio(
                    "What would you like to upload?",
                    [
                        f"Only valid entries ({valid_count} rows)",
                        f"Valid and warning entries ({valid_count + warning_count} rows)",
                        "All entries (not recommended)"
                    ],
                    help="Choose which entries to upload to the database"
                )
                
                # Determine which rows to upload
                if upload_option.startswith("Only valid"):
                    upload_df = original_df[validated_df['status'] == 'OK']
                elif upload_option.startswith("Valid and warning"):
                    upload_df = original_df[validated_df['status'].isin(['OK', 'WARNING'])]
                else:
                    upload_df = original_df
                
                # Confirm and upload
                if len(upload_df) > 0:
                    st.markdown("---")
                    
                    st.warning(f"⚠️ You are about to upload {len(upload_df)} institution(s) to the database.")
                    
                    col1, col2 = st.columns([1, 1])
                    
                    with col1:
                        if st.button("✅ Confirm Upload", type="primary", use_container_width=True):
                            with st.spinner(f"Uploading {len(upload_df)} institutions..."):
                                result = institution_service.bulk_create_institutions(
                                    upload_df,
                                    user=st.session_state.get('username', 'bulk_upload')
                                )
                                
                                # Show results
                                st.markdown("---")
                                st.subheader("Upload Results")
                                
                                col_a, col_b, col_c = st.columns(3)
                                with col_a:
                                    st.metric("Successful", result['successful'])
                                with col_b:
                                    st.metric("Failed", result['failed'])
                                with col_c:
                                    st.metric("Skipped", result['skipped'])
                                
                                st.write(result['summary'])
                                
                                # Show detailed results
                                with st.expander("View Detailed Results"):
                                    results_df = pd.DataFrame(result['details'])
                                    st.dataframe(results_df, use_container_width=True)
                                
                                # Clear session state
                                if result['successful'] > 0:
                                    if st.button("Upload Another File"):
                                        st.session_state.clear()
                                        st.rerun()
                    
                    with col2:
                        if st.button("❌ Cancel", use_container_width=True):
                            st.session_state.clear()
                            st.rerun()
                else:
                    st.info("No valid entries to upload. Please fix the errors in your CSV file.")
            else:
                st.info("👆 Click 'Validate All Entries' to check your data before uploading.")
        
        except Exception as e:
            show_error_message("Error reading CSV file", e)
    else:
        st.info("👆 Upload a CSV file to get started.")