"""
Debug version of standardization service to identify insert failures
"""
from typing import Optional, Dict, Any
import pandas as pd
from database.cached_queries import get_table_data_cached
from database.queries import QueryService
from utils.text_processing import TextProcessor
import streamlit as st


class DebugStandardizationService:
    """Debug version with detailed error reporting"""
    
    def __init__(self):
        self.query_service = QueryService()
    
    def debug_table_structure(self, table_name: str):
        """Debug table structure to understand schema"""
        try:
            st.write(f"**Debugging {table_name} table:**")
            
            # Check if table exists and get its structure
            df = get_table_data_cached(table_name, limit=5)
            
            if df.empty:
                st.write(f"- Table {table_name} exists but is empty")
                st.write("- Cannot determine schema from empty table")
                return None
            else:
                st.write(f"- Table {table_name} has {len(df)} rows")
                st.write(f"- Columns: {list(df.columns)}")
                st.write(f"- Column types: {df.dtypes.to_dict()}")
                st.write("- Sample data:")
                st.dataframe(df.head(2))
                return df.columns.tolist()
                
        except Exception as e:
            st.error(f"Error accessing {table_name}: {str(e)}")
            return None
    
    def debug_process_keep_institution(self, user_input: str, matched_institution: str) -> Dict[str, Any]:
        """Debug version of institution keep processing"""
        st.write("**Debug: Processing Institution Keep**")
        st.write(f"- User input: '{user_input}'")
        st.write(f"- Matched institution: '{matched_institution}'")
        
        try:
            # Debug table structure first
            columns = self.debug_table_structure('institution_standardization')
            if columns is None:
                return {'success': False, 'message': 'Cannot access institution_standardization table'}
            
            # Get standardization table data
            standardization_df = get_table_data_cached('institution_standardization', limit=None)
            st.write(f"- Standardization table has {len(standardization_df)} existing rows")
            
            # Step 1: Check if user input already exists
            if not standardization_df.empty:
                # Check institution_cpi column
                existing_in_cpi = standardization_df[
                    standardization_df['institution_cpi'].str.lower() == user_input.lower()
                ]
                
                # Check institution_original column  
                existing_in_original = standardization_df[
                    standardization_df['institution_original'].str.lower() == user_input.lower()
                ]
                
                st.write(f"- Found {len(existing_in_cpi)} matches in institution_cpi column")
                st.write(f"- Found {len(existing_in_original)} matches in institution_original column")
                
                if not existing_in_cpi.empty or not existing_in_original.empty:
                    return {
                        'success': True,
                        'action': 'no_action',
                        'message': f'Mapping for "{user_input}" already exists'
                    }
            
            # Step 2: Check if matched institution exists in institution_cpi column
            if not standardization_df.empty:
                matched_in_cpi = standardization_df[
                    standardization_df['institution_cpi'].str.lower() == matched_institution.lower()
                ]
                
                st.write(f"- Found {len(matched_in_cpi)} matches for '{matched_institution}' in institution_cpi")
                
                if not matched_in_cpi.empty:
                    # Create new mapping: user_input -> matched_institution
                    return self._debug_create_institution_mapping(
                        user_input,
                        matched_institution,
                        'Fuzzy match mapping created by analyst',
                        columns
                    )
            
            # Step 3: Check if matched institution exists in institution_original column
            if not standardization_df.empty:
                matched_in_original = standardization_df[
                    standardization_df['institution_original'].str.lower() == matched_institution.lower()
                ]
                
                st.write(f"- Found {len(matched_in_original)} matches for '{matched_institution}' in institution_original")
                
                if not matched_in_original.empty:
                    # Use the standardized name from that row
                    standardized_name = matched_in_original.iloc[0]['institution_cpi']
                    st.write(f"- Using existing standardized name: '{standardized_name}'")
                    return self._debug_create_institution_mapping(
                        user_input,
                        standardized_name,
                        f'Mapping via existing standardization of "{matched_institution}"',
                        columns
                    )
            
            # Step 4: If matched institution not found in standardization table, use it directly
            st.write("- No existing standardization found, creating direct mapping")
            return self._debug_create_institution_mapping(
                user_input,
                matched_institution,
                'Direct fuzzy match mapping created by analyst',
                columns
            )
            
        except Exception as e:
            st.error(f"Error in debug processing: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
            return {
                'success': False,
                'action': 'error',
                'message': f'Error processing keep action: {str(e)}'
            }
    
    def _debug_create_institution_mapping(self, original_name: str, standardized_name: str, 
                                        reference: str, expected_columns: list) -> Dict[str, Any]:
        """Debug version of creating institution mapping"""
        try:
            st.write("**Debug: Creating Institution Mapping**")
            
            # Get next ID
            existing_data = get_table_data_cached('institution_standardization', limit=None)
            id_column = 'id_institution'
            
            if existing_data.empty or id_column not in existing_data.columns:
                next_id = 1
                st.write(f"- Table empty or no {id_column} column, using ID: {next_id}")
            else:
                max_id = existing_data[id_column].max()
                next_id = int(max_id) + 1 if pd.notna(max_id) else 1
                st.write(f"- Current max ID: {max_id}, using next ID: {next_id}")
            
            # Create mapping data based on expected columns
            mapping_data = {}
            
            # Always include the ID
            mapping_data[id_column] = next_id
            
            # Add required fields
            mapping_data['institution_original'] = TextProcessor.normalize_institution_name(original_name)
            mapping_data['institution_cpi'] = standardized_name
            
            # Add reference field if it exists in schema
            if 'reference' in expected_columns:
                mapping_data['reference'] = reference
            
            st.write(f"- Mapping data to insert: {mapping_data}")
            st.write(f"- Expected columns: {expected_columns}")
            
            # Verify all required fields are present
            missing_fields = [col for col in expected_columns if col not in mapping_data and col != id_column]
            if missing_fields:
                st.warning(f"- Missing fields (will be NULL): {missing_fields}")
            
            # Insert into database
            st.write("- Attempting database insert...")
            success = self.query_service.execute_insert('institution_standardization', mapping_data)
            
            if success:
                st.success("- Database insert successful!")
                # Clear cache to pick up new data
                st.cache_data.clear()
                
                return {
                    'success': True,
                    'action': 'created_mapping',
                    'message': f'Created standardization mapping: "{original_name}" -> "{standardized_name}"',
                    'mapping_id': next_id
                }
            else:
                st.error("- Database insert failed!")
                return {
                    'success': False,
                    'action': 'insert_failed',
                    'message': 'Failed to insert standardization mapping'
                }
                
        except Exception as e:
            st.error(f"- Exception during insert: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
            return {
                'success': False,
                'action': 'error',
                'message': f'Error creating mapping: {str(e)}'
            }


# Test function to debug both tables
def debug_standardization_tables():
    """Test function to check both standardization tables"""
    st.subheader("Standardization Tables Debug")
    
    debug_service = DebugStandardizationService()
    
    # Test institution_standardization
    st.write("### Institution Standardization Table")
    inst_columns = debug_service.debug_table_structure('institution_standardization')
    
    # Test geography_standardization  
    st.write("### Geography Standardization Table")
    geo_columns = debug_service.debug_table_structure('geography_standardization')
    
    # Test a simple insert
    if st.button("Test Institution Mapping Insert"):
        result = debug_service.debug_process_keep_institution("test company", "Test Company Ltd")
        st.write("**Result:**", result)
        
    return inst_columns, geo_columns