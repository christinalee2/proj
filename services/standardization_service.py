"""
Fixed Standardization Service for Institution and Geography Mappings
Handles the "Keep" functionality for fuzzy matches with proper error handling
"""
from typing import Optional, Dict, Any
import pandas as pd
import traceback
from database.cached_queries import get_table_data_cached
from database.queries import QueryService
from utils.text_processing import TextProcessor


class StandardizationService:
    """Enhanced service for handling standardization mappings with Keep functionality"""
    
    def __init__(self):
        self.query_service = QueryService()
    
    def process_keep_institution(self, user_input: str, matched_institution: str) -> Dict[str, Any]:
        """
        Process "Keep" action for institution fuzzy match
        
        Args:
            user_input: The name the user typed (e.g., "petroquim")
            matched_institution: The fuzzy matched name from institution table (e.g., "petroquim sa")
            
        Returns:
            Dictionary with success status and message
        """
        try:
            print(f"DEBUG: Processing keep for institution: '{user_input}' -> '{matched_institution}'")
            
            # Get standardization table data
            standardization_df = get_table_data_cached('institution_standardization', limit=None)
            print(f"DEBUG: Found {len(standardization_df)} existing standardization records")
            
            # Step 1: Check if user input already exists in either column
            if not standardization_df.empty:
                # Check institution_cpi column
                existing_in_cpi = standardization_df[
                    standardization_df['institution_cpi'].str.lower() == user_input.lower()
                ]
                
                # Check institution_original column  
                existing_in_original = standardization_df[
                    standardization_df['institution_original'].str.lower() == user_input.lower()
                ]
                
                if not existing_in_cpi.empty or not existing_in_original.empty:
                    print(f"DEBUG: Mapping already exists for {user_input}")
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
                
                if not matched_in_cpi.empty:
                    print(f"DEBUG: Found existing mapping for {matched_institution}, creating new mapping")
                    # Create new mapping: user_input -> matched_institution
                    return self._create_institution_standardization_mapping(
                        user_input,
                        matched_institution,
                        f'Unknown'
                    )
            
            # Step 3: Check if matched institution exists in institution_original column
            if not standardization_df.empty:
                matched_in_original = standardization_df[
                    standardization_df['institution_original'].str.lower() == matched_institution.lower()
                ]
                
                if not matched_in_original.empty:
                    print(f"DEBUG: Found {matched_institution} in original column, using its standardized name")
                    # Use the standardized name from that row
                    standardized_name = matched_in_original.iloc[0]['institution_cpi']
                    return self._create_institution_standardization_mapping(
                        user_input,
                        standardized_name,
                        f'Mapping via existing standardization of "{matched_institution}"'
                    )
            
            # Step 4: If matched institution not found in standardization table, use it directly
            print(f"DEBUG: No existing standardization found, creating direct mapping")
            return self._create_institution_standardization_mapping(
                user_input,
                matched_institution,
                f'Direct fuzzy match mapping created by analyst'
            )
            
        except Exception as e:
            print(f"ERROR in process_keep_institution: {str(e)}")
            traceback.print_exc()
            return {
                'success': False,
                'action': 'error',
                'message': f'Error processing keep action: {str(e)}'
            }
    
    def process_keep_geography(self, user_input: str, matched_country: str) -> Dict[str, Any]:
        """
        Process "Keep" action for geography fuzzy match
        
        Args:
            user_input: The country name the user typed
            matched_country: The fuzzy matched name from geography table
            
        Returns:
            Dictionary with success status and message
        """
        try:
            print(f"DEBUG: Processing keep for geography: '{user_input}' -> '{matched_country}'")
            
            # Get standardization table data
            standardization_df = get_table_data_cached('geography_standardization', limit=None)
            print(f"DEBUG: Found {len(standardization_df)} existing geography standardization records")
            
            # Step 1: Check if user input already exists in either column
            if not standardization_df.empty:
                # Check country_cpi column
                existing_in_cpi = standardization_df[
                    standardization_df['country_cpi'].str.lower() == user_input.lower()
                ]
                
                # Check country_original column  
                existing_in_original = standardization_df[
                    standardization_df['country_original'].str.lower() == user_input.lower()
                ]
                
                if not existing_in_cpi.empty or not existing_in_original.empty:
                    print(f"DEBUG: Geography mapping already exists for {user_input}")
                    return {
                        'success': True,
                        'action': 'no_action',
                        'message': f'Mapping for "{user_input}" already exists'
                    }
            
            # Step 2: Check if matched country exists in country_cpi column
            if not standardization_df.empty:
                matched_in_cpi = standardization_df[
                    standardization_df['country_cpi'].str.lower() == matched_country.lower()
                ]
                
                if not matched_in_cpi.empty:
                    print(f"DEBUG: Found existing geography mapping for {matched_country}")
                    # Create new mapping: user_input -> matched_country
                    return self._create_geography_standardization_mapping(
                        user_input,
                        matched_country
                    )
            
            # Step 3: Check if matched country exists in country_original column
            if not standardization_df.empty:
                matched_in_original = standardization_df[
                    standardization_df['country_original'].str.lower() == matched_country.lower()
                ]
                
                if not matched_in_original.empty:
                    print(f"DEBUG: Found {matched_country} in geography original column")
                    # Use the standardized name from that row
                    standardized_name = matched_in_original.iloc[0]['country_cpi']
                    return self._create_geography_standardization_mapping(
                        user_input,
                        standardized_name
                    )
            
            # Step 4: If matched country not found in standardization table, use it directly
            print(f"DEBUG: No existing geography standardization found, creating direct mapping")
            return self._create_geography_standardization_mapping(
                user_input,
                matched_country
            )
            
        except Exception as e:
            print(f"ERROR in process_keep_geography: {str(e)}")
            traceback.print_exc()
            return {
                'success': False,
                'action': 'error',
                'message': f'Error processing geography keep action: {str(e)}'
            }
    
    def _create_institution_standardization_mapping(self, original_name: str, 
                                                   standardized_name: str, reference: str) -> Dict[str, Any]:
        """Create a new standardization mapping in institution_standardization table"""
        try:
            print(f"DEBUG: Creating institution mapping: '{original_name}' -> '{standardized_name}'")
            
            # Get next ID using the correct primary key from table config
            existing_data = get_table_data_cached('institution_standardization', limit=None)
            id_column = 'id_institution'  # From table_configs.py
            
            print(f"DEBUG: Existing standardization data has {len(existing_data)} rows")
            print(f"DEBUG: Using ID column: {id_column}")
            
            if existing_data.empty or id_column not in existing_data.columns:
                next_id = 1
                print(f"DEBUG: Table empty or missing ID column, starting with ID 1")
            else:
                print(f"DEBUG: Existing ID column values: {existing_data[id_column].tolist()}")
                max_id = existing_data[id_column].max()
                next_id = int(max_id) + 1 if pd.notna(max_id) else 1
                print(f"DEBUG: Calculated next ID: {next_id}")
            
            # Create mapping data
            mapping_data = {
                id_column: next_id,
                'institution_original': TextProcessor.normalize_institution_name(original_name),
                'institution_cpi': standardized_name,
                'reference': reference
            }
            
            print(f"DEBUG: Mapping data to insert: {mapping_data}")
            
            # Insert into database
            success = self.query_service.execute_insert('institution_standardization', mapping_data)
            print(f"DEBUG: Insert result: {success}")
            
            if success:
                print(f"DEBUG: Successfully created institution mapping with ID {next_id}")
                
                return {
                    'success': True,
                    'action': 'created_mapping',
                    'message': f'Created standardization mapping: "{original_name}" -> "{standardized_name}"',
                    'mapping_id': next_id
                }
            else:
                print(f"ERROR: Database insert returned False")
                return {
                    'success': False,
                    'action': 'insert_failed',
                    'message': 'Failed to insert standardization mapping into database'
                }
                
        except Exception as e:
            print(f"ERROR in _create_institution_standardization_mapping: {str(e)}")
            traceback.print_exc()
            return {
                'success': False,
                'action': 'error',
                'message': f'Error creating mapping: {str(e)}'
            }
    
    def _create_geography_standardization_mapping(self, original_name: str, 
                                                standardized_name: str) -> Dict[str, Any]:
        """Create a new geography standardization mapping"""
        try:
            print(f"DEBUG: Creating geography mapping: '{original_name}' -> '{standardized_name}'")
            
            # Get next ID using the correct primary key from table config
            existing_data = get_table_data_cached('geography_standardization', limit=None)
            id_column = 'id_geography'  # From table_configs.py
            
            print(f"DEBUG: Existing geography standardization data has {len(existing_data)} rows")
            print(f"DEBUG: Using ID column: {id_column}")
            
            if existing_data.empty or id_column not in existing_data.columns:
                next_id = 1
                print(f"DEBUG: Geography table empty or missing ID column, starting with ID 1")
            else:
                print(f"DEBUG: Existing geography ID column values: {existing_data[id_column].tolist()}")
                max_id = existing_data[id_column].max()
                next_id = int(max_id) + 1 if pd.notna(max_id) else 1
                print(f"DEBUG: Calculated next geography ID: {next_id}")
            
            # Create mapping data
            mapping_data = {
                id_column: next_id,
                'country_original': TextProcessor.normalize_institution_name(original_name),
                'country_cpi': standardized_name
            }
            
            print(f"DEBUG: Geography mapping data to insert: {mapping_data}")
            
            # Insert into database
            success = self.query_service.execute_insert('geography_standardization', mapping_data)
            print(f"DEBUG: Geography insert result: {success}")
            
            if success:
                # Cache will be cleared after all operations complete
                print(f"DEBUG: Successfully created geography mapping with ID {next_id}")
                
                return {
                    'success': True,
                    'action': 'created_mapping',
                    'message': f'Created geography mapping: "{original_name}" -> "{standardized_name}"',
                    'mapping_id': next_id
                }
            else:
                print(f"ERROR: Geography database insert returned False")
                return {
                    'success': False,
                    'action': 'insert_failed',
                    'message': 'Failed to insert geography mapping into database'
                }
                
        except Exception as e:
            print(f"ERROR in _create_geography_standardization_mapping: {str(e)}")
            traceback.print_exc()
            return {
                'success': False,
                'action': 'error',
                'message': f'Error creating geography mapping: {str(e)}'
            }
    
    def get_standardized_name(self, original_name: str, table_type: str = 'institution') -> Optional[str]:
        """
        Get the standardized name for an input if it exists
        
        Args:
            original_name: The original name to look up
            table_type: 'institution' or 'geography'
            
        Returns:
            Standardized name if found, None otherwise
        """
        try:
            if table_type == 'institution':
                table_name = 'institution_standardization'
                original_col = 'institution_original'
                cpi_col = 'institution_cpi'
            else:
                table_name = 'geography_standardization'
                original_col = 'country_original'
                cpi_col = 'country_cpi'
            
            standardization_df = get_table_data_cached(table_name, limit=None)
            
            if standardization_df.empty:
                return None
            
            # Look for exact match in original column
            matches = standardization_df[
                standardization_df[original_col].str.lower() == original_name.lower()
            ]
            
            if not matches.empty:
                return matches.iloc[0][cpi_col]
            
            return None
            
        except Exception as e:
            print(f"Error getting standardized name: {e}")
            return None