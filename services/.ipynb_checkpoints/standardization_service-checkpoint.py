from typing import Optional, Dict, Any
import pandas as pd
import traceback
from database.cached_queries import get_table_data_cached
from database.queries import QueryService
from utils.text_processing import TextProcessor


class StandardizationService:
    """Handles standardization mappings with Keep functionality by putting into the correct standardization table (only works for institution and geography"""
    
    def __init__(self):
        self.query_service = QueryService()
    
    def process_keep_institution(self, user_input: str, matched_institution: str, 
                               standardization_df: Optional[pd.DataFrame] = None,
                               institution_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Process "Keep" action for institution fuzzy match
        
        Args:
            user_input: The name the user typed
            matched_institution: The fuzzy matched name from institution table
            
        Returns:
            Dictionary with success status and message
        """
        try:
            
            # standardization_df = get_table_data_cached('institution_standardization', limit=None)
            if standardization_df is None:
                standardization_df = get_table_data_cached('institution_standardization', limit=None)
            
            # Check if user input already exists in either column
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
                    return {
                        'success': True,
                        'action': 'no_action',
                        'message': f'Mapping for "{user_input}" already exists'
                    }
            
            # Get the id_institution_cpi from the matched institution
            if institution_df is None:
                institution_df = get_table_data_cached('institution', limit=None)
            # institution_df = get_table_data_cached('institution', limit=None)
            matched_institution_row = None
            id_institution_cpi = None
            
            if not institution_df.empty:
                matches = institution_df[
                    institution_df['institution_cpi'].str.lower() == matched_institution.lower()
                ]
                if not matches.empty:
                    matched_institution_row = matches.iloc[0]
                    id_institution_cpi = matched_institution_row.get('id_institution_cpi')
                    print(f"DEBUG: Found id_institution_cpi = {id_institution_cpi} for matched institution '{matched_institution}'")
            
            if id_institution_cpi is None:
                print(f"WARNING: Could not find id_institution_cpi for matched institution '{matched_institution}'")
                return {
                    'success': False,
                    'action': 'error',
                    'message': f'Could not find ID for matched institution "{matched_institution}"'
                }
            
            # Check if matched institution exists in institution_cpi column
            if not standardization_df.empty:
                matched_in_cpi = standardization_df[
                    standardization_df['institution_cpi'].str.lower() == matched_institution.lower()
                ]
                
                if not matched_in_cpi.empty:
                    return self._create_institution_standardization_mapping(
                        user_input,
                        matched_institution,
                        id_institution_cpi,
                        f'Direct mapping to existing institution',
                        standardization_df  # Pass the data
                    )
            
            # Check if matched institution exists in institution_original column
            if not standardization_df.empty:
                matched_in_original = standardization_df[
                    standardization_df['institution_original'].str.lower() == matched_institution.lower()
                ]
                
                if not matched_in_original.empty:
                    print(f"DEBUG: Found {matched_institution} in original column, using its standardized name")
                    # Use the standardized name from that row, but keep the id_institution_cpi from the actual institution
                    standardized_name = matched_in_original.iloc[0]['institution_cpi']
                    return self._create_institution_standardization_mapping(
                        user_input,
                        standardized_name,
                        id_institution_cpi,
                        f'Mapping via existing standardization of "{matched_institution}"',
                        standardization_df  # Pass the data
                    )
            
            # If matched institution not found in standardization table, use it directly
            print(f"DEBUG: No existing standardization found, creating direct mapping")
            return self._create_institution_standardization_mapping(
                user_input,
                matched_institution,
                id_institution_cpi,
                f'Direct mapping to "{matched_institution}"',
                standardization_df  # Pass the data
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
            
            standardization_df = get_table_data_cached('geography_standardization', limit=None)
            
            # Check if user input already exists in either column
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
            
            # Check if matched country exists in country_cpi column
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
            
            # Check if matched country exists in country_original column
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
            
            # If matched country not found in standardization table, use it directly
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
                                                   standardized_name: str, 
                                                   id_institution_cpi: str,
                                                   reference: str,
                                                   existing_data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Create a new standardization mapping in institution_standardization table"""
        try:
            print(f"DEBUG: Creating institution mapping: '{original_name}' -> '{standardized_name}' (ID: {id_institution_cpi})")
            
            # Use passed data or load fresh
            if existing_data is None:
                existing_data = get_table_data_cached('institution_standardization', limit=None)
            
            id_column = 'id_institution'  
            
            if existing_data.empty or id_column not in existing_data.columns:
                next_id = 1
            else:
                max_id = existing_data[id_column].max()
                next_id = int(max_id) + 1 if pd.notna(max_id) else 1
            
            from config import CURRENT_YEAR
            mapping_data = {
                id_column: next_id,
                'id_institution_cpi': id_institution_cpi,  # NEW: Include the institution ID
                'institution_original': TextProcessor.normalize_institution_name(original_name),
                'institution_cpi': standardized_name,
                'reference': reference,
                'created_at': CURRENT_YEAR,     
                'created_by': 'analyst'       
            }
            
            mapping_data = {k: v for k, v in mapping_data.items() if v is not None}
            
            print(f"DEBUG: Inserting mapping data: {mapping_data}")
            
            success = self.query_service.execute_insert('institution_standardization', mapping_data)
            
            if success:
                print(f"DEBUG: Successfully created institution standardization mapping with ID {next_id}")
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
                'message': f'Error creating institution standardization mapping: {str(e)}'
            }
    
    def _create_geography_standardization_mapping(self, original_name: str, 
                                                standardized_name: str) -> Dict[str, Any]:
        """Create a new geography standardization mapping"""
        try:
            print(f"DEBUG: Creating geography mapping: '{original_name}' -> '{standardized_name}'")
            
            existing_data = get_table_data_cached('geography_standardization', limit=None)
            id_column = 'id_geography'  
            
            
            if existing_data.empty or id_column not in existing_data.columns:
                next_id = 1
            else:
                max_id = existing_data[id_column].max()
                next_id = int(max_id) + 1 if pd.notna(max_id) else 1
            
            from config import CURRENT_YEAR
            mapping_data = {
                id_column: next_id,
                'country_original': TextProcessor.normalize_institution_name(original_name),
                'country_cpi': standardized_name,
                'created_at': CURRENT_YEAR,    
                'created_by': 'analyst'       
            }
            
            mapping_data = {k: v for k, v in mapping_data.items() if v is not None}
            
            
            success = self.query_service.execute_insert('geography_standardization', mapping_data)
            
            if success:
                # Cache will be cleared after all operations complete
                
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
    
    # def get_standardized_name(self, original_name: str, table_type: str = 'institution') -> Optional[str]:
    #     """
    #     Get the standardized name for an input if it exists
        
    #     Args:
    #         original_name: The original name to look up
    #         table_type: 'institution' or 'geography'
            
    #     Returns:
    #         Standardized name if found, None otherwise
    #     """
    #     try:
    #         if table_type == 'institution':
    #             table_name = 'institution_standardization'
    #             original_col = 'institution_original'
    #             cpi_col = 'institution_cpi'
    #         else:
    #             table_name = 'geography_standardization'
    #             original_col = 'country_original'
    #             cpi_col = 'country_cpi'
            
    #         standardization_df = get_table_data_cached(table_name, limit=None)
            
    #         if standardization_df.empty:
    #             return None
            
    #         matches = standardization_df[
    #             standardization_df[original_col].str.lower() == original_name.lower()
    #         ]
            
    #         if not matches.empty:
    #             return matches.iloc[0][cpi_col]
            
    #         return None
            
    #     except Exception as e:
    #         print(f"Error getting standardized name: {e}")
    #         return None