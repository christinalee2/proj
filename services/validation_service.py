from typing import Dict, List, Tuple, Optional
import pandas as pd
from database.queries import QueryService
from utils.fuzzy_matching import FuzzyMatcher
from utils.text_processing import TextProcessor


class ValidationService:
    """Handles validation of institution data"""
    
    def __init__(self):
        self.fuzzy_matcher = FuzzyMatcher()
        self.query_service = QueryService()


        
    def validate_institution_entry(
        self,
        institution_name: str,
        existing_institutions: pd.DataFrame
    ) -> Dict[str, any]:
        """
        Validate a new institution entry against the existing institutions and returns a dict with the results

        """
        normalized_name = TextProcessor.normalize_institution_name(institution_name)
        
        validation_result = {
            'is_valid': True,
            'normalized_name': normalized_name,
            'has_exact_duplicate': False,
            'exact_match': None,
            'has_fuzzy_duplicate': False,
            'fuzzy_matches': [],
            'warnings': [],
            'errors': []
        }
        
        # Check for exact duplicates in institution table (case-insensitive)
        exact_match = existing_institutions[
            existing_institutions['institution_cpi'].str.lower() == normalized_name.lower()
        ]
        
        if not exact_match.empty:
            validation_result['has_exact_duplicate'] = True
            validation_result['exact_match'] = exact_match.iloc[0].to_dict()
            validation_result['errors'].append(
                f"Institution '{normalized_name}' already exists in the institution table"
            )
            validation_result['is_valid'] = False
        
        if not validation_result['has_exact_duplicate']:
            try:
                # Use the query service to get standardization data
                standardization_df = self.query_service.get_table_data('institution_standardization', limit=None)
                if not standardization_df.empty and 'institution_original' in standardization_df.columns:
                    standardization_match = standardization_df[
                        standardization_df['institution_original'].str.lower() == normalized_name.lower()
                    ]
                    
                    if not standardization_match.empty:
                        validation_result['has_exact_duplicate'] = True
                        validation_result['exact_match'] = standardization_match.iloc[0].to_dict()
                        validation_result['errors'].append(
                            f"Institution '{normalized_name}' already exists in the standardization table"
                        )
                        validation_result['is_valid'] = False
                        
            except Exception as e:
                print(f"Error checking institution_standardization table: {e}")
                # Don't fail validation if we can't check standardization table
        
        # Check for fuzzy matches (only if no exact duplicate found)
        if not validation_result['has_exact_duplicate']:
            fuzzy_matches = self.fuzzy_matcher.find_similar_institutions(
                normalized_name,
                existing_institutions,
                limit=5
            )
            
            if fuzzy_matches:
                validation_result['has_fuzzy_duplicate'] = True
                validation_result['fuzzy_matches'] = [
                    {'name': name, 'score': score}
                    for name, score in fuzzy_matches
                ]
                validation_result['warnings'].append(
                    f"Found {len(fuzzy_matches)} similar institution(s) in the database"
                )
        
        # Check if name is too short
        if len(normalized_name) < 2:
            validation_result['errors'].append("Institution name is too short")
            validation_result['is_valid'] = False
        
        # Check for invalid characters
        if normalized_name.strip() == "":
            validation_result['errors'].append("Institution name cannot be empty")
            validation_result['is_valid'] = False
        
        return validation_result




        
    def validate_bulk_entries(
        self,
        df: pd.DataFrame,
        existing_institutions: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Validate multiple institution entries from a CSV, takes in a df of new institutions and compares to existing, returns as a df with the results added
        
        """
        validation_results = []
        
        for idx, row in df.iterrows():
            institution_name = row.get('institution_cpi', '') or row.get('institution_name', '')
            
            if not institution_name:
                validation_results.append({
                    'row_number': idx + 1,
                    'is_valid': False,
                    'status': 'ERROR',
                    'message': 'Institution name is missing'
                })
                continue
            
            result = self.validate_institution_entry(institution_name, existing_institutions)
            
            if result['has_exact_duplicate']:
                status = 'DUPLICATE'
                exact_match = result['exact_match']
                if 'institution_cpi' in exact_match:
                    # Match found in institution table
                    message = f"Exact duplicate in institution table: {exact_match['institution_cpi']}"
                elif 'institution_original' in exact_match:
                    # Match found in standardization table
                    message = f"Exact duplicate in standardization table: {exact_match['institution_original']}"
                else:
                    message = "Exact duplicate found"
            elif result['has_fuzzy_duplicate']:
                status = 'WARNING'
                best_match = result['fuzzy_matches'][0]
                message = f"Similar to: {best_match['name']} ({best_match['score']}% match)"
            elif not result['is_valid']:
                status = 'ERROR'
                message = '; '.join(result['errors'])
            else:
                status = 'OK'
                message = 'Ready to insert'
            
            validation_results.append({
                'row_number': idx + 1,
                'is_valid': result['is_valid'] and not result['has_exact_duplicate'],
                'status': status,
                'message': message,
                'normalized_name': result['normalized_name']
            })
        
        validation_df = pd.DataFrame(validation_results)
        return pd.concat([df.reset_index(drop=True), validation_df], axis=1)
    
   