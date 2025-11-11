from typing import Dict, Optional, List, Any, Tuple
import uuid
import pandas as pd
from database.queries import QueryService
from database.cached_queries import get_table_data_cached
from utils.fuzzy_matching import get_fitted_matcher
from utils.text_processing import TextProcessor
from config import CURRENT_YEAR


class HierarchyService:
    """Handles hierarchy table operations to match parent-child institution relationships"""
    
    def __init__(self):
        self.query_service = QueryService()
    
    def search_institution_for_hierarchy(self, query: str, existing_institutions: pd.DataFrame, limit: int = 10) -> List[Tuple[str, str, float]]:
        """
        Search for institutions to use in hierarchy relationships with fuzzy matching, basically same process ot how institutions are checked for the main entry form
        
        Args:
            query: institution name typed in
            existing_institutions: current institutions
            limit: Maximum number of results
            
        Output:
            List of tuples (institution_name, institution_id, score)
        """
        if existing_institutions.empty or query.strip() == "":
            return []
        
        # First try exact matches
        exact_matches = []
        query_lower = query.lower().strip()
        
        for _, row in existing_institutions.iterrows():
            inst_name = str(row.get('institution_cpi', '')).strip()
            if inst_name.lower() == query_lower:
                exact_matches.append((
                    inst_name, 
                    str(row.get('id_institution_cpi', '')), 
                    100.0
                ))
        
        if exact_matches:
            return exact_matches[:limit]
        
        # Then try fuzzy matching
        try:
            matcher = get_fitted_matcher(existing_institutions, threshold=0.6)
            fuzzy_matches = matcher.find_similar_institutions(
                query=query,
                institution_df=existing_institutions,
                limit=limit,
                tfidf_top_k=50
            )
            

            results = []
            for name, score in fuzzy_matches:
                # Find the corresponding ID for the parent/child, this will be input into the hierarchy under id_parent/child
                matching_row = existing_institutions[
                    existing_institutions['institution_cpi'].str.lower() == name.lower()
                ]
                if not matching_row.empty:
                    inst_id = str(matching_row.iloc[0].get('id_institution_cpi', ''))
                    results.append((name, inst_id, score))
            
            return results
            
        except Exception as e:
            print(f"Error in hierarchy institution search: {e}")
            return []



            
    def validate_hierarchy_entry(
        self,
        parent_institution: str,
        child_institution: str,
        percent_ownership: Optional[float] = None,
        existing_institutions: Optional[pd.DataFrame] = None,
        existing_hierarchy: Optional[pd.DataFrame] = None
    ) -> Dict[str, Any]:
        """
        Validate a hierarchy entry and outputs dict to be inserted

        """
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'parent_id': None,
            'child_id': None,
            'normalized_parent': None,
            'normalized_child': None
        }
        
        if existing_institutions is None:
            existing_institutions = get_table_data_cached('institution', limit=None)
        if existing_hierarchy is None:
            existing_hierarchy = get_table_data_cached('hierarchy', limit=None)
        
        # Validate parent institution exists so that the correct id_insittution_cpi will be added
        parent_match = self._find_institution_by_name(parent_institution, existing_institutions)
        if not parent_match:
            result['errors'].append(f"Parent institution '{parent_institution}' not found in institution table")
            result['is_valid'] = False
        else:
            result['parent_id'] = parent_match['id']
            result['normalized_parent'] = parent_match['name']
        
        # Validate child institution exists
        child_match = self._find_institution_by_name(child_institution, existing_institutions)
        if not child_match:
            result['errors'].append(f"Child institution '{child_institution}' not found in institution table")
            result['is_valid'] = False
        else:
            result['child_id'] = child_match['id']
            result['normalized_child'] = child_match['name']
        
        # Check if parent and child are the same, not a good match if so
        if parent_institution.lower().strip() == child_institution.lower().strip():
            result['errors'].append("Parent and child institutions cannot be the same")
            result['is_valid'] = False
        
        if percent_ownership is not None:
            if not isinstance(percent_ownership, (int, float)):
                result['errors'].append("Ownership percentage must be a number")
                result['is_valid'] = False
            elif percent_ownership < 0 or percent_ownership > 1:
                result['errors'].append("Ownership percentage must be between 0.0 and 1.0")
                result['is_valid'] = False
        
        # Check for duplicate relationships
        if result['parent_id'] and result['child_id'] and not existing_hierarchy.empty:
            duplicate_check = existing_hierarchy[
                (existing_hierarchy.get('id_parent', '') == result['parent_id']) &
                (existing_hierarchy.get('id_child', '') == result['child_id'])
            ]
            if not duplicate_check.empty:
                result['warnings'].append("This relationship already exists in the hierarchy table")
        
        return result



    
    def _find_institution_by_name(self, name: str, institutions_df: pd.DataFrame) -> Optional[Dict[str, str]]:
        """Find institution by name in the institutions DataFrame"""
        if institutions_df.empty or not name:
            return None
        
        name_normalized = name.lower().strip()
        
        for _, row in institutions_df.iterrows():
            inst_name = str(row.get('institution_cpi', '')).lower().strip()
            if inst_name == name_normalized:
                return {
                    'id': str(row.get('id_institution_cpi', '')),
                    'name': str(row.get('institution_cpi', ''))
                }
        
        return None




    
    def create_hierarchy_entry(
        self,
        parent_institution: str,
        child_institution: str,
        percent_ownership: Optional[float] = None,
        relationship_type: Optional[str] = None,
        user: str = "system",
        existing_institutions: Optional[pd.DataFrame] = None, 
        existing_hierarchy: Optional[pd.DataFrame] = None  
    ) -> Dict[str, Any]:
        """
        Create a new hierarchy entry
        
        Args:
            parent_institution: Name of parent institution
            child_institution: Name of child institution
            percent_ownership: Ownership percentage (defaults to 1.0 if not specified)
            relationship_type: Type of relationship
            user: User creating the entry
            
        Returns:
            Dictionary with creation result
        """
        result = {
            'success': False,
            'hierarchy_id': None,
            'message': '',
            'validation': None
        }
        
        # Defaults to 1
        if percent_ownership is None:
            percent_ownership = 1.0
        
        validation = self.validate_hierarchy_entry(
            parent_institution, child_institution, percent_ownership, existing_institutions, existing_hierarchy
        )
        result['validation'] = validation
        
        if not validation['is_valid']:
            result['message'] = '; '.join(validation['errors'])
            return result
        
        # Determine controlling status, user can either click or will go if over 50%
        is_controlling = percent_ownership > 0.5 if percent_ownership is not None else False
        
        hierarchy_data = {
            'id_parent': int(validation['parent_id']),
            'parent_institution': validation['normalized_parent'],
            'id_child': int(validation['child_id']),
            'child_institution': validation['normalized_child'],
            'percent_ownership': float(percent_ownership),
            'is_controlling_institution': is_controlling,
            'relationship_type': relationship_type or '',
            'created_by': user,
            'created_at': CURRENT_YEAR
        }
        
        hierarchy_data = {k: v for k, v in hierarchy_data.items() if v is not None}
        
        success = self.query_service.execute_insert('hierarchy', hierarchy_data)
        
        if success:
            result['success'] = True
            result['message'] = 'Hierarchy relationship created successfully'
        else:
            result['message'] = 'Failed to insert hierarchy relationship into database'
        
        return result





    
    def get_next_hierarchy_id(self) -> int:
        """Get the next available hierarchy ID"""
        try:
            query = "SELECT MAX(id_hierarchy) as max_id FROM hierarchy"
            result = self.query_service.execute_query(query)
            if not result.empty and result.iloc[0]['max_id'] is not None:
                return int(result.iloc[0]['max_id']) + 1
            else:
                return 1
        except Exception as e:
            print(f"Error getting next hierarchy ID: {e}")
            return 1





            
    def get_institution_relationships(self, institution_name: str) -> Dict[str, List[Dict]]:
        """
        Get all relationships (parent and child) for an institution
        
        Args:
            institution_name: Name of the institution
            
        Returns:
            Dictionary with 'as_parent' and 'as_child' relationship lists
        """
        try:
            hierarchy_df = get_table_data_cached('hierarchy', limit=None)
            if hierarchy_df.empty:
                return {'as_parent': [], 'as_child': []}
            
            name_normalized = institution_name.lower().strip()
            
            # Find relationships where institution is parent
            as_parent = []
            parent_matches = hierarchy_df[
                hierarchy_df['parent_institution'].str.lower().str.strip() == name_normalized
            ]
            for _, row in parent_matches.iterrows():
                as_parent.append({
                    'related_institution': row.get('child_institution', ''),
                    'relationship_type': row.get('relationship_type', ''),
                    'percent_ownership': row.get('percent_ownership', 0),
                    'is_controlling': row.get('is_controlling_institution', False)
                })
            
            # Find relationships where institution is child
            as_child = []
            child_matches = hierarchy_df[
                hierarchy_df['child_institution'].str.lower().str.strip() == name_normalized
            ]
            for _, row in child_matches.iterrows():
                as_child.append({
                    'related_institution': row.get('parent_institution', ''),
                    'relationship_type': row.get('relationship_type', ''),
                    'percent_ownership': row.get('percent_ownership', 0),
                    'is_controlling': row.get('is_controlling_institution', False)
                })
            
            return {'as_parent': as_parent, 'as_child': as_child}
            
        except Exception as e:
            print(f"Error getting institution relationships: {e}")
            return {'as_parent': [], 'as_child': []}






    def create_hierarchy_entry_direct(
        self,
        parent_id: int,
        parent_name: str,
        child_id: int,
        child_name: str,
        percent_ownership: Optional[float] = None,
        relationship_type: Optional[str] = None,
        user: str = "system"
    ) -> Dict[str, Any]:
        """
        Create a hierarchy entry directly without validation (for newly created institutions), this is necessary to avoid having to reload the full institution table which takes a long time
        
        Args:
            parent_id: ID of parent institution
            parent_name: Name of parent institution
            child_id: ID of child institution
            child_name: Name of child institution
            percent_ownership: Ownership percentage (defaults to 1.0)
            relationship_type: Type of relationship
            user: User creating the entry
            
        Returns:
            Dictionary with creation result
        """
        result = {
            'success': False,
            'hierarchy_id': None,
            'message': ''
        }
        
        if percent_ownership is None:
            percent_ownership = 1.0
        
        # Basic validation
        if parent_id == child_id:
            result['message'] = "Parent and child institutions cannot be the same"
            return result
        
        if percent_ownership < 0 or percent_ownership > 1:
            result['message'] = "Ownership percentage must be between 0.0 and 1.0"
            return result
        
        is_controlling = percent_ownership > 0.5

        next_hierarchy_id = self.get_next_hierarchy_id()
        
        hierarchy_data = {
            'id_hierarchy': next_hierarchy_id, 
            'id_parent': int(parent_id),
            'parent_institution': parent_name,
            'id_child': int(child_id),
            'child_institution': child_name,
            'percent_ownership': float(percent_ownership),
            'is_controlling_institution': is_controlling,
            'relationship_type': relationship_type or '',
            'created_by': user,
            'created_at': CURRENT_YEAR
        }
        
        hierarchy_data = {k: v for k, v in hierarchy_data.items() if v is not None}
        
        success = self.query_service.execute_insert('hierarchy', hierarchy_data)
        
        if success:
            result['success'] = True
            result['message'] = 'Hierarchy relationship created successfully'
        else:
            result['message'] = 'Failed to insert hierarchy relationship into database'
        
        return result