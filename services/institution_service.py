from typing import Dict, Optional, List, Any
import pandas as pd
from datetime import datetime

from database.queries import QueryService
from database.cached_queries import get_all_institutions_cached
from services.validation_service import ValidationService
from services.standardization_service import StandardizationService
from utils.text_processing import TextProcessor
from config import CURRENT_YEAR


class InstitutionService:
    """For all institution-related stuff, all other tables will follow a generic structure"""
    
    def __init__(self):
        self.query_service = QueryService()
        self.validation_service = ValidationService()
        self.standardization_service = StandardizationService()
    
    def create_institution(
        self,
        institution_name: str,
        institution_type_layer1: Optional[str] = None,
        institution_type_layer2: Optional[str] = None,
        institution_type_layer3: Optional[str] = None,
        country_sub: Optional[str] = None,
        country_parent: Optional[str] = None,
        double_counting_risk: Optional[str] = None,
        contact_info: Optional[str] = None,
        comments: Optional[str] = None,
        user: str = "system",
        last_verified: Optional[int] = None,
        created_by: Optional[str] = None,
        created_at: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a new institution with validation and enrichment - important to ensure types match parquet file
        Takes in all the institution form variables and outputs a dict of the results 
        """
        result = {
            'success': False,
            'institution_id': None,
            'institution_name': None,
            'validation': None,
            'suggestions': None,
            'message': ''
        }
        
        
        final_name = TextProcessor.normalize_institution_name(institution_name)
        institution_short = TextProcessor.generate_short_name(final_name)
        
        institution_data = {
            # 'id_institution_cpi': institution_id,
            'institution_cpi': final_name,
            'institution_cpi_short': institution_short,
            'last_verified': CURRENT_YEAR,
            'institution_type_layer1': institution_type_layer1,
            'institution_type_layer2': institution_type_layer2,
            'institution_type_layer3': institution_type_layer3,
            'country_sub': country_sub,
            'country_parent': country_parent,
            'double_counting_risk': double_counting_risk,
            'contact_info': contact_info,
            'comments': comments,
            'created_at': CURRENT_YEAR,
            'created_by': user
        }
        
        institution_data = {k: v for k, v in institution_data.items() if v is not None}
        
        # success = self.query_service.insert_institution(institution_data)
        success = self.query_service.execute_insert('institution', institution_data)
        
        if success:
            
            result['success'] = True
            result['institution_name'] = final_name
            result['message'] += 'Institution created successfully.'
        else:
            result['message'] = 'Failed to insert institution into database.'
        
        return result
    
    def bulk_create_institutions(
        self,
        df: pd.DataFrame,
        user: str = "system"
    ) -> Dict[str, Any]:
        """
        Create multiple institutions from a DataFrame, needs to
        
        Args:
            df: DataFrame with institution data
            user: User performing the bulk operation
            
        Output:
            Dict with bulk creation results
        """
        result = {
            'total_rows': len(df),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'details': [],
            'summary': None
        }
        
        existing_institutions = get_all_institutions_cached()
        
        validated_df = self.validation_service.validate_bulk_entries(df, existing_institutions)
        
        for idx, row in validated_df.iterrows():
            row_result = {
                'row_number': idx + 1,
                'institution_name': row.get('institution_cpi') or row.get('institution_name', ''),
                'status': 'pending'
            }
            
            if row.get('status') == 'DUPLICATE':
                row_result['status'] = 'skipped'
                row_result['message'] = 'Duplicate entry'
                result['skipped'] += 1
            elif row.get('status') == 'ERROR':
                row_result['status'] = 'failed'
                row_result['message'] = row.get('message', 'Validation error')
                result['failed'] += 1
            else:
                creation_result = self.create_institution(
                    institution_name=row.get('institution_cpi') or row.get('institution_name', ''),
                    institution_type_layer1=row.get('institution_type_layer1'),
                    institution_type_layer2=row.get('institution_type_layer2'),
                    institution_type_layer3=row.get('institution_type_layer3'),
                    country_sub=row.get('country_sub'),
                    country_parent=row.get('country_parent'),
                    double_counting_risk=row.get('double_counting_risk'),
                    contact_info=row.get('contact_info'),
                    comments=row.get('comments'),
                    user=user,
                    last_verified=row.get('last_verified'),
                    created_by=row.get('created_by') or user,
                    created_at=CURRENT_YEAR
                )
                
                if creation_result['success']:
                    row_result['status'] = 'success'
                    row_result['institution_id'] = creation_result['institution_id']
                    row_result['message'] = 'Created successfully'
                    result['successful'] += 1
                else:
                    row_result['status'] = 'failed'
                    row_result['message'] = creation_result['message']
                    result['failed'] += 1
            
            result['details'].append(row_result)
        
        result['summary'] = (
            f"Processed {result['total_rows']} rows: "
            f"{result['successful']} successful, "
            f"{result['failed']} failed, "
            f"{result['skipped']} skipped (duplicates)"
        )
        
        return result
    
    def search_institutions(
        self,
        query: str,
        limit: int = 20
    ) -> pd.DataFrame:

        return self.query_service.search_institutions_by_prefix(query, limit)
    