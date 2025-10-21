"""
Institution-specific business logic service - OPTIMIZED
Uses cached queries for better performance
"""
from typing import Dict, Optional, List, Any
import uuid
import pandas as pd
from datetime import datetime

from database.queries import QueryService
from database.cached_queries import get_all_institutions_cached
from services.validation_service import ValidationService
from services.enrichment_service import EnrichmentService
from services.standardization_service import StandardizationService
from services.audit_service import AuditService
from utils.text_processing import TextProcessor
from config import CURRENT_YEAR


class InstitutionService:
    """Handles all institution-related operations"""
    
    def __init__(self):
        self.query_service = QueryService()
        self.validation_service = ValidationService()
        self.enrichment_service = EnrichmentService()
        self.standardization_service = StandardizationService()
        self.audit_service = AuditService()
    
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
        user: str = "system"
    ) -> Dict[str, Any]:
        """
        Create a new institution with validation and enrichment
        
        Args:
            institution_name: Name of the institution
            institution_type_layer1: Private/Public
            institution_type_layer2: Category (Funds, Corporation, etc.)
            institution_type_layer3: Specific type
            country_sub: Primary operating country
            country_parent: HQ country
            double_counting_risk: Double counting risk indicator
            contact_info: Contact information
            comments: Additional comments
            user: User creating the institution
            
        Returns:
            Dictionary with creation result and metadata
        """
        result = {
            'success': False,
            'institution_id': None,
            'institution_name': None,
            'validation': None,
            'suggestions': None,
            'message': ''
        }
        
        # Get existing institutions from cache - FAST
        existing_institutions = get_all_institutions_cached()
        
        # Validate the entry
        validation = self.validation_service.validate_institution_entry(
            institution_name,
            existing_institutions
        )
        result['validation'] = validation
        
        # Check for exact duplicates
        if validation['has_exact_duplicate']:
            result['message'] = f"Institution already exists: {validation['exact_match']['institution_cpi']}"
            return result
        
        # # Check for standardization mapping
        # should_standardize, standardized_name = self.standardization_service.should_use_standardized_name(
        #     institution_name,
        #     existing_institutions
        # )
        
        # if should_standardize:
        #     final_name = standardized_name
        #     result['message'] += f"Mapped to standardized name: {standardized_name}. "
        # else:
        final_name = validation['normalized_name']
        
        # Get suggestions if fields are missing
        suggestions = None
        if not all([institution_type_layer1, institution_type_layer2, institution_type_layer3]):
            suggestions = self.enrichment_service.suggest_institution_metadata(final_name)
            result['suggestions'] = suggestions
            
            # Use suggestions if fields are empty
            if not institution_type_layer1 and suggestions.get('institution_type_layer1'):
                institution_type_layer1 = suggestions['institution_type_layer1']
            if not institution_type_layer2 and suggestions.get('institution_type_layer2'):
                institution_type_layer2 = suggestions['institution_type_layer2']
            if not institution_type_layer3 and suggestions.get('institution_type_layer3'):
                institution_type_layer3 = suggestions['institution_type_layer3']
            if not country_sub and suggestions.get('country_sub'):
                country_sub = suggestions['country_sub']
            if not country_parent and suggestions.get('country_parent'):
                country_parent = suggestions['country_parent']
        
        # Generate institution ID and short name
        institution_id = str(uuid.uuid4())
        institution_short = TextProcessor.generate_short_name(final_name)
        
        # Prepare data for insertion
        institution_data = {
            'id_institution': institution_id,
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
            'comments': comments
        }
        
        # Remove None values
        institution_data = {k: v for k, v in institution_data.items() if v is not None}
        
        # Insert into database
        success = self.query_service.insert_institution(institution_data)
        
        if success:
            # Log the creation
            self.audit_service.log_insert('institution', institution_id, institution_data, user)
            
            result['success'] = True
            result['institution_id'] = institution_id
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
        Create multiple institutions from a DataFrame
        
        Args:
            df: DataFrame with institution data
            user: User performing the bulk operation
            
        Returns:
            Dictionary with bulk creation results
        """
        result = {
            'total_rows': len(df),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'details': [],
            'summary': None
        }
        
        # Get existing institutions from cache - FAST
        existing_institutions = get_all_institutions_cached()
        
        # Validate all entries first
        validated_df = self.validation_service.validate_bulk_entries(df, existing_institutions)
        
        # Process each row
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
                # Attempt to create institution
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
                    user=user
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
        
        # Create summary
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
        """
        Search for institutions by name prefix
        
        Args:
            query: Search query
            limit: Maximum results
            
        Returns:
            DataFrame with matching institutions
        """
        return self.query_service.search_institutions_by_prefix(query, limit)
    
    def get_institution_suggestions(
        self,
        institution_name: str
    ) -> Dict[str, Any]:
        """
        Get enrichment suggestions for an institution
        
        Args:
            institution_name: Name of the institution
            
        Returns:
            Dictionary with suggestions and research links
        """
        suggestions = self.enrichment_service.suggest_institution_metadata(institution_name)
        research_links = self.enrichment_service.get_research_links(institution_name)
        
        return {
            'suggestions': suggestions,
            'research_links': research_links
        }