"""
Generic table service that works with any table configuration
Handles CRUD operations, validation, and bulk uploads for any table
"""
from typing import Dict, List, Optional, Any, Tuple
import uuid
import pandas as pd
from datetime import datetime

from database.queries import QueryService
from database.cached_queries import get_table_data_cached
from table_configs import TableConfig, get_table_config
from utils.text_processing import TextProcessor


class GenericTableService:
    """Generic service for any table operations"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.config = get_table_config(table_name)
        if not self.config:
            raise ValueError(f"No configuration found for table: {table_name}")
        
        self.query_service = QueryService()
    
    def validate_entry(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single entry against table configuration
        
        Args:
            data: Dictionary of field values
            
        Returns:
            Dictionary with validation results
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'normalized_data': {}
        }
        
        # Check required fields
        for field_name in self.config.required_fields:
            value = data.get(field_name)
            if not value or str(value).strip() == '':
                result['valid'] = False
                result['errors'].append(f"Missing required field: {field_name}")
        
        # Validate each field according to its configuration
        for field_config in self.config.fields:
            value = data.get(field_config.name)
            
            if value is not None and value != '':
                # Run custom validation if available
                if field_config.validation_fn:
                    if not field_config.validation_fn(value):
                        result['valid'] = False
                        result['errors'].append(f"Invalid value for {field_config.display_name}: {value}")
                
                # Normalize text fields
                if field_config.field_type == 'text' and isinstance(value, str):
                    if field_config.name in ['institution_cpi', 'country_cpi', 'sector', 'instrument_type']:
                        # Normalize names for key fields
                        result['normalized_data'][field_config.name] = TextProcessor.normalize_institution_name(value)
                    else:
                        result['normalized_data'][field_config.name] = value.strip()
                else:
                    result['normalized_data'][field_config.name] = value
            else:
                result['normalized_data'][field_config.name] = None if not field_config.required else value
        
        return result
    
    def check_duplicates(self, data: Dict[str, Any], existing_data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Check for duplicate entries
        
        Args:
            data: New entry data
            existing_data: Optional existing table data (will fetch if not provided)
            
        Returns:
            Dictionary with duplicate check results
        """
        if existing_data is None:
            existing_data = get_table_data_cached(self.table_name)
        
        result = {
            'has_duplicates': False,
            'exact_matches': [],
            'similar_matches': []
        }
        
        if existing_data.empty or not self.config.duplicate_check_fields:
            return result
        
        # Check exact duplicates based on configured fields
        for _, row in existing_data.iterrows():
            is_duplicate = True
            for field_name in self.config.duplicate_check_fields:
                new_value = data.get(field_name, '')
                existing_value = row.get(field_name, '')
                
                # Normalize for comparison
                if isinstance(new_value, str) and isinstance(existing_value, str):
                    new_norm = TextProcessor.normalize_institution_name(new_value).lower()
                    existing_norm = TextProcessor.normalize_institution_name(existing_value).lower()
                    if new_norm != existing_norm:
                        is_duplicate = False
                        break
                elif new_value != existing_value:
                    is_duplicate = False
                    break
            
            if is_duplicate:
                result['has_duplicates'] = True
                result['exact_matches'].append(dict(row))
        
        return result
    
    def create_entry(self, data: Dict[str, Any], user: str = "system") -> Dict[str, Any]:
        """
        Create a new entry in the table
        
        Args:
            data: Entry data
            user: User creating the entry
            
        Returns:
            Dictionary with creation results
        """
        result = {
            'success': False,
            'entry_id': None,
            'message': '',
            'validation_errors': []
        }
        
        # Validate entry
        validation = self.validate_entry(data)
        if not validation['valid']:
            result['validation_errors'] = validation['errors']
            result['message'] = f"Validation failed: {'; '.join(validation['errors'])}"
            return result
        
        # Check for duplicates
        duplicate_check = self.check_duplicates(validation['normalized_data'])
        if duplicate_check['has_duplicates']:
            result['message'] = "Entry already exists"
            return result
        
        # Generate primary key if needed
        normalized_data = validation['normalized_data'].copy()
        if self.config.primary_key_field not in normalized_data or not normalized_data[self.config.primary_key_field]:
            if self.config.primary_key_field.startswith('id_'):
                normalized_data[self.config.primary_key_field] = str(uuid.uuid4())
            else:
                # For non-UUID keys, try to use a meaningful value
                first_required = self.config.required_fields[0]
                base_value = normalized_data.get(first_required, 'unknown')
                normalized_data[self.config.primary_key_field] = str(base_value).lower().replace(' ', '_')
        
        # Add metadata fields if they exist in the table
        if 'last_verified' in [f.name for f in self.config.fields]:
            normalized_data['last_verified'] = datetime.now().year
        
        if 'year_partition' in [f.name for f in self.config.fields]:
            normalized_data['year_partition'] = str(normalized_data.get('year', datetime.now().year))
        
        # Insert into database
        success = self.query_service.execute_insert(self.table_name, normalized_data)
        
        if success:
            result['success'] = True
            result['entry_id'] = normalized_data[self.config.primary_key_field]
            result['message'] = 'Entry created successfully'
        else:
            result['message'] = 'Failed to insert into database'
        
        return result
    
    def bulk_create_entries(self, df: pd.DataFrame, user: str = "system") -> Dict[str, Any]:
        """
        Create multiple entries from DataFrame
        
        Args:
            df: DataFrame with entry data
            user: User performing bulk operation
            
        Returns:
            Dictionary with bulk creation results
        """
        result = {
            'total_rows': len(df),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'details': [],
            'summary': ''
        }
        
        # Get existing data once for all duplicate checks
        existing_data = get_table_data_cached(self.table_name)
        
        for idx, row in df.iterrows():
            row_data = row.to_dict()
            row_result = {
                'row_number': idx + 1,
                'status': 'pending',
                'message': ''
            }
            
            # Add primary identifier to result
            identifier_field = self.config.required_fields[0]
            row_result['identifier'] = row_data.get(identifier_field, f'Row {idx + 1}')
            
            try:
                creation_result = self.create_entry(row_data, user)
                
                if creation_result['success']:
                    row_result['status'] = 'success'
                    row_result['entry_id'] = creation_result['entry_id']
                    result['successful'] += 1
                else:
                    row_result['status'] = 'failed'
                    row_result['message'] = creation_result['message']
                    result['failed'] += 1
                    
            except Exception as e:
                row_result['status'] = 'failed'
                row_result['message'] = str(e)
                result['failed'] += 1
            
            result['details'].append(row_result)
        
        result['summary'] = (
            f"Processed {result['total_rows']} rows: "
            f"{result['successful']} successful, "
            f"{result['failed']} failed"
        )
        
        return result
    
    def get_dropdown_options(self) -> Dict[str, List[str]]:
        """
        Get dropdown options for select fields
        
        Returns:
            Dictionary mapping field names to option lists
        """
        options = {}
        
        for field_config in self.config.fields:
            if field_config.field_type == 'select' and field_config.options:
                if not field_config.options:  # Empty list means populate from related table
                    if field_config.name in ['country_sub', 'country_parent', 'country_cpi']:
                        # Get countries from geography table
                        try:
                            geo_data = get_table_data_cached('geography')
                            if not geo_data.empty and 'country_cpi' in geo_data.columns:
                                countries = sorted(geo_data['country_cpi'].dropna().unique())
                                options[field_config.name] = [''] + countries
                            else:
                                options[field_config.name] = ['']
                        except:
                            options[field_config.name] = ['']
                    else:
                        options[field_config.name] = ['']
                else:
                    options[field_config.name] = field_config.options
        
        return options


class GenericTableServiceFactory:
    """Factory for creating table services"""
    
    _services = {}
    
    @classmethod
    def get_service(cls, table_name: str) -> GenericTableService:
        """Get or create a service for the specified table"""
        if table_name not in cls._services:
            cls._services[table_name] = GenericTableService(table_name)
        return cls._services[table_name]