from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid
import pandas as pd
from database.queries import QueryService
from config import CURRENT_YEAR


class AuditService:
    """Handles audit logging for all data changes"""
    
    def __init__(self):
        self.query_service = QueryService()
    
    def log_insert(
        self,
        table_name: str,
        record_id: str,
        data: Dict[str, Any],
        user: str = "system"
    ) -> bool:
        """
        Log an INSERT operation
        
        Args:
            table_name: Name of the table
            record_id: ID of the inserted record
            data: Dictionary of inserted data
            user: User who performed the operation
            
        Returns:
            True if successful, False otherwise
        """
        try:
            for field_name, new_value in data.items():
                audit_entry = {
                    'id': str(uuid.uuid4()),
                    'table_name': table_name,
                    'operation': 'INSERT',
                    'record_id': record_id,
                    'field_name': field_name,
                    'old_value': None,
                    'new_value': str(new_value) if new_value is not None else None,
                    'changed_by': user,
                    'changed_at': CURRENT_YEAR,
                    'notes': f'New record inserted into {table_name}'
                }
                
                self.query_service.insert_audit_log(audit_entry)
            
            return True
        except Exception as e:
            print(f"Error logging insert: {e}")
            return False
    
    def log_update(
        self,
        table_name: str,
        record_id: str,
        field_name: str,
        old_value: Any,
        new_value: Any,
        user: str = "system",
        notes: Optional[str] = None
    ) -> bool:
        """
        Log an UPDATE operation
        
        Args:
            table_name: Name of the table
            record_id: ID of the updated record
            field_name: Name of the field that was updated
            old_value: Previous value
            new_value: New value
            user: User who performed the operation
            notes: Optional notes about the change
            
        Returns:
            True if successful, False otherwise
        """
        try:
            audit_entry = {
                'id': str(uuid.uuid4()),
                'table_name': table_name,
                'operation': 'UPDATE',
                'record_id': record_id,
                'field_name': field_name,
                'old_value': str(old_value) if old_value is not None else None,
                'new_value': str(new_value) if new_value is not None else None,
                'changed_by': user,
                'changed_at': CURRENT_YEAR,
                'notes': notes or f'Updated {field_name} in {table_name}'
            }
            
            return self.query_service.insert_audit_log(audit_entry)
        except Exception as e:
            print(f"Error logging update: {e}")
            return False
    
    def log_bulk_insert(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        user: str = "system"
    ) -> bool:
        """
        Log multiple INSERT operations from bulk upload
        
        Args:
            table_name: Name of the table
            records: List of record dictionaries
            user: User who performed the operation
            
        Returns:
            True if successful, False otherwise
        """
        try:
            for record in records:
                record_id = record.get('id_institution') or record.get('id') or str(uuid.uuid4())
                self.log_insert(table_name, record_id, record, user)
            
            return True
        except Exception as e:
            print(f"Error logging bulk insert: {e}")
            return False
    
    def get_audit_history(
        self,
        table_name: Optional[str] = None,
        record_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Retrieve audit history with optional filters
        
        Args:
            table_name: Filter by table name
            record_id: Filter by record ID
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Maximum number of records to return
            
        Returns:
            DataFrame with audit history
        """
        try:
            query = "SELECT * FROM audit_log WHERE 1=1"
            params = []
            
            if table_name:
                query += " AND table_name = ?"
                params.append(table_name)
            
            if record_id:
                query += " AND record_id = ?"
                params.append(record_id)
            
            if start_date:
                query += " AND changed_at >= ?"
                params.append(start_date.isoformat())
            
            if end_date:
                query += " AND changed_at <= ?"
                params.append(end_date.isoformat())
            
            query += f" ORDER BY changed_at DESC LIMIT {limit}"
            
            return self.query_service.execute_query(query, tuple(params) if params else None)
        except Exception as e:
            print(f"Error retrieving audit history: {e}")
            return pd.DataFrame()
    
    def get_record_history(self, table_name: str, record_id: str) -> pd.DataFrame:
        """
        Get complete change history for a specific record
        
        Args:
            table_name: Name of the table
            record_id: ID of the record
            
        Returns:
            DataFrame with chronological change history
        """
        return self.get_audit_history(table_name=table_name, record_id=record_id, limit=1000)
    
    def get_recent_changes(self, days: int = 7, limit: int = 100) -> pd.DataFrame:
        """
        Get recent changes across all tables
        
        Args:
            days: Number of days to look back
            limit: Maximum number of records
            
        Returns:
            DataFrame with recent changes
        """
        start_date = datetime.now() - timedelta(days=days)
        return self.get_audit_history(start_date=start_date, limit=limit)


from datetime import timedelta