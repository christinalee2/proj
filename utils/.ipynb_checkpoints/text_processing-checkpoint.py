"""
Text processing and normalization utilities
"""
import re
import unicodedata
from typing import Optional


class TextProcessor:
    """Handles text normalization and processing"""
    
    @staticmethod
    def normalize_institution_name(name: str) -> str:
        """
        Normalize institution name by removing accents, standardizing whitespace,
        and converting to a consistent format
        
        Args:
            name: Original institution name
            
        Returns:
            Normalized institution name
        """
        if not name:
            return ""
        
        # Remove leading/trailing whitespace
        name = name.strip()
        
        # Remove accents and diacritics
        name = TextProcessor.remove_accents(name)
        
        # Standardize whitespace (multiple spaces to single space)
        name = re.sub(r'\s+', ' ', name)
        
        # Remove special characters that might cause issues
        # Keep alphanumeric, spaces, hyphens, and common business punctuation
        name = re.sub(r'[^\w\s\-.,&()\']', '', name)
        
        return name
    
    @staticmethod
    def remove_accents(text: str) -> str:
        """
        Remove accents and diacritical marks from text
        
        Args:
            text: Input text
            
        Returns:
            Text without accents
        """
        if not text:
            return ""
        
        # Normalize to NFD (decomposed form)
        nfd = unicodedata.normalize('NFD', text)
        
        # Filter out combining characters (accents)
        without_accents = ''.join(
            char for char in nfd
            if unicodedata.category(char) != 'Mn'
        )
        
        # Normalize back to NFC (composed form)
        return unicodedata.normalize('NFC', without_accents)
    
    @staticmethod
    def extract_suffix(institution_name: str) -> Optional[str]:
        """
        Extract common business suffix from institution name
        
        Args:
            institution_name: Full institution name
            
        Returns:
            Extracted suffix in lowercase, or None if no common suffix found
        """
        # Common business suffixes
        suffixes = [
            'llc', 'ltd', 'limited', 'inc', 'incorporated', 'corp', 'corporation',
            'gmbh', 'sarl', 'srl', 'pvt', 'pty', 'pte', 'bv', 'nv', 'ag', 'sa',
            'sas', 'ab', 'plc', 'public limited company', 'se', 'oyj', 'spa',
            'l.l.c.', 'l.t.d.', 'p.l.c.', 's.a.', 's.r.l.'
        ]
        
        name_lower = institution_name.lower()
        
        # Check for suffixes at the end of the name
        for suffix in suffixes:
            # Pattern: suffix at end, possibly preceded by comma or space
            pattern = r'[,\s]+' + re.escape(suffix) + r'\.?$'
            if re.search(pattern, name_lower):
                return suffix.replace('.', '')
        
        return None
    
    @staticmethod
    def extract_keywords(institution_name: str) -> list:
        """
        Extract keywords from institution name for matching
        
        Args:
            institution_name: Full institution name
            
        Returns:
            List of keywords
        """
        # Remove common business suffixes and articles
        name = institution_name.lower()
        
        # Remove punctuation
        name = re.sub(r'[^\w\s]', ' ', name)
        
        # Split into words
        words = name.split()
        
        # Remove common stop words
        stop_words = {'the', 'a', 'an', 'and', 'or', 'of', 'at', 'by', 'for', 'in', 'on', 'to'}
        keywords = [w for w in words if w not in stop_words and len(w) > 2]
        
        return keywords
    
    @staticmethod
    def generate_short_name(institution_name: str, max_length: int = 50) -> str:
        """
        Generate a shortened version of institution name
        
        Args:
            institution_name: Full institution name
            max_length: Maximum length for short name
            
        Returns:
            Shortened institution name
        """
        if len(institution_name) <= max_length:
            return institution_name
        
        # Try removing suffix first
        suffix = TextProcessor.extract_suffix(institution_name)
        if suffix:
            # Remove suffix and trim
            pattern = r'[,\s]+' + re.escape(suffix) + r'\.?$'
            short_name = re.sub(pattern, '', institution_name, flags=re.IGNORECASE)
            if len(short_name) <= max_length:
                return short_name.strip()
        
        # If still too long, extract acronym or truncate
        words = institution_name.split()
        if len(words) > 3:
            # Try creating acronym from first letters
            acronym = ''.join(w[0].upper() for w in words if len(w) > 2)
            if 3 <= len(acronym) <= 10:
                return acronym
        
        # Last resort: truncate with ellipsis
        return institution_name[:max_length-3] + "..."
    
    @staticmethod
    def clean_csv_value(value: str) -> str:
        """
        Clean a value from CSV input
        
        Args:
            value: Raw CSV value
            
        Returns:
            Cleaned value
        """
        if pd.isna(value) or value is None:
            return ""
        
        # Convert to string and strip whitespace
        value = str(value).strip()
        
        # Remove quotes if present
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        
        return value


# Import pandas for CSV processing
import pandas as pd