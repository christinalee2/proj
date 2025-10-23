import re
import unicodedata
from typing import Optional
import pandas as pd


class TextProcessor:
    """General functions for text normalization"""
    
    @staticmethod
    def normalize_institution_name(name: str) -> str:
        """
        Normalize institution name by removing accents, standardizing whitespace, and converting to a consistent format
        
        Args:
            name: Original institution name
            
        Output:
            Normalized institution name
        """
        if not name:
            return ""
        
        name = name.strip()
        
        name = TextProcessor.remove_accents(name)

        name = re.sub(r'\s+', ' ', name)
        
        # Remove special characters that might cause issues
        # Keep alphanumeric, spaces, hyphens
        name = re.sub(r'[^\w\s\-.,&()\']', '', name)
        
        return name
    
    @staticmethod
    def remove_accents(text: str) -> str:
        """
        Remove accents and diacritical marks
        
        Args:
            text: Input text
            
        Output:
            Text without accents 
        """
        if not text:
            return ""
        
        nfd = unicodedata.normalize('NFD', text)
        
        without_accents = ''.join(
            char for char in nfd
            if unicodedata.category(char) != 'Mn'
        )
        
        return unicodedata.normalize('NFC', without_accents)
    
    @staticmethod
    def extract_suffix(institution_name: str) -> Optional[str]:
        """
        Extract common business suffixes from institution name to try to match without (i.e. 123Venture should match to 123Venture SA), can add to this list as needed
        
        Args:
            institution_name: Full institution name
            
        Returns:
            Extracted suffix in lowercase, or None if no common suffix found
        """
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
    def generate_short_name(institution_name: str, max_length: int = 50) -> str:
        """
        Generate a shortened version of institution name, maybe we don't need this? just some general shortening/acronym production
        
        Args:
            institution_name: Full institution name
            max_length: Maximum length for short name
            
        Output:
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
        

        return institution_name[:max_length-3] + "..."
    

