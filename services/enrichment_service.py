"""
Data enrichment service for suggesting institution metadata
"""
from typing import Dict, Optional, List
import json
from openai import OpenAI
from utils.text_processing import TextProcessor
from config import SUFFIX_MAPPINGS, ENRICHMENT_PROMPT_TEMPLATE, OPENAI_API_KEY


class EnrichmentService:
    """Handles automatic enrichment of institution data"""
    
    def __init__(self):
        self.openai_client = None
        if OPENAI_API_KEY:
            try:
                self.openai_client = OpenAI(api_key=OPENAI_API_KEY)
            except Exception as e:
                print(f"Warning: Could not initialize OpenAI client: {e}")
    
    def suggest_institution_metadata(self, institution_name: str) -> Dict[str, Optional[str]]:
        """
        Suggest metadata for an institution based on its name
        
        Args:
            institution_name: Name of the institution
            
        Returns:
            Dictionary with suggested metadata
        """
        suggestions = {
            'institution_type_layer1': None,
            'institution_type_layer2': None,
            'institution_type_layer3': None,
            'country_sub': None,
            'country_parent': None,
            'confidence': 'low',
            'sources': []
        }
        
        # First, try suffix-based matching
        suffix_suggestions = self._suggest_from_suffix(institution_name)
        suggestions.update(suffix_suggestions)
        
        # Then, try GPT enrichment if available
        if self.openai_client:
            gpt_suggestions = self._suggest_from_gpt(institution_name)
            
            # Merge GPT suggestions, giving them higher priority
            for key, value in gpt_suggestions.items():
                if value and key in suggestions:
                    suggestions[key] = value
            
            if gpt_suggestions.get('sources'):
                suggestions['sources'] = gpt_suggestions['sources']
                suggestions['confidence'] = 'high'
            elif suffix_suggestions.get('institution_type_layer1'):
                suggestions['confidence'] = 'medium'
        
        return suggestions
    
    def _suggest_from_suffix(self, institution_name: str) -> Dict[str, Optional[str]]:
        """
        Suggest institution types based on business suffixes and keywords
        
        Args:
            institution_name: Name of the institution
            
        Returns:
            Dictionary with suggested types
        """
        suggestions = {
            'institution_type_layer1': None,
            'institution_type_layer2': None,
            'institution_type_layer3': None
        }
        
        name_lower = institution_name.lower()
        
        # Check suffixes for layer1 (Private/Public)
        for suffix, type_value in SUFFIX_MAPPINGS['layer1'].items():
            if suffix in name_lower:
                suggestions['institution_type_layer1'] = type_value
                break
        
        # Check keywords for layer2 and layer3
        for suffix, type_value in SUFFIX_MAPPINGS['layer2'].items():
            if suffix in name_lower:
                suggestions['institution_type_layer2'] = type_value
                break
        
        for suffix, type_value in SUFFIX_MAPPINGS['layer3'].items():
            if suffix in name_lower:
                suggestions['institution_type_layer3'] = type_value
                break
        
        return suggestions
    
    def _suggest_from_gpt(self, institution_name: str) -> Dict[str, Optional[str]]:
        """
        Use GPT to suggest institution metadata and find sources
        
        Args:
            institution_name: Name of the institution
            
        Returns:
            Dictionary with suggested metadata and sources
        """
        if not self.openai_client:
            return {}
        
        try:
            prompt = ENRICHMENT_PROMPT_TEMPLATE.format(institution_name=institution_name)
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a financial data analyst with expertise in institutional classification."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            # Parse JSON response
            content = response.choices[0].message.content
            
            # Extract JSON from markdown code blocks if present
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            result = json.loads(content)
            
            # Clean up null values
            return {k: v for k, v in result.items() if v is not None}
            
        except Exception as e:
            print(f"Error in GPT enrichment: {e}")
            return {}
    
    def batch_suggest_metadata(
        self,
        institution_names: List[str]
    ) -> List[Dict[str, Optional[str]]]:
        """
        Suggest metadata for multiple institutions
        
        Args:
            institution_names: List of institution names
            
        Returns:
            List of suggestion dictionaries
        """
        suggestions = []
        
        for name in institution_names:
            suggestion = self.suggest_institution_metadata(name)
            suggestion['institution_name'] = name
            suggestions.append(suggestion)
        
        return suggestions
    
    def get_research_links(self, institution_name: str) -> List[Dict[str, str]]:
        """
        Generate useful research links for an institution
        
        Args:
            institution_name: Name of the institution
            
        Returns:
            List of dictionaries with link information
        """
        encoded_name = institution_name.replace(' ', '+')
        
        links = [
            {
                'title': 'Google Search',
                'url': f'https://www.google.com/search?q={encoded_name}'
            },
            {
                'title': 'LinkedIn Company Search',
                'url': f'https://www.linkedin.com/search/results/companies/?keywords={encoded_name}'
            },
            {
                'title': 'Crunchbase',
                'url': f'https://www.crunchbase.com/textsearch?q={encoded_name}'
            },
            {
                'title': 'OpenCorporates',
                'url': f'https://opencorporates.com/companies?q={encoded_name}'
            }
        ]
        
        return links