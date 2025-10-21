from typing import Dict, List, Optional, Tuple
import json
import requests
from openai import OpenAI
import os
from dataclasses import dataclass
from datetime import datetime
import pandas as pd


@dataclass
class InstitutionLookupResult:
    """Structured result from institution lookup"""
    institution_name: str
    institution_type_layer1: Optional[str]  # Public/Private
    institution_type_layer2: Optional[str]  # Funds/Corporation/etc
    institution_type_layer3: Optional[str]  # Asset Manager/Bank/etc
    parent_country: Optional[str]  # HQ country
    subsidiary_country: Optional[str]  # Operating country
    confidence_score: float  # 0-1
    sources: List[Dict[str, str]]  # List of source URLs with descriptions
    reasoning: str  # Why the LLM made these choices
    timestamp: str


class InstitutionLookupService:
    """
    Automated institution data lookup using:
    1. Comprehensive suffix-based public/private detection
    2. Serper.dev API for Google search
    3. OpenAI to extract structured data
    4. Institution table country matching
    """
    
    # Public company suffixes (stock exchange listed)
    # From international suffix reference data
    PUBLIC_SUFFIXES = {

    }
    
    # Private company suffixes
    PRIVATE_SUFFIXES = {
        'Ltd', 'LTD', 'Limited', 'Ltd.',
        'LLC', 'L.L.C', 'L.L.C.',
        'Inc', 'Inc.', 'Incorporated',
        'Corp', 'Corp.', 'Corporation',
        'LP', 'L.P', 'L.P.',
        'LLP', 'L.L.P', 'L.L.P.',
        'ULC', 'U.L.C',  # Canada (unlimited liability)
        'Ltda', 'Limitada',  # Latin America
        'S de RL de CV',  # Mexico (private)
        'NA', 'N.A',  # US (national association)
        'GmbH', 'mbH',  # Germany, Switzerland
        'GmbH & Co KG', 'GmbH & Co.KG',  # Germany, Austria
        'ApS',  # Denmark
        'BV', 'B.V', 'B.V.',  # Netherlands
        'Sarl', 'SARL', 'S.A.R.L', 'Sàrl',  # France, Switzerland, etc.
        'SAS', 'S.A.S',  # France (unlisted)
        'SRL', 'S.R.L', 'Srl', 'S.r.l',  # Italy, Romania, etc.
        'SL', 'S.L',  # Spain
        'SNC', 'S.N.C',  # General partnership
        'Scarl', 'S.C.A.R.L',  # Italy
        'SCR de RS SAU',  # Spain
        'eG', 'e.G',  # Switzerland (partnership)
        'FB', 'F.B',  # Germany
        'KG', 'K.G',  # Austria, Germany, Switzerland (limited partnership)
        'KY', 'K.Y',  # Finland (limited partnership)
        'Oy',  # Finland (private)
        'doo', 'd.o.o',  # Bosnia, Croatia, etc.
        'dd',  # Bosnia and Herzegovina
        'zoo', 'z.o.o',  # Poland
        'sro', 'S.R.O',  # Czech Republic, Slovakia
        'Pte Ltd', 'Pte. Ltd.',  # Singapore, Cambodia, Philippines
        'Pty Ltd', 'Pty. Ltd.',  # Australia, South Africa, Namibia
        'Pvt Ltd', 'Pvt. Ltd.',  # India
        'PT', 'P.T',  # Indonesia
        'GK', 'G.K.',  # Japan (private)
        'YK', 'Y.K',  # Japan (private)
        'Co Ltd', 'Co. Ltd.',  # Various Asian countries
        'QSC', 'Q.S.C',  # Qatar
        'JSC',  # Kazakhstan, Ukraine (private)
        'Ltd Sti',  # Turkey, Denmark
        'SAC', 'S.A.C',  # Peru
        'SPE Ltda',  # Brazil
        'SPC', 'S.P.C',  # Japan (special purpose vehicle)
        'PLC', 'plc', 'P.L.C', 'Plc',  # UK, Ireland, Ethiopia, Nigeria
        'PCL', 'P.C.L',  # Thailand
        'Oyj',  # Finland
        'ASA', 'A.S.A',  # Norway
        'AB', 'A.B',  # Sweden
        'AB (publ)', 'AB publ',
        'A/S',  # Denmark
        'AS', 'A.S',  # Turkey (public), Denmark
        'SpA', 'S.p.A',  # Italy, Chile (public)
        'NV', 'N.V',  # Netherlands (public)
        'AG', 'A.G',  # Germany, Switzerland (can be public)
        'SA', 'S.A.',  # Many countries - public when listed
        'Sad', 'S.a.d',  # Serbia (public)
        'SE', 'S.E',  # European public company
        'SA de CV', 'S.A. de C.V',  # Mexico (public with variable capital)
        'SAB de CV', 'S.A.B. de C.V',  # Mexico (public with variable capital)
        'SAPI de CV',  # Mexico (public with variable capital)
        'Bhd',  # Malaysia, Singapore, Brunei
        'KK', 'K.K',  # Japan (corporation)
        'SAOG', #Oman
        'AG & Co KGaA',  # Germany (publicly traded partnership)
        'GmbH & Co KGaA',  # Germany (publicly traded partnership)
    }
    
    # Suffixes that could indicate government/public sector
    GOVERNMENT_INDICATORS = [
        'SOE', 'State Owned', 'Ministry', 'Department', 'Authority',
        'Commission', 'Agency', 'Bureau', 'Administration', 'Government',
        'Municipal', 'Federal', 'National', 'Regional', 'Provincial',
        'State Corporation', 'Public Corporation', 'Crown Corporation'
    ]
    
    # Trusted domains for search
    TRUSTED_DOMAINS = [
        'bloomberg.com',
        'reuters.com',
        'ft.com',
        'wsj.com',
        'sec.gov',
        'crunchbase.com',
        'linkedin.com',
        'companieshouse.gov.uk',
        'dnb.com',
        'opencorporates.com',
        'wikipedia.org',
        'forbes.com',
    ]
    
    def __init__(self, valid_countries: Optional[List[str]] = None):
        """
        Initialize with API keys and optional list of valid countries
        
        Args:
            valid_countries: List of valid country names from institution table
        """
        self.serper_api_key = os.getenv('SERPER_API_KEY', '')
        self.openai_api_key = os.getenv('OPENAI_API_KEY', '')
        self.openai_client = OpenAI(api_key=self.openai_api_key) if self.openai_api_key else None
        
        self.valid_countries = valid_countries or []
        if self.valid_countries:
            print(f"Loaded {len(self.valid_countries)} valid countries from institution table")
    
    def detect_government_entity(self, institution_name: str) -> bool:
        """
        Check if institution name suggests government/public sector entity
        
        Args:
            institution_name: Full institution name
            
        Returns:
            True if appears to be government entity
        """
        name_lower = institution_name.lower()
        
        for indicator in self.GOVERNMENT_INDICATORS:
            if indicator.lower() in name_lower:
                return True
        
        return False
    
    def detect_public_private_from_suffix(self, institution_name: str) -> Optional[str]:
        """
        Detect if institution is Public or Private based on legal suffix
        
        Args:
            institution_name: Full institution name
            
        Returns:
            'Public', 'Private', or None if can't determine
        """
        # First check if it's a government entity
        # Government entities should be classified as "Public" (public sector)
        if self.detect_government_entity(institution_name):
            return 'Public'
        
        # Check for public suffixes (stock exchange listed)
        for suffix in self.PUBLIC_SUFFIXES:
            if self._has_suffix(institution_name, suffix):
                return 'Public'
        
        # Check for private suffixes
        for suffix in self.PRIVATE_SUFFIXES:
            if self._has_suffix(institution_name, suffix):
                return 'Private'
        
        return None
    
    def _has_suffix(self, institution_name: str, suffix: str) -> bool:
        """
        Check if institution name has the given suffix
        Handles various formats: " Ltd", ".Ltd", "-Ltd", ", Ltd"
        """
        patterns = [
            f' {suffix}',
            f'.{suffix}',
            f'-{suffix}',
            f', {suffix}',
        ]
        
        for pattern in patterns:
            if institution_name.endswith(pattern):
                return True
        
        # Also check without separator for patterns like "CompanyPLC"
        if institution_name.endswith(suffix) and len(institution_name) > len(suffix):
            char_before = institution_name[-(len(suffix)+1)]
            # Check if character before suffix suggests it's a suffix
            if char_before.isupper() or char_before in [' ', '.', '-', ',', '(']:
                return True
        
        return False
    
    def match_country_to_institution_table(self, country_name: str) -> Optional[str]:
        """
        Match a country name to valid institution table country entries
        
        Args:
            country_name: Country name to match
            
        Returns:
            Matched country name from institution table, or original
        """
        if not self.valid_countries or not country_name:
            return country_name
        
        country_clean = country_name.strip()
        
        # Exact match (case-insensitive)
        for valid_country in self.valid_countries:
            if country_clean.lower() == valid_country.lower():
                return valid_country
        
        # Partial match
        for valid_country in self.valid_countries:
            if country_clean.lower() in valid_country.lower():
                return valid_country
            if valid_country.lower() in country_clean.lower():
                return valid_country
        
        # ISO code mappings
        iso_mappings = {
            'USA': ['United States', 'USA', 'US', 'United States of America', 'U.S.', 'U.S.A.'],
            'US': ['United States', 'USA', 'US', 'United States of America', 'U.S.', 'U.S.A.'],
            'UK': ['United Kingdom', 'UK', 'Great Britain', 'Britain', 'U.K.'],
            'GBR': ['United Kingdom', 'UK', 'Great Britain', 'Britain', 'U.K.'],
            'ARG': ['Argentina'],
            'BRA': ['Brazil', 'Brasil'],
            'CHL': ['Chile'],
            'MEX': ['Mexico', 'México'],
            'CAN': ['Canada'],
            'FRA': ['France'],
            'DEU': ['Germany', 'Deutschland'],
            'GER': ['Germany', 'Deutschland'],
            'ITA': ['Italy', 'Italia'],
            'ESP': ['Spain', 'España'],
            'CHN': ['China', 'People\'s Republic of China', 'PRC'],
            'JPN': ['Japan'],
            'IND': ['India'],
            'AUS': ['Australia'],
            'NLD': ['Netherlands', 'The Netherlands'],
            'BEL': ['Belgium'],
            'SWE': ['Sweden'],
            'NOR': ['Norway'],
            'DNK': ['Denmark'],
            'FIN': ['Finland'],
            'POL': ['Poland'],
            'TUR': ['Turkey', 'Türkiye'],
        }
        
        if country_clean.upper() in iso_mappings:
            possible_names = iso_mappings[country_clean.upper()]
            for possible_name in possible_names:
                for valid_country in self.valid_countries:
                    if possible_name.lower() in valid_country.lower() or valid_country.lower() in possible_name.lower():
                        return valid_country
        
        return country_name
    
    def search_trusted_sources(self, institution_name: str) -> List[Dict[str, str]]:
        """Search Google for institution info using Custom Search API"""
        # Try to get API keys from both environment and streamlit secrets
        google_api_key = os.getenv('GOOGLE_API_KEY', '')
        search_engine_id = os.getenv('GOOGLE_SEARCH_ENGINE_ID', '')
        
        # If not in env vars, try streamlit secrets
        if not google_api_key or not search_engine_id:
            try:
                import streamlit as st
                google_api_key = google_api_key or st.secrets.get('GOOGLE_API_KEY', '')
                search_engine_id = search_engine_id or st.secrets.get('GOOGLE_SEARCH_ENGINE_ID', '')
            except:
                pass
        
        if not google_api_key or not search_engine_id:
            print("DEBUG: Missing API credentials, using fallback")
            return self._fallback_search(institution_name)
        
        results = []
        base_url = "https://www.googleapis.com/customsearch/v1"
        
        # Try multiple search strategies
        search_queries = [
            f'"{institution_name}" company',  # Exact match with "company"
            f'"{institution_name}" headquarters',  # Original exact query
            f'{institution_name} company information',  # Without quotes
            f'{institution_name} corporation',  # Try "corporation"
            f'{institution_name} business',  # Try "business"
        ]
        
        # Remove suffixes for broader search
        broad_name = institution_name
        for suffix in [' SA', ' S.A.', ' Inc', ' Inc.', ' Ltd', ' LLC', ' Corp', ' PLC', ' Company', ' Co']:
            if broad_name.endswith(suffix):
                broad_name = broad_name[:-len(suffix)].strip()
        
        # Add broad searches
        if broad_name != institution_name:
            search_queries.extend([
                f'"{broad_name}" company',
                f'{broad_name} business information',
            ])
        
        for i, query in enumerate(search_queries):
            if len(results) >= 8:  # Stop if we have enough results
                break
                
            try:
                print(f"DEBUG: Search attempt {i+1}: {query}")
                response = requests.get(
                    base_url,
                    params={
                        'key': google_api_key,
                        'cx': search_engine_id,
                        'q': query,
                        'num': 10
                    },
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    new_results = len(data.get('items', []))
                    print(f"DEBUG: Query '{query}' returned {new_results} results")
                    
                    existing_links = {r['link'] for r in results}
                    for item in data.get('items', []):
                        link = item.get('link', '')
                        if link not in existing_links:
                            results.append({
                                'title': item.get('title', ''),
                                'link': link,
                                'snippet': item.get('snippet', ''),
                                'match_type': 'exact' if i < 2 else 'broad'
                            })
                            
                    if new_results > 0:
                        break  # Found results, don't need to try more queries
                        
                elif response.status_code == 403:
                    print("DEBUG: API quota exceeded or permissions issue")
                    error_data = response.json()
                    print(f"DEBUG: Error details: {error_data}")
                    break
                else:
                    print(f"DEBUG: API error {response.status_code}: {response.text}")
                    
            except Exception as e:
                print(f"Search error for query '{query}': {e}")
                continue  # Try next query
        
        print(f"DEBUG: Total unique results found: {len(results)}")
        
        # If still no results, try a very broad search
        if not results:
            try:
                fallback_query = broad_name if broad_name != institution_name else institution_name.split()[0]
                print(f"DEBUG: Final fallback search: {fallback_query}")
                
                response = requests.get(
                    base_url,
                    params={
                        'key': google_api_key,
                        'cx': search_engine_id,
                        'q': fallback_query,
                        'num': 5
                    },
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    for item in data.get('items', []):
                        results.append({
                            'title': item.get('title', ''),
                            'link': item.get('link', ''),
                            'snippet': item.get('snippet', ''),
                            'match_type': 'fallback'
                        })
                    print(f"DEBUG: Fallback search found {len(results)} results")
            except Exception as e:
                print(f"Fallback search error: {e}")
        
        if not results:
            print("DEBUG: No results found, using manual fallback")
            return self._fallback_search(institution_name)
        
        return results[:10]
    
    def _fallback_search(self, institution_name: str) -> List[Dict[str, str]]:
        """Fallback search results"""
        encoded_name = institution_name.replace(" ", "+")
        return [
            {
                'title': f'{institution_name} - Google Search',
                'link': f'https://www.google.com/search?q="{encoded_name}"+company',
                'snippet': 'Manual search required',
                'match_type': 'fallback'
            }
        ]
    
    def extract_institution_data(
        self,
        institution_name: str,
        search_results: List[Dict[str, str]],
        suffix_detected_type1: Optional[str] = None
    ) -> InstitutionLookupResult:
        """Use LLM to extract structured institution data"""
        if not self.openai_client:
            return self._create_empty_result(institution_name, "OpenAI API key not configured")
        
        if not search_results:
            return self._create_empty_result(institution_name, "No search results found")
        
        context = self._build_search_context(search_results)
        
        valid_countries_str = ""
        if self.valid_countries:
            sample_countries = self.valid_countries[:50]
            valid_countries_str = f"\n\nVALID COUNTRIES (must use one of these):\n{', '.join(sample_countries)}\n... and {len(self.valid_countries) - 50} more"
        
        suffix_info = ""
        if suffix_detected_type1:
            suffix_info = f"\n\nIMPORTANT: Suffix analysis indicates: {suffix_detected_type1}\nUse this for institution_type_layer1 unless search results clearly contradict it."
        
        prompt = f"""Extract information about this institution from search results.

Institution Name: {institution_name}
{suffix_info}

Search Results:
{context}
{valid_countries_str}

Extract:
1. Institution Type Layer 1: "Public" or "Private"{' - USE SUFFIX DETECTION ABOVE' if suffix_detected_type1 else ''}
   - Public = government entity 
   - Private = privately held company
2. Institution Type Layer 2: Classify as one of the options on this list (MUST be one of these options): 'Corporation' 'Funds' 'Commercial FI' 'Government' 'Institutional Investors' 'Bilateral DFI' 'SOE 'Multilateral Climate Funds' 'Multilateral DFI' 'Export Credit Agency (ECA)' 'State-owned FI' 'National DFI' 'Household/Individual' 'Public Fund' 'Third Sector Organisation'
3. Institution Type Layer 3: Classify as one of the options on this list (MUST be one of these options): 'Corporate' 'Venture Capital Funds' 'Commercial Bank' 'Infrastructure Funds' 'Subnational Government' 'Pension Fund' 'Private Equity Funds' 'Corporate & Investment Banks' 'Central Government' 'Asset Manager' 'Insurance Company' 'Government Agencies' 'Social Enterprise/Cooperative' 'Philanthropic Organisations' 'Charitable Organisations' 'Supranational Government' 'Retail Bank' 'Bilateral DFI' 'Corporate and Investment Banks' 'National DFI' 'Multilateral DFI' 'Sovereign Wealth Fund' 'State-owned Enterprise' 'Climate Fund' 'Central Bank' 'Households' 'Wealth Management' 'Export Credit Agencies' 'High Net Worth Individuals' 'Private Debt Funds' 'Bond Fund' 'Unknown' 'Exchange Traded Funds (ETF)' 'Civil Society and Advocacy Organisations' 'Equity Fund' 'Mutual Fund' 'Corporation' 'Real Estate' 'Hedge Funds' 'Subnational government' 'REIT' 'Exchanges' 'Private Equity Fund' 'Investment Bank' 'Insurance' 'Utility' 'Electricity Company' 'Infrastructure Fund' 'SOE' 'United States of America' 'Industry' 'Consumer Discretionary Products' 'Consumer Staples' 'Construction' 'Staffing & Recruiting' 'Renewable Energy' 'Agriculture' 'Financial Services' 'Consumer' 'Automobile' 'Energy' 'Mining' 'Domestic' 'Charitable and Aid Delivery Organisations' 'National' 'Sub-national' 'Manufacturing' 'Health Care' 'Water' 'Switzerland' 'Agro-industrial' 'Engineering' 'Investment Advisor' 'Materials' 'Private Equity' 'Water and Wastewater' 'Plastic Recycling' 'Energy and Water' 'Water and Infrastructure' 'Business consulting, tax, and financial advisory' 'Sovereign wealth fund' 'Foundation' 'Universal Bank' 'Private Equity and Venture Capital' 'Export Credit Agency'
4. Parent Country: country the institution headquarters/parent company are based in (this could include country names like 'United States of America', 'Chile', 'Ethiopia', etc. or could be certain transnational groups like 'World Bank', 'Adaptation Fund', 'African Development Bank', etc.)
5. Subsidiary Country: country of the subsidiary company, i.e. where the institution is operating in. This is often same as parent country but not always. This should also include both the country names as well as the transnational groups.

CONFIDENCE SCORING:
- 0.9-1.0: Perfect match with clear, specific information about this exact institution
- 0.7-0.9: Good match with solid information, minor uncertainties
- 0.5-0.7: Partial information or possible match with some concerns
- 0.3-0.5: Limited information or unclear if correct institution
- 0.0-0.3: Results appear to be about wrong company or no useful information

CRITICAL: 
- If Layer 2 or Layer 3 indicates government (Government, Ministry, Agency, etc.), then Layer 1 MUST be "Public"
- Match countries exactly to valid list
- Government/public sector = "Public", not "Private"

Return JSON:
{{
    "institution_type_layer1": "Public/Private or null",
    "institution_type_layer2": "Category or null",
    "institution_type_layer3": "Specific type or null",
    "parent_country": "Country name or null",
    "subsidiary_country": "Country name or null",
    "confidence_score": 0.0-1.0,
    "reasoning": "Brief explanation"
}}"""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Extract institution data. Return only valid JSON. Government entities are Public, not Private."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=600
            )
            
            content = response.choices[0].message.content.strip()
            
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            data = json.loads(content)
            
            # Override with suffix detection if LLM returned null
            # Get values
            type1 = data.get('institution_type_layer1')
            type2 = data.get('institution_type_layer2')
            type3 = data.get('institution_type_layer3')
            
            # PRIORITY 1: Check if government based on type2/type3
            if type2 and 'government' in type2.lower():
                type1 = 'Public'
                data['reasoning'] = f"Corrected to Public (government entity). {data.get('reasoning', '')}"
            elif type3 and any(word in type3.lower() for word in ['government', 'ministry', 'agency', 'authority', 'bureau']):
                type1 = 'Public'
                data['reasoning'] = f"Corrected to Public (government entity). {data.get('reasoning', '')}"
            
            # PRIORITY 2: Use suffix detection if available (high confidence)
            elif suffix_detected_type1:
                # Suffix detection should take priority over LLM guess
                if type1 != suffix_detected_type1:
                    print(f"DEBUG: LLM said '{type1}' but suffix says '{suffix_detected_type1}' - using suffix")
                type1 = suffix_detected_type1
                data['reasoning'] = f"Used suffix detection ({suffix_detected_type1}). {data.get('reasoning', '')}"
            
            # PRIORITY 3: Use LLM value if no suffix and not government
            elif not type1:
                type1 = data.get('institution_type_layer1')
            
            # Match countries
            parent_country = self.match_country_to_institution_table(data.get('parent_country'))
            subsidiary_country = self.match_country_to_institution_table(data.get('subsidiary_country'))
            
            return InstitutionLookupResult(
                institution_name=institution_name,
                institution_type_layer1=type1,
                institution_type_layer2=type2,
                institution_type_layer3=type3,
                parent_country=parent_country,
                subsidiary_country=subsidiary_country,
                confidence_score=float(data.get('confidence_score', 0.5)),
                sources=[{'url': r['link'], 'title': r['title']} for r in search_results[:5]],
                reasoning=data.get('reasoning', ''),
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            print(f"LLM extraction error: {e}")
            import traceback
            traceback.print_exc()
            return self._create_empty_result(institution_name, f"Extraction error: {str(e)}")
    
    def _build_search_context(self, search_results: List[Dict[str, str]]) -> str:
        """Build context string from search results"""
        context_parts = []
        for idx, result in enumerate(search_results[:8], 1):
            match_type = result.get('match_type', 'unknown')
            context_parts.append(f"""
Source {idx} [{match_type}]:
Title: {result['title']}
URL: {result['link']}
Snippet: {result['snippet']}
""")
        return "\n".join(context_parts)
    
    def _create_empty_result(self, institution_name: str, reason: str) -> InstitutionLookupResult:
        """Create empty result when lookup fails"""
        return InstitutionLookupResult(
            institution_name=institution_name,
            institution_type_layer1=None,
            institution_type_layer2=None,
            institution_type_layer3=None,
            parent_country=None,
            subsidiary_country=None,
            confidence_score=0.0,
            sources=[],
            reasoning=reason,
            timestamp=datetime.now().isoformat()
        )
    
    def lookup_institution(self, institution_name: str) -> InstitutionLookupResult:
        """
        Main method: Lookup institution data
        
        Args:
            institution_name: Name of institution
            
        Returns:
            Structured institution data
        """
        # Step 1: Suffix-based detection
        suffix_detected = self.detect_public_private_from_suffix(institution_name)
        
        # Step 2: Search for additional info
        search_results = self.search_trusted_sources(institution_name)
        
        # Step 3: Extract structured data
        result = self.extract_institution_data(
            institution_name, 
            search_results,
            suffix_detected_type1=suffix_detected
        )
        
        return result