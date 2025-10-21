from typing import List, Tuple, Optional
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from cpi_tools import fuzzy_matching_cpi
from utils.text_processing import TextProcessor
import streamlit as st


class FuzzyMatcher: 

    def __init__(self, threshold: float = 0.85):
        """
        Initializes fuzzy matcher
        
        Args:
            threshold: threshold matches should reach to be output
        """
        self.threshold = threshold
        self.vectorizer = None
        self.tfidf_matrix = None
        self.institution_names = None
    
    def fit(self, institution_df: pd.DataFrame):
        """
        Pre-compute TF-IDF vectors for all institutions 
        
        Args:
            institution_df: institution_cpi df
        """
        if institution_df.empty or 'institution_cpi' not in institution_df.columns:
            return
        
        self.institution_names = institution_df['institution_cpi'].dropna().tolist()
        
        if not self.institution_names:
            return
        
        normalized_names = [
            TextProcessor.normalize_institution_name(name).lower()
            for name in self.institution_names
        ]
        
        # Used character n-grams (around 2-4) with word boundaries to handle typos better
        self.vectorizer = TfidfVectorizer(
            analyzer='char_wb',
            ngram_range=(2, 4),  
            lowercase=True,
            strip_accents='unicode'
        )
        
        # Fit and transform institution names
        self.tfidf_matrix = self.vectorizer.fit_transform(normalized_names)
    
    def find_similar_institutions(
        self,
        query: str,
        institution_df: pd.DataFrame,
        limit: int = 5,
        tfidf_top_k: int = 50  # Get top 50 from TF-IDF, then use cpi tools matching
    ) -> List[Tuple[str, float]]:
        """
        Find similar institutions with both tf-idf vectorization then the cpi tools fuzzy matching
        
        Args:
            query: Institution name to search for
            institution_df: institution_cpi df table
            limit: Final number of results to return
            tfidf_top_k: Number of candidates to get from tf-idf before cpi tools 
            
        Output:
            List of tuples (institution_name, similarity_score)
        """
        if not query or query.strip() == '':
            return []
        
        if self.vectorizer is None or self.tfidf_matrix is None:
            self.fit(institution_df)
        
        if not self.institution_names:
            return []
        

        normalized_query = TextProcessor.normalize_institution_name(query).lower()
        
        query_vector = self.vectorizer.transform([normalized_query])
        
        similarities = cosine_similarity(query_vector, self.tfidf_matrix).flatten()
        
        top_indices = np.argsort(similarities)[-tfidf_top_k:][::-1]
        top_candidates = [self.institution_names[i] for i in top_indices]
        
        filtered_candidates = [
            self.institution_names[idx] 
            for idx in top_indices 
            if similarities[idx] > 0.1 
        ]
        
        if not filtered_candidates:
            return []
        
        try:
            match_df = pd.DataFrame({'query': [query]})
            
            result_df = fuzzy_matching_cpi.fm_dataset(
                dataframe_to_match=match_df,
                original_column='query',
                search_list=filtered_candidates,
                stop_words=None,
                clean_names=True,
                multiple_matches=True,
                set_threshold_dynamically=False,
                lower_threshold=self.threshold,
                acronym_map=None
            )

            if result_df.empty or 'Matched string' not in result_df.columns:
                return []
            
            matched_strings = result_df['Matched string'].iloc[0]
            matched_scores = result_df['Matched score'].iloc[0]
            
            if isinstance(matched_strings, list) and isinstance(matched_scores, list):
                matches = list(zip(matched_strings, matched_scores))
                matches.sort(key=lambda x: x[1], reverse=True)
                return matches[:limit]
            
            return []
            
        except Exception as e:
            print(f"Error in cpi tools matching: {e}")
            tfidf_matches = [
                (self.institution_names[idx], float(similarities[idx]))
                for idx in top_indices[:limit]
                if similarities[idx] >= 0.3
            ]
            return tfidf_matches
    
    def batch_match(
        self,
        queries: List[str],
        institution_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Batch match multiple queries efficiently
        
        Args:
            queries: List of institution names
            institution_df: df with institutions (institution_cpi)
            
        Output:
            df with match results
        """
        results = []
        
        for query in queries:
            matches = self.find_similar_institutions(query, institution_df, limit=1)
            
            if matches:
                best_match, score = matches[0]
                is_dup = score >= self.threshold
            else:
                best_match, score = None, 0.0
                is_dup = False
            
            results.append({
                'query': query,
                'best_match': best_match,
                'score': score,
                'is_duplicate': is_dup
            })
        
        return pd.DataFrame(results)


@st.cache_resource(ttl=600)
def get_fitted_matcher(institution_df: pd.DataFrame, threshold: float = 0.85) -> FuzzyMatcher:
    """
    Get fitted fuzzy matcher (cached)
    
    Args:
        institution_df: df with institutions (institution_cpi)
        threshold: Similarity threshold
        
    Output:
        fuzzy matcher
    """
    matcher = FuzzyMatcher(threshold=threshold)
    matcher.fit(institution_df)
    return matcher