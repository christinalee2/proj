# Import packages
from io import StringIO
import pandas as pd
import numpy as np
import string
from unidecode import unidecode
import warnings
import time
import s3fs
import boto3
import jaro  # pip install jaro-winkler
from typing import List, Tuple, Optional, Dict

# ------------------------------------
# -- Define cleaning function
# ------------------------------------


def clean_entity_name(original_entity: Optional[str], stop_words: Optional[List[str]] = None) -> str:
    """
    Robust normalisation for entity names.

    - Handles None/NaN
    - Lowercases, strips, removes parenthetical tail like " (x)", removes punctuation
    - Normalises unicode to ASCII where possible and collapses whitespace
    - Optionally removes stop words
    """
    if original_entity is None or (isinstance(original_entity, float) and np.isnan(original_entity)):
        return ""

    s = str(original_entity).strip().lower()
    # drop trailing parenthetical (common in datasets)
    s = s.split(' (')[0]
    # remove punctuation
    translator = str.maketrans('', '', string.punctuation)
    s = s.translate(translator)
    # normalize diacritics
    s = unidecode(s)
    # collapse whitespace
    s = ' '.join(s.split())
    if stop_words:
        s = ' '.join(w for w in s.split() if w not in stop_words)
    return s


# ------------------------------------
# -- Define threshold function
# ------------------------------------


def dynamic_threshold(matching_scores: List[float], lower_threshold: float) -> float:
    """
    Pick a dynamic threshold from matching_scores by finding the largest gap in sorted scores >= lower_threshold.

    Returns a fallback of lower_threshold when not enough data to compute a dynamic value.
    """
    if not matching_scores:
        return lower_threshold
    # Keep only scores >= lower_threshold
    sorted_scores = sorted([s for s in matching_scores if s >= lower_threshold], reverse=True)
    if not sorted_scores:
        return lower_threshold
    if len(sorted_scores) == 1:
        return sorted_scores[0]
    # compute ratios defensively
    score_ratios = []
    for i in range(len(sorted_scores) - 1):
        denom = sorted_scores[i + 1]
        if denom <= 0:
            score_ratios.append(float('inf'))
        else:
            score_ratios.append(sorted_scores[i] / denom)
    # index of largest ratio
    idx = int(np.argmax(score_ratios))
    return sorted_scores[idx]


# ------------------------------------
# -- Define manual input of
# -- threshold function
# ------------------------------------


def suggest_threshold(matched_scores: List[float], default: float = 0.8) -> float:
    """
    Deterministically suggest a threshold given a list of matched_scores.
    Falls back to `default` when not enough information is available.
    """
    if not matched_scores:
        return default
    s = sorted(matched_scores, reverse=True)
    if len(s) < 2:
        return s[0]
    ratios = []
    for i in range(len(s) - 1):
        denom = s[i + 1]
        ratios.append(float('inf') if denom <= 0 else s[i] / denom)
    idx = int(np.argmax(ratios))
    return s[idx]


# ------------------------------------
# -- Define fuzzy matching function
# ------------------------------------


def _generate_variants(s: str, stop_words: Optional[List[str]], acronym_map: Optional[Dict[str, str]] = None) -> List[str]:
    """Generate cleaned variants for a string, including acronym expansions when provided."""
    base = clean_entity_name(s, stop_words)
    variants = {base}
    if not acronym_map:
        return list(variants)
    # check tokens for acronym map matches
    toks = base.split()
    # build variant by expanding tokens that match keys in acronym_map
    expanded_tokens = [acronym_map.get(t.upper(), t) for t in toks]
    expanded = ' '.join([t for t in expanded_tokens if t])
    expanded = clean_entity_name(expanded, stop_words)
    variants.add(expanded)
    # also include initials-only form (e.g., ABN AMRO -> abn amro -> abnamro?) keep simple: join initials
    initials = ''.join([t[0] for t in toks if t])
    if initials:
        variants.add(initials)
    return list(variants)


def run_fuzzy_match(
    entity_to_match: str,
    search_list: List[str],
    stop_words: Optional[List[str]] = None,
    clean_names: bool = True,
    multiple_matches: bool = False,
    set_threshold_dynamically: bool = False,
    lower_threshold: float = 0.8,
    acronym_map: Optional[Dict[str, str]] = None,
) -> Tuple[List[float], List[str]]:
    """
    Robust fuzzy match using Jaro-Winkler with optional acronym expansion.

    Always returns (list_of_scores, list_of_entities) for consistency.
    """
    if not search_list:
        return [], []

    # Prepare variants for the query
    if clean_names:
        query_variants = _generate_variants(entity_to_match, stop_words, acronym_map)
        # prepare candidate variants list of lists
        candidate_variants = [_generate_variants(x, stop_words, acronym_map) for x in search_list]
    else:
        query_variants = [entity_to_match or '']
        candidate_variants = [ [x or ''] for x in search_list]

    matched_entities: List[str] = []
    matched_scores: List[float] = []

    # compute best score per candidate by comparing all variant pairs
    for cand_idx, cand_forms in enumerate(candidate_variants):
        best_score_for_candidate = 0.0
        for q in query_variants:
            for c in cand_forms:
                try:
                    score = jaro.jaro_winkler_metric(q, c)
                except Exception:
                    score = 0.0
                if score > best_score_for_candidate:
                    best_score_for_candidate = score
        matched_entities.append(search_list[cand_idx])
        matched_scores.append(best_score_for_candidate)

    # process results according to flags
    if not multiple_matches:
        max_score = max(matched_scores)
        if max_score >= lower_threshold:
            best_idx = matched_scores.index(max_score)
            return [max_score], [matched_entities[best_idx]]
        return [], []

    # multiple matches without dynamic threshold
    if multiple_matches and not set_threshold_dynamically:
        kept = [(s, e) for s, e in zip(matched_scores, matched_entities) if s >= lower_threshold]
        kept.sort(reverse=True)
        return [s for s, e in kept], [e for s, e in kept]

    # multiple matches with dynamic threshold
    if multiple_matches and set_threshold_dynamically:
        calc_th = dynamic_threshold(matched_scores, lower_threshold)
        paired = sorted(list(zip(matched_scores, matched_entities)), reverse=True)
        kept = [(s, e) for s, e in paired if s >= calc_th]
        return [s for s, e in kept], [e for s, e in kept]

    return [], []


# ------------------------------------
# -- Define master function
# ------------------------------------


def fm_dataset(
    dataframe_to_match: pd.DataFrame,
    original_column: str,
    search_list: List[str],
    stop_words: Optional[List[str]],
    clean_names: bool = True,
    multiple_matches: bool = False,
    set_threshold_dynamically: bool = False,
    lower_threshold: float = 0.8,
    acronym_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:

    """
    fm_dateset performs a fuzzy match for all lines appear in the original dataframe passed as input. For each pair of strings, the Jaro-Winkler metric is calculated and, depending on the arguments specified, the combination will be either treated as a match and the matched string retained and stored in a new column, or discared.

    *Arguments*
    dataframe_to_match: a pd.DataFrame object that contains the column that needs to be matched to one or more elements of another list.
    
    original_column: a string, specifying the name of the column in "dataframe_to_match" that needs to be used as base for the fuzzy match. For each item in this column, the algorithm will try and find a match in the list passed in the following argument.
    
    search_list: a list of strings, containing all the potential matches to the "original_column" items. The algorithm will iterate over each of the elements in this list and calculate the distance score between it and the item to be matched. 
    
    stop_words: a list of strings, containing stop words to be removed from both the base strings to be matched and the strings of potential matches.
    
    clean_names: a boolean indicating whether to perform the names' cleaning (removal of stop words, lower case, removal of punctuation). Defaulted to True. 
    
    multiple_matches: a boolean indicating whether multiple matches are to be allowed by the algorithm. If False (as per default), each base string to matched will be matched with at most one string from the "search_list" list (the match with the higher score will be retained); otheriwse, an indefinite amount of matches will be returned. 
    
    set_threshold_dynamically: a boolean, defaulted to False, specifying whether the lower threshoold (after which pair of matches will be discared) should  be set dynamically (contact the author of this function for more details) or whether it should be fixed to a constant (see next argument).
    
    lower_threshold: a float between 0 and 1 that indicates the lower bound for matches to be considered as such. The higher the threshold, the lower the  probability of finding a match, but also the lower the probability of a  false positive. Defaulted to 0.8.


    *Output*
    dataframe_to_match: pd.DataFrame object similar to the inputted one, with the addition of two extra columns: "Matched score" and "Matched string", each in a list format.

    *Example* 

    *Author*
    Nikita Marini - nikita.marini@cpiglobal.org

    Last updated on November 15th, 2023.
    """

    # Apply function (ensure consistent return types)
    def _apply_row(x):
        scores, ents = run_fuzzy_match(
            x,
            search_list,
            stop_words,
            clean_names,
            multiple_matches,
            set_threshold_dynamically,
            lower_threshold,
            acronym_map,
        )
        return pd.Series({'Matched score': scores, 'Matched string': ents})

    results = dataframe_to_match[original_column].apply(_apply_row)
    # concat results columns
    dataframe_to_match = pd.concat([dataframe_to_match.reset_index(drop=True), results.reset_index(drop=True)], axis=1)
    return dataframe_to_match


# Test
# importpath = '/Users/nikitamarini/Desktop/CPI'
# df = pd.read_csv(f'{importpath}/gfanz_entities.csv')
# search_list_test = ['Ageas', 'ABN AMRO', 'Intesa San Paolo']

# df = run_fuzzy_match(df, 'Entity', search_list_test)
# print(df[df['Matched entity'].apply(len) > 0])
