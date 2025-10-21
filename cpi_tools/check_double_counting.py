from collections import defaultdict, deque
from io import StringIO
import pandas as pd
import numpy as np
import boto3
import s3fs
from datetime import datetime
import jaro  # pip install jaro-winkler
import warnings 
import os
from cpi_tools import aws_tools

warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)


def extract_data_sources(data_dict, df1_name, df2_name, verbosity_global=2):
    """ 
    Function to read in both of the data sources to compare

    Parameters:
    - data_dict: dictionary of data to use, where keys are the dataset names, values are the datasets
    - df1_name: str name of the first dataset, which we'll always keep (the other will have its duplicates removed)
    - df2_name: str name of the second dataset, which we'll prune of duplicates
    - verbosity_global: int. which verbosity level is being applied

    Returns:
    - tuple: Two DataFrames (df1_include_only, df2_include_only) corresponding to the two data sources (include only)
    - tuple: Two DataFrames (df1_already_excluded, df2_already_excluded) corresponding to the two data sources (exclude only)
    
    """

    if len(data_dict) != 2:
        raise ValueError("Exactly two datasets must be provided for comparison")
    
    df1 = data_dict[df1_name]
    df2 = data_dict[df2_name]

    df1_include_only = df1[df1['exclude_include'] == 'Include']
    df1_already_excluded = df1[df1['exclude_include'] != 'Include']

    df2_include_only = df2[df2['exclude_include'] == 'Include']
    df2_already_excluded = df2[df2['exclude_include'] != 'Include']

    return df1_include_only, df2_include_only, df1_already_excluded, df2_already_excluded


def prepare_dataframes_for_merge(df1, df2, data_source1, data_source2, 
                                project_columns=['project_name'], matching_columns=None,
                                validation_columns=None, value_column='value_USDm'):
    """
    Prepare datasets for initial deduplication merge by standardizing and aggregating their columns.
    
    Parameters:
    - df1 (DataFrame): First data source DataFrame
    - df2 (DataFrame): Second data source DataFrame
    - data_source1 (str): Name of the first data source
    - data_source2 (str): Name of the second data source
    - project_columns (list): List of project-related columns to include for fuzzy matching
    - matching_columns (list, optional): Columns to match on for potential duplicates
    - validation_columns (list, optional): Additional columns to include in validation
    - value_column (str): Column containing the values to be summed
    
    Returns:
    - tuple: Two prepared DataFrames (df1_for_merge, df2_for_merge)
    """
    # Default matching columns if not provided
    if matching_columns is None:
        matching_columns = ['year', 'solution_cpi', 'country_destination_cpi']
    
    
    # Build core columns including data_source, matching columns, project columns, and value column
    core_columns = ['data_source'] + matching_columns + project_columns + validation_columns + [value_column]
    
    # Build groupby columns (matching + project columns + data_source)
    groupby_columns = matching_columns + project_columns + ['data_source'] + validation_columns
    
    # Prepare first data source
    df1_cols = [col for col in core_columns if col in df1.columns]
    df1_for_merge = df1[df1_cols].copy()
    
    groupby1 = [col for col in groupby_columns if col in df1_for_merge.columns]
    groupby1_agg = {value_column: 'sum'}
    df1_for_merge = df1_for_merge.groupby(groupby1).agg(groupby1_agg).reset_index()
    
    # Prepare second data source
    df2_cols = [col for col in core_columns if col in df2.columns]
    df2_for_merge = df2[df2_cols].copy()
    
    groupby2 = [col for col in groupby_columns if col in df2_for_merge.columns]
    groupby2_agg = {value_column: 'sum'}
    df2_for_merge = df2_for_merge.groupby(groupby2).agg(groupby2_agg).reset_index()
    
    return df1_for_merge, df2_for_merge


def identify_potential_duplicates(df1_for_merge, df2_for_merge, data_source1, data_source2, matching_columns=None):
    """
    Identify potential duplicate projects across two data sources by merging them on matching columns.
    
    Parameters:
    - df1_for_merge (DataFrame): Prepared DataFrame from first data source
    - df2_for_merge (DataFrame): Prepared DataFrame from second data source
    - data_source1 (str): Name of the first data source
    - data_source2 (str): Name of the second data source
    - matching_columns (list, optional): Columns to use for matching records across data sources
    
    Returns:
    - DataFrame: DataFrame containing potential duplicate records
    """
    # Default matching columns if not provided
    if matching_columns is None:
        matching_columns = ['year', 'solution_cpi', 'country_destination_cpi']
    
    # Ensure all matching columns exist in both dataframes
    valid_match_columns = [col for col in matching_columns 
                          if col in df1_for_merge.columns and col in df2_for_merge.columns]
    
    if not valid_match_columns:
        raise ValueError("No valid matching columns found in both dataframes")
    
    # Merge datasets on matching columns
    suffixes = [f'_{data_source2}', f'_{data_source1}']
    
    df_overlap = df2_for_merge.merge(
        df1_for_merge, 
        how='inner', 
        on=valid_match_columns,
        suffixes=suffixes
    )
    
    return df_overlap


def compute_name_similarity(df_overlap, data_source1, data_source2, project_columns=['project_name'], similarity_threshold=0.65):
    """
    After they've been merged, compute name similarity between project columns to identify likely duplicates.
    
    We now compute the Jaro-Winkler similarity (https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
    in order to identify possible duplicates on the basis of project-related columns.
    Note that, in an effort of capturing as many instances of double counting as possible, 
    the threshold is kept relatively low and will therefore imply a certain level of false positives 
    (i.e., projects that are identified as existing in both data sources, despite actually relating to different RE project).
    
    Parameters:
    - df_overlap (DataFrame): DataFrame containing potential duplicates
    - data_source1 (str): Name of the first data source
    - data_source2 (str): Name of the second data source
    - project_columns (list): List of project-related columns to compare
    - similarity_threshold (float): Threshold for determining duplicates (0-1)
    
    Returns:
    - DataFrame: DataFrame with similarity scores and duplicate flags
    """
    
    # Clean project columns by removing content in parentheses and compute similarities
    similarity_scores = []
    
    for col in project_columns:
        col1 = f'{col}_{data_source1}'
        col2 = f'{col}_{data_source2}'
        
        if col1 in df_overlap.columns and col2 in df_overlap.columns:
            # Clean columns
            df_overlap[f'{col1}_clean'] = df_overlap[col1].str.replace(r'\([^)]*\)', '', regex=True)
            df_overlap[f'{col2}_clean'] = df_overlap[col2].str.replace(r'\([^)]*\)', '', regex=True)
            
            # Compute Jaro-Winkler similarity
            similarity_col = f'jw_distance_{col}'
            df_overlap[similarity_col] = df_overlap.apply(
                lambda row: jaro.jaro_winkler_metric(
                    str(row[f'{col1}_clean']) if pd.notna(row[f'{col1}_clean']) else '', 
                    str(row[f'{col2}_clean']) if pd.notna(row[f'{col2}_clean']) else ''
                ), 
                axis=1
            )
            similarity_scores.append(similarity_col)
    
    if not similarity_scores:
        raise ValueError("No valid project columns found for similarity comparison")
    
    # Compute maximum similarity across all project columns
    df_overlap['jw_distance_max'] = df_overlap[similarity_scores].max(axis=1)
    
    # Flag duplicates based on threshold (if ANY project column pair exceeds threshold)
    df_overlap['double_counted'] = df_overlap['jw_distance_max'] > similarity_threshold
    df_overlap['double_counted'] = df_overlap['double_counted'].astype(bool)
    
    return df_overlap


def extract_duplicates_for_validation(df_overlap, data_source1, data_source2, project_columns=['project_name'],
                                    matching_columns=None, additional_validation_columns=None):
    """
    If requested, given the overlapping scores, extract potential duplicates for manual validation.
    
    Parameters:
    - df_overlap (DataFrame): DataFrame with computed similarity scores
    - data_source1 (str): Name of the first data source
    - data_source2 (str): Name of the second data source
    - project_columns (list): List of project-related columns used for matching
    - matching_columns (list, optional): Columns used for matching records
    - additional_validation_columns (list, optional): Additional columns to include in validation
    
    Returns:
    - DataFrame: DataFrame containing potential duplicates for manual validation
    """
    # Default matching columns if not provided
    if matching_columns is None:
        matching_columns = ['year', 'solution_cpi', 'country_destination_cpi']
    
    # Build base validation columns
    validation_columns = matching_columns.copy()
    
    # Add project columns for both data sources
    for col in project_columns:
        validation_columns.extend([f'{col}_{data_source2}', f'{col}_{data_source1}'])
    
    # Add value columns
    validation_columns.extend([f'value_USDm_{data_source2}', f'value_USDm_{data_source1}'])
    
    # Add similarity scores
    validation_columns.append('jw_distance_max')
    validation_columns.append('double_counted')
    
    # Add individual similarity scores for each project column
    for col in project_columns:
        similarity_col = f'jw_distance_{col}'
        if similarity_col in df_overlap.columns:
            validation_columns.append(similarity_col)
    
    # Add additional validation columns if provided
    if additional_validation_columns:
        for col in additional_validation_columns:
            # Add both suffixed versions if they exist
            col1 = f'{col}_{data_source1}'
            col2 = f'{col}_{data_source2}'
            if col1 in df_overlap.columns:
                validation_columns.append(col1)
            if col2 in df_overlap.columns:
                validation_columns.append(col2)
    
    # Ensure all validation columns exist in the dataframe
    valid_validation_columns = [col for col in validation_columns if col in df_overlap.columns]
    
    # Extract relevant validation columns
    validation_df = df_overlap[valid_validation_columns].copy()

    # Add column for manual validation
    # Defaults to Include (no duplicates) unless marked as double counted
    validation_df['confirm_include_no_dupes'] = np.where(validation_df['double_counted'], False, True)
    
    # Filter for only the initial duplicates (those are the ones we'll want to check)
    validation_df = validation_df[validation_df['confirm_include_no_dupes'] == False]

    # Build groupby columns for aggregation
    groupby_cols = matching_columns.copy()
    for col in project_columns:
        groupby_cols.extend([f'{col}_{data_source2}', f'{col}_{data_source1}'])
    groupby_cols.extend(['jw_distance_max', 'confirm_include_no_dupes'])
    
    # Build aggregation dictionary
    agg_dict = {}
    
    # Add value columns to aggregation
    for val_col in [f'value_USDm_{data_source2}', f'value_USDm_{data_source1}']:
        if val_col in validation_df.columns:
            agg_dict[val_col] = 'unique'
    
    # Add additional validation columns to aggregation
    if additional_validation_columns:
        for col in additional_validation_columns:
            col1 = f'{col}_{data_source1}'
            col2 = f'{col}_{data_source2}'
            if col1 in validation_df.columns:
                agg_dict[col1] = 'unique'
            if col2 in validation_df.columns:
                agg_dict[col2] = 'unique'
    
    # Add individual similarity scores to aggregation
    for col in project_columns:
        similarity_col = f'jw_distance_{col}'
        if similarity_col in validation_df.columns:
            agg_dict[similarity_col] = 'unique'
    
    agg_dict['double_counted'] = 'unique'
    
    # Aggregate relevant duplicates
    validation_df = validation_df.drop_duplicates().groupby(groupby_cols).agg(agg_dict).reset_index()

    # Sort by similarity score in descending order
    primary_project_col = f'{project_columns[0]}_{data_source2}'
    if 'jw_distance_max' in validation_df.columns and primary_project_col in validation_df.columns:
        validation_df = validation_df.sort_values(by=[primary_project_col, 'jw_distance_max'], ascending=False)
    
    return validation_df


def export_validation_file(validation_df, s3_bucket, output_validation_path, year, df1_name, df2_name):
    """
    Export potential duplicates to a file for manual validation.
    
    Parameters:
    - validation_df (DataFrame): DataFrame with potential duplicates
    - s3_bucket (str): Name of the S3 bucket to store in
    - output_validation_path (str): path in the bucket to save the data
    - year (int): Year of the data
    - df1_name (str): Name of the first data source
    - df2_name (str): Name of the second data source
    
    Returns:
    - str: Path to the exported file
    """
    
    file_path = f's3://{s3_bucket}/{output_validation_path}/{df2_name}_deduplicated_with_{df1_name}_to_validate.csv'
    aws_tools.write_to_s3(validation_df, s3_bucket, output_validation_path, f'{df2_name}_deduplicated_with_{df1_name}_to_validate')
    
    return file_path


def import_validation_results(validation_file_path, s3_bucket=None):
    """
    Import manually validated duplicates.
    
    Parameters:
    - validation_file_path (str): Path to the validation file (relative to S3 bucket if provided)
    - s3_bucket (str, optional): Name of the S3 bucket containing validation file
    
    Returns:
    - DataFrame: DataFrame with manual validation results
    """
    # Import helper functions - assuming they exist in the package
    try:
        from cpi_tools.helper_functions import read_raw_data, extract_file_type
    except ImportError:
        # Fallback if helpers not available
        def read_raw_data(path, file_type):
            if file_type == 'csv':
                return pd.read_csv(path)
            elif file_type in ['xlsx', 'xls']:
                return pd.read_excel(path)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
        
        def extract_file_type(path):
            return path.split('.')[-1].lower()
    
    # Read the validation file
    if s3_bucket:
        validation_file_path = f"s3://{s3_bucket}/{validation_file_path}"
    file_type = extract_file_type(validation_file_path)
    validated_df = read_raw_data(validation_file_path, file_type)
    
    # Ensure the confirm_include column exists
    if 'confirm_manual_include' in validated_df.columns:
        print("RENAMING 'confirm_manual_include' to 'confirm_include_no_dupes'")
        validated_df['confirm_include_no_dupes'] = validated_df['confirm_manual_include']
    if 'confirm_include_no_dupes' not in validated_df.columns:
        print("ERROR: Validation file does not contain 'confirm_include_no_dupes' column")
            
    return validated_df


def apply_validation_results(df, validated_df, data_source2, project_columns=['project_name'], value_column='value_USDm'):
    """
    Apply manual validation results to the original dataset.
    
    Parameters:
    - df (DataFrame): Original second data source DataFrame
    - validated_df (DataFrame): DataFrame with manual validation results
    - data_source2 (str): Name of the second data source
    - project_columns (list): List of project-related columns used for matching
    - value_column (str): Column containing the monetary values
    
    Returns:
    - DataFrame: Updated DataFrame with validation results applied
    """
    for col in project_columns: 
        df[f'{col}_std'] = df[col].str.lower().str.strip()
        validated_df[f'{col}_{data_source2}'] = validated_df[f'{col}_{data_source2}'].str.lower().str.strip()
    
    # Build merge columns - use all project columns for merging back to original data
    merge_on_left = ['year'] + [f'{col}_std' for col in project_columns]
    merge_on_right = ['year'] + [f'{col}_{data_source2}' for col in project_columns]

    
    # We'll keep only if it's true for all rows in the group, otherwise (if marked exclude for any), exclude all
    validation_results = validated_df.groupby(merge_on_right)['confirm_include_no_dupes'].apply(lambda x: x.all()).reset_index()
    validation_results['confirm_include_no_dupes'] = validation_results['confirm_include_no_dupes'].astype(bool)

    # Merge back to original DataFrame
    result_df = df.merge(
        validation_results, 
        how='left',
        left_on=merge_on_left,
        right_on=merge_on_right
    )
    
    # Set exclude_include based on confirm_include (with default to Include)
    result_df['exclude_include'] = np.where(
        result_df['confirm_include_no_dupes'] == False, 
        'Exclude', 
        'Include'
    )
    
    # Handle rows not in validation set
    result_df['exclude_include'] = result_df['exclude_include'].fillna('Include')
    
    # Clean up temporary columns
    for col in project_columns:
        temp_col = f'{col}_{data_source2}'
        if temp_col in result_df.columns:
            result_df = result_df.drop(columns=[temp_col])
        std_col = f'{col}_std'
        if std_col in result_df.columns:
            result_df = result_df.drop(columns=[std_col])
    
    if 'confirm_include_no_dupes' in result_df.columns:
        result_df = result_df.drop(columns=['confirm_include_no_dupes'])
    
    return result_df


def mark_duplicates_without_validation(df, df_overlap, data_source2, project_columns=['project_name']):
    """
    Mark duplicates in the original dataset without manual validation if requested,
    just assuming all threshold-based duplicates are actual duplicates.
    
    Parameters:
    - df (DataFrame): Original second data source DataFrame
    - df_overlap (DataFrame): DataFrame with identified duplicates
    - data_source2 (str): Name of the second data source
    - project_columns (list): List of project-related columns used for matching
    
    Returns:
    - DataFrame: Updated DataFrame with duplicate flags applied
    """
    # Extract minimal duplicate information
    df_double_counted = df_overlap[df_overlap['double_counted'] == True]
    
    # Build columns to keep for merging
    merge_cols = ['year'] + [f'{col}_{data_source2}' for col in project_columns] + ['double_counted']
    simplified_dups = df_double_counted[merge_cols].drop_duplicates()

    # Build merge keys
    merge_on_left = ['year'] + project_columns
    merge_on_right = ['year'] + [f'{col}_{data_source2}' for col in project_columns]
    
    # Merge back to original DataFrame
    result_df = df.merge(
        simplified_dups,
        how='left',
        left_on=merge_on_left,
        right_on=merge_on_right
    )
    
    # Mark records for inclusion/exclusion
    result_df['exclude_include'] = np.where(
        result_df['double_counted'] == True,
        'Exclude',
        'Include'
    )
    
    # Handle rows not matched as duplicates
    result_df['exclude_include'] = result_df['exclude_include'].fillna('Include')
    
    # Clean up temporary columns
    for col in project_columns:
        temp_col = f'{col}_{data_source2}'
        if temp_col in result_df.columns:
            result_df = result_df.drop(columns=[temp_col])
    
    if 'double_counted' in result_df.columns:
        result_df = result_df.drop(columns=['double_counted'])
    
    return result_df


def verbose_print(messages, required_level, current_level):
    """
    Print messages based on verbosity level.
    
    Parameters:
    - messages (str or list): Message(s) to print
    - required_level (int): Required verbosity level to print
    - current_level (int): Current verbosity setting
    """
    if current_level >= required_level:
        if isinstance(messages, list):
            for msg in messages:
                print(msg)
        else:
            print(messages)


def deduplication_workflow_with_validation(s3_bucket, datasets_to_process, df_keep, output_data_path,
                                          output_validation_path, input_validation_file=None,
                                          similarity_threshold=0.65, skip_validation=False,
                                          project_columns=['project_name'], matching_columns=None, 
                                          additional_validation_columns=None, verbosity_global=2):
    """
    Run the complete deduplication workflow for two climate finance data sources with optional manual validation.
    
    Parameters:
    - s3_bucket (str): Name of the S3 bucket containing data
    - datasets_to_process (dict): Dictionary of data sources to process, where keys are df names and values are paths relative to s3_bucket
    - df_keep (str): Name of the data source to keep in case of duplicates
    - output_data_path (str): AWS folder path, relative to S3_bucket, to export deduplicated data for dataset 2 (the non df_keep one)
    - output_validation_path (str): AWS folder path, relative to S3_bucket, to export validation duplicates. 
    - input_validation_file (str, optional): AWS file path, relative to S3_bucket, to previously validated file to import
    - similarity_threshold (float, optional): Jaro-Winkler threshold for determining duplicates (0-1)
    - skip_validation (bool, optional): If True, skip manual validation and proceed with purely threshold results
    - project_columns (list, optional): List of project-related columns to use for fuzzy matching
    - matching_columns (list, optional): Columns to use for matching projects across sources
    - additional_validation_columns (list, optional): Additional columns to include in validation file
    - verbosity_global (int, optional): Global verbosity level for printing
    
    Returns:
    - tuple: (result_df, validation_file_path, stats)
      - result_df: Final Combined DataFrame with duplicate records marked
      - validation_file_path: Path to the validation file (or None if skip_validation=True)
      - stats: Statistics about the deduplication process, which can be printed in bulk.
    """
    
    # Set default matching columns if not provided
    if matching_columns is None:
        matching_columns = ['year', 'solution_cpi', 'country_destination_cpi']
    
    # First complete steps 1-4 for core deduplication logic. 
    verbose_print("Step 1: Pre-Processing Two Datasets", 2, verbosity_global)
    df1_name = df_keep
    df2_name = [k for k in datasets_to_process.keys() if k != df_keep][0]
    df1, df2, df1_exclude_only, df2_exclude_only = extract_data_sources(datasets_to_process, df1_name, df2_name, verbosity_global)

    verbose_print("Step 2: Preparing data for deduplication", 2, verbosity_global)
    df1_for_merge, df2_for_merge = prepare_dataframes_for_merge(
        df1, df2, df1_name, df2_name,
        project_columns=project_columns, validation_columns=additional_validation_columns, matching_columns=matching_columns
    )
    
    verbose_print("Step 3: Identifying potential duplicates", 2, verbosity_global)
    df_overlap = identify_potential_duplicates(
        df1_for_merge, df2_for_merge, df1_name, df2_name,
        matching_columns=matching_columns
    )
 
    verbose_print("Step 4: Computing name similarity", 2, verbosity_global)
    similarity_df = compute_name_similarity(
        df_overlap, df1_name, df2_name, project_columns, similarity_threshold
    )
    
    # Now, if skipping validation, we'll assume everything above the threshold is a duplicate!
    # We'll stop, and just export under that assumption. 
    if skip_validation:
        verbose_print("Skipping validation (step 5) - assuming everything above threshold is a duplicate!", 2, verbosity_global)
        result_df = mark_duplicates_without_validation(df2, similarity_df, df2_name, project_columns)

        verbose_print("Skipping to Step 6: Saving final deduplicated dataset", 2, verbosity_global)
        stats = {
            'total_projects': len(result_df[['project_name', 'year']].drop_duplicates()),
            'excluded_projects': len(result_df[result_df['exclude_include'] == 'Exclude'][['project_name', 'year']].drop_duplicates()),
            'excluded_value': result_df[result_df['exclude_include'] == 'Exclude']['value_USDm'].sum(),
            'remaining_value': result_df[result_df['exclude_include'] == 'Include']['value_USDm'].sum(),
            'status': 'completed_without_validation'
        }
        validation_file_path = None

        # add back already excluded data
        result_df = pd.concat([result_df, df2_exclude_only])
        aws_tools.write_to_s3(result_df, s3_bucket, output_data_path, f'{df2_name}_deduplicated_with_{df1_name}_no_checks')
        verbose_print(f"Exported successfully to s3://{s3_bucket}/{output_data_path}/{df2_name}_deduplicated_with_{df1_name}_no_checks.csv", 0, verbosity_global)
        
        return result_df, validation_file_path, stats
    
    # But... if we're not skipping validation, we'll proceed with the full validation workflow.
    # If this is the first time, we'll export the validation file and stop.
    if input_validation_file is None:
        verbose_print("Step 6a: Extracting potential duplicates for validation, then stopping", 2, verbosity_global)
        validation_df = extract_duplicates_for_validation(
            similarity_df, df1_name, df2_name,
            project_columns=project_columns, matching_columns=matching_columns,
            additional_validation_columns=additional_validation_columns
        )
        validation_file_path = export_validation_file(validation_df, s3_bucket, output_validation_path, None, df1_name, df2_name)
        num_duplicates = len(validation_df[validation_df['double_counted'] == True])
        
        without_validation_alternative = mark_duplicates_without_validation(df2, similarity_df, df2_name, project_columns)
        stats = {
            'total_projects': len(df2[project_columns + ['year']].drop_duplicates()),
            'potential_duplicates': num_duplicates,
            'potential_duplicate_value': without_validation_alternative[without_validation_alternative['exclude_include'] == 'Exclude']['value_USDm'].sum(),
            'status': 'awaiting_validation'
        }
        if num_duplicates > 0:
            verbose_print([
                f"Exported {num_duplicates} potential duplicates for validation to {validation_file_path}",
                "Please manually review the file and set 'confirm_include_no_dupes' to True/False as appropriate.",
                "Then run this workflow again with the input_validation_file parameter pointing to the validated file.",
                "Alternatively, run with skip_validation=True to apply algorithm results directly."
            ], 2, verbosity_global)
            return None, validation_file_path, stats
        else:
            verbose_print("No potential duplicates found. Skipping validation and just exporting.", 2, verbosity_global)
            # add back already excluded data
            result_df = pd.concat([without_validation_alternative, df2_exclude_only])
            aws_tools.write_to_s3(result_df, s3_bucket, output_data_path, f'{df2_name}_deduplicated_with_{df1_name}_no_checks')
            verbose_print(f"Exported successfully to s3://{s3_bucket}/{output_data_path}/{df2_name}_deduplicated_with_{df1_name}_no_checks.csv", 0, verbosity_global)
            validation_file_path = 'validated_not_needed'
            return result_df, validation_file_path, stats
    
    # Otherwise, if this is not the first time and we have indeed provided a validation file, then import it instead. 
    verbose_print("Step 6b: Import and apply validation results:", 2, verbosity_global)
    validated_df = import_validation_results(input_validation_file, s3_bucket)
    result_df = apply_validation_results(df2, validated_df, df2_name, project_columns)
    stats = {
        'total_projects': len(result_df[project_columns + ['year']].drop_duplicates()),
        'excluded_projects': len(result_df[result_df['exclude_include'] == 'Exclude'][project_columns + ['year']].drop_duplicates()),
        'excluded_value': result_df[result_df['exclude_include'] == 'Exclude']['value_USDm'].sum(),
        'remaining_value': result_df[result_df['exclude_include'] == 'Include']['value_USDm'].sum(),
        'status': 'completed_with_validation'
    }
    
    verbose_print("Step 7: Saving final deduplicated dataset (may take a bit...)", 2, verbosity_global)
    # combine with already excluded data
    result_df = pd.concat([result_df, df2_exclude_only])
    aws_tools.write_to_s3(result_df, s3_bucket, output_data_path, f'{df2_name}_deduplicated_with_{df1_name}_validated')
    verbose_print(f"Exported successfully to s3://{s3_bucket}/{output_data_path}/{df2_name}_deduplicated_with_{df1_name}_validated.csv", 0, verbosity_global)
    
    return result_df, input_validation_file, stats


def generate_validation_report(stats, data_source1, data_source2, verbosity_global):
    """
    Generate a summary report of the deduplication process for each scenario.
    
    Parameters:
    - stats (dict): Statistics about the deduplication process
    - data_source1 (str): Name of the first data source
    - data_source2 (str): Name of the second data source
    - verbosity_global (int): Current global verbosity level
    
    Returns:
    - str: Summary report
    """
    if stats['status'] == 'awaiting_validation':
        verbose_print([
            "Deduplication Summary (Awaiting Validation)",
            "-------------------------------------------",
            f"Data Sources: {data_source1} and {data_source2}",
            f"Total Projects in {data_source2}: {stats['total_projects']:,}",
            f"Potential Duplicates Identified: {stats['potential_duplicates']:,}",
            f"Potential Duplicate Value: ${stats['potential_duplicate_value']:.2f} million",
            "",
            "Please review the validation file and confirm which projects should be excluded."
        ], 2, verbosity_global)
    
    elif stats['status'] == 'completed_without_validation':
        verbose_print([
            "Deduplication Summary (Completed Without Manual Validation)",
            "----------------------------------------------------------",
            f"Data Sources: {data_source1} and {data_source2}",
            f"Total Projects in {data_source2}: {stats['total_projects']:,}",
            f"Projects Excluded Based on Algorithm: {stats['excluded_projects']:,}",
            f"Value Excluded: ${stats['excluded_value']:.2f} million",
            f"Remaining Value: ${stats['remaining_value']:.2f} million",
            f"Percentage Reduction: {(stats['excluded_value'] / (stats['excluded_value'] + stats['remaining_value']) * 100):.2f}%",
            "",
            "Note: These results were applied without manual validation."
        ], 2, verbosity_global)
    
    else:  # completed_with_validation
        verbose_print([
            "Deduplication Summary (Completed With Manual Validation)",
            "-------------------------------------------------------",
            f"Data Sources: {data_source1} and {data_source2}",
            f"Total Projects in {data_source2}: {stats['total_projects']:,}",
            f"Projects Excluded After Validation: {stats['excluded_projects']:,}",
            f"Value Excluded: ${stats['excluded_value']:.2f} million",
            f"Remaining Value: ${stats['remaining_value']:.2f} million",
            f"Percentage Reduction: {(stats['excluded_value'] / (stats['excluded_value'] + stats['remaining_value']) * 100):.2f}%"
        ], 2, verbosity_global)


################################### Functions for determining order of checks ###################################

def _build_graph_edges(mapping):
    """Return edges list (delete -> keep) and the set of all nodes."""
    edges = []
    nodes = set()
    for deleter, keepers in mapping.items():
        nodes.add(deleter)
        for k in keepers:
            nodes.add(k)
            edges.append((deleter, k))
    return edges, nodes


def _toposort_delete_keep(mapping):
    """
    Topologically order nodes so that keepers appear before any dataset that
    deletes against them. Returns a list of nodes in upstream→downstream order.
    """
    edges, nodes = _build_graph_edges(mapping)
    # Build in-degree
    indeg = {n: 0 for n in nodes}
    adj = defaultdict(list)
    for a, b in edges:
        adj[a].append(b)
        indeg[b] += 1

    # Kahn's algorithm, but note we want **keepers first**, i.e., nodes with indeg==0 are anchors
    q = deque([n for n in nodes if indeg[n] == 0])
    order = []
    while q:
        n = q.popleft()
        order.append(n)
        for m in adj[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                q.append(m)

    if len(order) != len(nodes):
        raise ValueError("Cycle detected in double-counting mapping. Please break cycles.")

    return order  # upstream → downstream


def _keepers_sorted_for(node, mapping, topo_order):
    """Return node's keep-list sorted by the topo order (upstream-first)."""
    rank = {n: i for i, n in enumerate(topo_order)}
    keepers = mapping.get(node, [])
    return sorted(keepers, key=lambda k: rank[k])