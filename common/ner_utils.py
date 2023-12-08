import os
import re
import numpy as np
import pandas as pd

from itertools import chain
from datetime import datetime
from typing import Tuple, List, Dict, Union


def spacy_annotations_to_pandas(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """
    This method converts spacy annotations to pandas
    :param df:
    :param verbose:
    :return:
    """
    final_results = {}

    for idx in df.index:
        text = df.loc[idx, 'text']
        spans = df.loc[idx, 'spans']

        final_results[idx] = {}
        final_results[idx]['notes'] = text

        for span in spans:
            if 'label' in span:
                label = span['label'].lower()
                if 'text' in span:
                    labeled_text = span['text']
                else:
                    start = span['start']
                    end = span['end']
                    labeled_text = text[start:end]

                if label not in final_results[idx]:
                    final_results[idx][label] = []
                    final_results[idx][label].append(labeled_text)
                else:
                    final_results[idx][label].append(labeled_text)

    final_results_df = pd.DataFrame(final_results).T.sort_index().fillna('')

    # Re-arrange the order
    cols_order = ['notes', 'gpe', 'loc', 'fac', 'date', 'prxy_gpe_loc_fac', 'prxy_dates']
    cols_order = [c for c in cols_order if c in final_results_df.columns]
    final_results_df = final_results_df[cols_order]

    if verbose:
        print(f"- Total number of annotated samples: {final_results_df.shape[0]}")

    return final_results_df


def read_prodigy_annotation_file(path_to_annotations: str, file: str) -> pd.DataFrame:
    """

    :param path_to_annotations:
    :param file:
    :return:
    """

    print(f"\nReading the following file: {os.path.join(path_to_annotations, file)}")

    df = pd.read_json(os.path.join(path_to_annotations, file),
                      lines=True, encoding='utf-8')
    return df


def read_prodigy_annotation_file_into_pandas(path_to_annotations: str, file: str,
                                             verbose: bool = False) -> pd.DataFrame:
    """

    :param path_to_annotations:
    :param file:
    :param verbose:
    :return:
    """

    if verbose:
        print(f"\nReading the following file: {os.path.join(path_to_annotations, file)}")

    df = pd.read_json(os.path.join(path_to_annotations, file),
                      lines=True, encoding='utf-8')
    df = spacy_annotations_to_pandas(df, verbose=verbose)
    return df


def read_reviewed_prodigy_annotation_file(path_to_annotations: str, file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """

    :param path_to_annotations:
    :param file:
    :return:
    """

    print(f"\nReading the following file: {os.path.join(path_to_annotations, file)}")

    # This DF contains both reviewed and original data - this is the standard of Prodigy
    df_annot_reviewed = pd.read_json(os.path.join(path_to_annotations, file),
                                     lines=True, encoding='utf-8')

    # Extract original annotations from the data
    df_original = pd.DataFrame(list(chain.from_iterable(df_annot_reviewed['versions'].tolist())))
    df_original.drop(['_session_id', 'sessions', 'default'], axis=1, inplace=True)

    # Extract reviewed annotations from the data
    df_reviewed = df_annot_reviewed.copy()
    df_reviewed['_view_id'] = 'ner_manual'

    cols_to_drop = [c for c in ['sessions', 'view_id', 'label', '_session_id', 'versions'] if
                    c in df_reviewed.columns.tolist()]
    df_reviewed.drop(cols_to_drop, axis=1, inplace=True)

    return df_reviewed, df_original


def read_reviewed_prodigy_annotation_file_into_pandas(path_to_annotations: str, file: str, verbose: bool = False) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
    """

    :param path_to_annotations:
    :param file:
    :param verbose:
    :return:
    """

    df_reviewed, df_original = read_reviewed_prodigy_annotation_file(path_to_annotations=path_to_annotations, file=file)

    # Convert to human readable format
    df_original = spacy_annotations_to_pandas(df=df_original, verbose=verbose)
    df_reviewed = spacy_annotations_to_pandas(df=df_reviewed, verbose=verbose)

    return df_reviewed, df_original


def prepare_comparison_of_annotations(ner_input_col: str, list_of_tags: List[str], assignees: List[str],
                                      path_to_annotations_rootdir: str, select_annot_file_containing_str: str,
                                      annotations_reviewed: bool = False, verbose: bool = False):
    """

    :param ner_input_col:
    :param list_of_tags:
    :param assignees:
    :param path_to_annotations_rootdir:
    :param select_annot_file_containing_str:
    :param annotations_reviewed:
    :param verbose:
    :return:
    """

    if annotations_reviewed:
        print(">>> NOTE: WE ARE GOING TO COMPARE REVIEWED ANNOTATIONS !!!")

    # ===========================================
    # Read jsonl files in each directory
    # ===========================================

    all_results = []
    for i, assignee in enumerate(assignees):
        path_to_annotations = os.path.join(path_to_annotations_rootdir, assignee)

        if os.path.exists(path_to_annotations):
            files = os.listdir(path_to_annotations)

            if verbose:
                print(f"Found annotations files: {files}")

            if files:
                file = [f for f in files if select_annot_file_containing_str in f]

                if verbose:
                    print(f"Annotation file of interest: {file}")

                if file:
                    file = file[0]

                    print(f"\nReading annotation file {os.path.join(path_to_annotations, file)} ...")

                    if not annotations_reviewed:
                        df = read_prodigy_annotation_file_into_pandas(path_to_annotations=path_to_annotations,
                                                                      file=file)
                    else:
                        df_reviewed, df_original = read_reviewed_prodigy_annotation_file_into_pandas(
                            path_to_annotations=path_to_annotations,
                            file=file)
                        # We are going to compare reviewed results
                        df = df_reviewed

                    rename_dict = {c: '_'.join([c, assignee.lstrip('_')]) for c in list_of_tags}
                    df.rename(columns=rename_dict, inplace=True)
                    all_results.append(df)
                else:
                    print(
                        f"There is no annotation file from {assignee} that contains "
                        f"'{select_annot_file_containing_str}' in its name")
            else:
                print(f"There are no annotations from {assignee}")
        else:
            print(f"There are no annotations from {assignee}")

    # ===========================================
    # Merge all labelers results to a single DF
    # ===========================================
    print("")
    for i in range(len(all_results)):
        print(f"Assignee: {assignees[i]}, size of DF: {all_results[i].shape[0]}")
        if i == 0:
            all_results_df = all_results[i]
        else:
            all_results_df = all_results_df.merge(all_results[i], how='left', on=ner_input_col)

    # ===========================================
    # Re-arrange columns
    # ===========================================

    cols_order = []
    list_of_columns_by_tag = {}
    for tag in list_of_tags:
        cols_covered_by_tag = [c for c in all_results_df.columns if c.startswith(tag)]

        # Add count of unique responses per each tag
        all_results_df[tag + '_nunique'] = all_results_df[cols_covered_by_tag].astype(str).nunique(axis=1)
        cols_covered_by_tag += [tag + '_nunique']

        cols_order.append(cols_covered_by_tag)
        list_of_columns_by_tag[tag] = cols_covered_by_tag

    cols_order = list(chain(*cols_order))
    all_results_df = all_results_df[[ner_input_col] + cols_order]

    # ===========================================
    # Convert to string values in all columns
    # ===========================================
    for c in cols_order:
        if '_nunique' not in c:
            all_results_df[c] = all_results_df[c].astype(str)

    # all_results_df = all_results_df.replace({'': np.nan})
    all_results_df = all_results_df.replace({'nan': '', np.nan: ''})

    return all_results_df, list_of_columns_by_tag


def show_labeling_progress(path_to_annotations: str, file: str, analyst: str) -> pd.DataFrame:
    """

    :param path_to_annotations:
    :param file:
    :param analyst:
    :return:
    """

    progress_df = pd.read_json(os.path.join(path_to_annotations, file),
                               lines=True, encoding='utf-8')

    progress_df['timestamp'] = progress_df['_timestamp'].apply(datetime.fromtimestamp)
    progress_df['date'] = pd.to_datetime(progress_df['timestamp'].dt.date)

    progress_df = progress_df.groupby(['date']).size().reset_index().rename(columns={0: 'n_annotations'})
    progress_df.insert(loc=0, column='analyst', value=analyst)
    progress_df['n_annotations_cumsum'] = progress_df['n_annotations'].cumsum()
    progress_df['filename'] = file

    return progress_df[['analyst', 'date', 'n_annotations_cumsum', 'n_annotations', 'filename']]


def get_disagreement_rate_per_each_tag(annotations_df, list_of_columns_by_tag: Dict, assignees: List) -> Dict:
    """

    :param annotations_df:
    :param list_of_columns_by_tag:
    :param assignees:
    :return:
    """

    print(f"Disagreement rate for: {assignees}\n")

    diasgreement_rate_per_each_tag = {}

    for col in list_of_columns_by_tag:
        mask_disagreements = annotations_df[col + '_nunique'] != 1
        disagreement_rate = round(annotations_df.loc[mask_disagreements].shape[0] / annotations_df.shape[0] * 100., 2)
        diasgreement_rate_per_each_tag[col] = disagreement_rate

        space = '\t\t' if col != 'prxy_gpe_loc_fac' else '\t'
        print(f"- [{col}]:{space}{annotations_df.loc[mask_disagreements].shape[0]} / "
              f"{annotations_df.shape[0]} ({disagreement_rate}%)")

    return diasgreement_rate_per_each_tag


def get_number_of_null_entries_per_each_tag(annotations_df, list_of_columns_by_tag: Dict, assignees: List) -> Dict:
    """

    :param annotations_df:
    :param list_of_columns_by_tag:
    :param assignees:
    :return:
    """

    print(f"Number of null entries for each tag made by: {assignees}\n")

    number_null_entries_per_each_tag = {}

    for col in list_of_columns_by_tag:
        col_set = [c for c in list_of_columns_by_tag[col] if '_nunique' not in c]
        stats = annotations_df[col_set].replace({'': np.nan}).isnull().sum()
        stats.index = stats.index.str.rsplit('_').str[-1]
        number_null_entries_per_each_tag[col] = stats.to_dict()
        space = '\t\t' if col != 'prxy_gpe_loc_fac' else '\t'
        print(f"- [{col}]:{space}{number_null_entries_per_each_tag[col]}")

    return number_null_entries_per_each_tag


def compare_annotations_pre_and_post_review(assignee: str, path_to_annotations_rootdir: str,
                                            list_of_tags: List[str], ner_input_col: str) \
        -> Tuple[pd.DataFrame, Dict]:
    """

    :param assignee:
    :param path_to_annotations_rootdir:
    :param list_of_tags:
    :param ner_input_col:
    :return:
    """

    path_to_annotations = os.path.join(path_to_annotations_rootdir, assignee)

    if assignee == '_nazar':
        file_reviewed = 'acled_gpe_loc_fac_date_train_100_samples_nazar_reviewed.jsonl'
    else:
        file_reviewed = f'acled_gpe_loc_fac_date_train_200_samples_{assignee}_annot_reviewed.jsonl'

    df_reviewed, df_original = read_reviewed_prodigy_annotation_file_into_pandas(
        path_to_annotations=path_to_annotations,
        file=file_reviewed)

    orig_annot_cols = {}
    reviewed_annot_cols = {}
    for c in list_of_tags:
        orig_annot_cols[c] = f"{c}_orig"
        reviewed_annot_cols[c] = f"{c}_rev"

    df_reviewed.rename(columns=reviewed_annot_cols, inplace=True)
    df_original.rename(columns=orig_annot_cols, inplace=True)

    compare_df = df_reviewed.merge(df_original, how='left', on=ner_input_col)

    cols_order = []
    orig_review_columns_by_tag = {}
    for tag in list_of_tags:
        cols_covered_by_tag = [c for c in compare_df.columns if c.startswith(tag)]

        # Add count of unique responses per each tag
        compare_df[tag + '_nunique'] = compare_df[cols_covered_by_tag].astype(str).nunique(axis=1)
        cols_covered_by_tag += [tag + '_nunique']

        cols_order.append(cols_covered_by_tag)
        orig_review_columns_by_tag[tag] = cols_covered_by_tag

    cols_order = list(chain(*cols_order))
    compare_df = compare_df[[ner_input_col] + cols_order]
    return compare_df, orig_review_columns_by_tag


def compare_annotations_pre_and_post_smart_review(path_to_original_annots: str,
                                                  path_to_reviewed_annots: str,
                                                  fn_original: str,
                                                  fn_reviewed: str,
                                                  list_of_tags: List[str],
                                                  ner_input_col: str,
                                                  verbose: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """

    :param path_to_original_annots:
    :param path_to_reviewed_annots:
    :param fn_original:
    :param fn_reviewed:
    :param list_of_tags:
    :param ner_input_col:
    :param verbose:
    :return:
    """

    df_original = read_prodigy_annotation_file_into_pandas(path_to_annotations=path_to_original_annots,
                                                           file=fn_original, verbose=verbose)

    df_reviewed = read_prodigy_annotation_file_into_pandas(path_to_annotations=path_to_reviewed_annots,
                                                           file=fn_reviewed, verbose=verbose)

    orig_annot_cols = {}
    reviewed_annot_cols = {}
    for c in list_of_tags:
        orig_annot_cols[c] = f"{c}_orig"
        reviewed_annot_cols[c] = f"{c}_rev"

    df_original.rename(columns=orig_annot_cols, inplace=True)
    df_reviewed.rename(columns=reviewed_annot_cols, inplace=True)

    # De-dupe on NER input column
    df_original = df_original.drop_duplicates(subset=[ner_input_col])
    df_original.reset_index(drop=True, inplace=True)

    df_reviewed = df_reviewed.drop_duplicates(subset=[ner_input_col])
    df_reviewed.reset_index(drop=True, inplace=True)

    if verbose:
        print(f"\n- Size of original dataset after de-duplication on {ner_input_col}: {df_original.shape[0]}")
        print(f"- Size of reviewed dataset after de-duplication on {ner_input_col}: {df_reviewed.shape[0]}")

    # Merge original and reviewed DFs
    compare_df = df_reviewed.merge(df_original, how='left', on=ner_input_col)

    if verbose:
        print("\n>>> Comparing annotations pre- and post- smart review (highest disagreement samples first)")
        print("\nStats about introduced modifications during the review:")

    cols_order = []
    pct_samples_modified_df = []
    orig_review_columns_by_tag = {}
    for tag in list_of_tags:
        cols_covered_by_tag = [c for c in compare_df.columns if c.startswith(tag)]

        # Add count of unique responses per each tag
        compare_df[tag + '_nunique'] = compare_df[cols_covered_by_tag].astype(str).nunique(axis=1)
        cols_covered_by_tag += [tag + '_nunique']

        if verbose:
            n_samples_modified = compare_df[compare_df[tag + '_nunique'] > 1].shape[0]
            pct_samples_modified = round(n_samples_modified / compare_df.shape[0] * 100., 2)
            print(f"- [{tag}]: {n_samples_modified} / {compare_df.shape[0]} ({pct_samples_modified}%)")
            pct_samples_modified_df.append((tag, n_samples_modified, compare_df.shape[0], pct_samples_modified))

        cols_order.append(cols_covered_by_tag)
        orig_review_columns_by_tag[tag] = cols_covered_by_tag

    cols_order = list(chain(*cols_order))
    compare_df = compare_df[[ner_input_col] + cols_order]
    
    # Summary on modifications during the review process (per each tag)
    pct_samples_modified_df = pd.DataFrame(pct_samples_modified_df, columns=['tag',
                                                                             'n_samples_modified',
                                                                             'n_samples_reviewed',
                                                                             'pct_samples_modified'])
    
    return compare_df, pct_samples_modified_df, orig_review_columns_by_tag


def get_most_recent_annot_file_in_dir(path_to_dir: str) -> Union[str, None]:
    """
    Get most recent annotation file in the labeling output directory (based on datetime in its name)
    :param path_to_dir:
    :return:
    """
    
    annot_files = [f for f in os.listdir(path_to_dir) if re.search("([0-9]{4}\_[0-9]{2}\_[0-9]{2})", f)]
    annot_files_dates = sorted(set([re.search("([0-9]{4}\_[0-9]{2}\_[0-9]{2})", f).group(1) for f in annot_files]))
    latest_annot_file_date = max([datetime.strptime(d, "%Y_%m_%d") for d in annot_files_dates]).strftime("%Y_%m_%d")
    latest_annot_file = [f for f in annot_files if latest_annot_file_date in f]
    
    if latest_annot_file:
        print(f"Latest annotation file in {path_to_dir}: {latest_annot_file[0]}")
        return latest_annot_file[0]
    
    return
