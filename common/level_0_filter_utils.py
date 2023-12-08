import re
import json
from typing import List

import pandas as pd
import numpy as np
import spacy

import level_0_filter_key_words as fltr


def base_filter(text: str) -> str:
    if isinstance(text, str):
        text_cleaned = re.sub(fltr.PUNCTUATION_REGEX, " ", text)

        relevant_body = []
        for regex in fltr.relevant_regex_list:
            body = [x.strip() for x in sorted((set(regex.findall(text_cleaned))))]
            relevant_body.append(bool(body))

        not_relevant_body = []
        for regex in fltr.not_relevant_regex_list:
            body = [x.strip() for x in sorted((set(regex.findall(text_cleaned))))]
            not_relevant_body.append(bool(body))

        has_relevant_words = any(relevant_body)
        has_not_relevant_words = any(not_relevant_body)

        if has_relevant_words and not has_not_relevant_words:
            return "RELEVANT"
        else:
            return "NOT_RELEVANT"

    return None


def base_filter_for_dataframe(df_input: pd.DataFrame) -> pd.DataFrame:
    df = df_input.copy(deep=True)

    categories = fltr.relevant_categories_names + fltr.not_relevant_categories_names
    regexes = fltr.relevant_regex_list + fltr.not_relevant_regex_list

    for category, regex in zip(categories, regexes):
        col_name = 'is_' + category
        body_name = category + '_body'
        df[body_name] = df['text'].str.lower().apply(
            lambda x: re.sub(fltr.PUNCTUATION_REGEX, " ", x) if isinstance(x, str) else x)
        df[body_name] = df[body_name].apply(regex.findall)
        df[body_name] = df[body_name].apply(lambda x: [x.strip() for x in sorted(set(x))])
        df[col_name] = 0
        df.loc[df[body_name].astype(str) != '[]', col_name] = 1

    df['has_relevant_words'] = df[['is_' + cat for cat in fltr.relevant_categories_names]].apply(any, axis=1).astype(
        int)
    df['has_not_relevant_words'] = df[['is_' + cat for cat in fltr.not_relevant_categories_names]].apply(any,
                                                                                                         axis=1).astype(
        int)

    df['base_filter'] = np.all(df[['has_relevant_words', 'has_not_relevant_words']].values == [1, 0], axis=1)
    df['base_filter_int'] = df['base_filter'].astype(int)
    df['base_filter'] = df['base_filter'].map({True: "RELEVANT", False: "NOT_RELEVANT"})

    return df


def find_keywords_for_dataframe(df_input: pd.DataFrame, unique: bool = True) -> pd.DataFrame:
    df = df_input.copy(deep=True)

    categories = fltr.relevant_categories_names + fltr.not_relevant_categories_names
    regexes = fltr.relevant_regex_list + fltr.not_relevant_regex_list

    for category, regex in zip(categories, regexes):
        col_name = 'is_' + category
        body_name = category + '_body'
        df[body_name] = df['text'].str.lower().apply(
            lambda x: re.sub(fltr.PUNCTUATION_REGEX, " ", x) if isinstance(x, str) else x)
        df[body_name] = df[body_name].apply(regex.findall)
        if unique:
            df[body_name] = df[body_name].apply(lambda x: [x.strip() for x in sorted(set(x))])
        else:
            df[body_name] = df[body_name].apply(lambda x: [x.strip() for x in sorted(x)])
        df[col_name] = 0
        df.loc[df[body_name].astype(str) != '[]', col_name] = 1

    df['has_relevant_words'] = df[['is_' + cat for cat in fltr.relevant_categories_names]].apply(any, axis=1).astype(
        int)
    df['has_not_relevant_words'] = df[['is_' + cat for cat in fltr.not_relevant_categories_names]].apply(any,
                                                                                                         axis=1).astype(
        int)

    return df


def create_prodigy_patterns(key_words_list: List[str], label: str) -> str:
    final_pattern = ''
    nlp = spacy.blank('en')

    for key_words in key_words_list:
        pattern = []
        for token in nlp.make_doc(key_words):
            if token.is_alpha:
                pattern.append({'lower': token.lower_})
            else:
                pattern.append({'orth': token.lower_})

        final_pattern += json.dumps({'label': label, 'pattern': pattern}) + '\n'

    return final_pattern
