import re
from re import Pattern
from retrie.retrie import Checklist

SPACE_REGEX = re.compile(r"\s+")
NUMERIC_REGEX = re.compile("[0-9]")
NON_ALPHA_NUMERIC_REGEX = re.compile(r"[^a-z0-9]", re.IGNORECASE)
WORD_REGEX = re.compile(r"(\s|^|\()[a-zA-Z]{2,}(\s|$|[,.\):;])")
PUNCTUATION_REGEX = re.compile(r"""[=\?.,\/\\><:;'"\[\]()!%$*|^\-~`+#]""")


##############################
#   CLEANING STRINGS         #
##############################

def squash_spaces(s, space_re: Pattern = SPACE_REGEX) -> str:
    return re.sub(space_re, " ", s) if isinstance(s, str) else s


def remove_spaces(s, space_re: Pattern = SPACE_REGEX) -> str:
    return re.sub(space_re, "", s) if isinstance(s, str) else s


def sub_non_alpha_numeric_to_spaces(s: str) -> str:
    return squash_spaces(NON_ALPHA_NUMERIC_REGEX.sub(" ", s)) if isinstance(s, str) else s


def remove_non_alpha_numeric(s: str) -> str:
    return NON_ALPHA_NUMERIC_REGEX.sub("", s) if isinstance(s, str) else s


def nullify_if_no_words(text: str) -> str:
    """
    Checks if there are any proper words, defined as sequences of alphabetical characters between spaces
    """
    match = WORD_REGEX.search(text)
    return text if text and match else ""


##############################
# PREDEFINED REGEXES         #
##############################


def construct_distinct_word_regex(word_array) -> Pattern:
    word_regex_trie = Checklist(word_array, match_substrings=False, re_flags=False)
    return word_regex_trie.compiled
