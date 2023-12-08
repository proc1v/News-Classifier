from typing import Union, Iterable, Optional, List

import spacy
from prodigy import recipe, get_stream
from prodigy.models.matcher import PatternMatcher
from prodigy.types import RecipeSettingsType
from prodigy.util import get_labels


@recipe(
    "textcat.manual_patterns",
    # fmt: off
    dataset=("Dataset to save annotations to", "positional", None, str),
    source=("Data to annotate (file path or '-' to read from standard input)", "positional", None, str),
    spacy_model=("Loadable spaCy pipeline or blank:lang (e.g. blank:en)", "positional", None, str),
    labels=("Comma-separated label(s) to annotate or text file with one label per line", "option", "l", get_labels),
    patterns=("Path to match patterns file", "option", "pt", str),
    # fmt: on
)
def manual(
    dataset: str,
    source: Union[str, Iterable[dict]],
    spacy_model: str,
    labels: Optional[List[str]] = None,
    patterns: Optional[str] = None,
) -> RecipeSettingsType:
    stream = get_stream(source, rehash=True, dedup=True, input_key="text")
    nlp = spacy.load(spacy_model)

    matcher = PatternMatcher(
        nlp,
        label_span=False,
        label_task=True,
        combine_matches=True,
        all_examples=True
    )
    matcher = matcher.from_disk(patterns)
    stream = list(matcher(stream))

    return {
        "view_id": "choice",
        "dataset": dataset,
        "stream": stream,
        "config": {
            "labels": labels,
            "choice_style": "multiple",
            "choice_auto_accept": False,
            "exclude_by": "task",
            "auto_count_stream": True,
        },
    }
