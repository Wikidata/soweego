#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A set of custom features suitable for the
`Record Linkage Toolkit <https://recordlinkage.readthedocs.io/>`_,
where feature extraction stands for *record pairs comparison*.

**Input:** pairs of :class:`list` objects
coming from Wikidata and target catalog :class:`pandas.DataFrame` columns as per
:func:`preprocess_wikidata() <soweego.linker.workflow.preprocess_wikidata>` and
:func:`preprocess_target() <soweego.linker.workflow.preprocess_target>` output.

**Output:** a *feature vector* :class:`pandas.Series`.

All classes in this module share the following constructor parameters:

- **left_on** (str) - a Wikidata column label
- **right_on** (str) - a target catalog column label
- **missing_value** - (optional) a score to fill null values
- **label** - (optional) a label for the output feature
  :class:`Series <pandas.Series>`

Specific parameters are documented in the  *__init__* method of each class.

All classes in this module implement
:class:`recordlinkage.base.BaseCompareFeature`, and can be
added to the feature extractor object :class:`recordlinkage.Compare`.

Usage::

>>> import recordlinkage as rl
>>> from soweego.linker import features
>>> extractor = rl.Compare()
>>> source_column, target_column = 'birth_name', 'fullname'
>>> feature = features.ExactMatch(source_column, target_column)
>>> extractor.add(feature)

"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import itertools
import logging
from multiprocessing import Manager
from typing import List, Set, Tuple

import jellyfish
import numpy as np
import pandas as pd
from recordlinkage.base import BaseCompareFeature
from recordlinkage.utils import fillna
from sklearn.feature_extraction.text import CountVectorizer

from soweego.commons import constants, text_utils
from soweego.wikidata import sparql_queries

LOGGER = logging.getLogger(__name__)

# When calling `SharedOccupations._expand_occupations`, it's useful to have
# a concurrent-safe dict where we cache the return value.
# In this way, all threads of a parallel feature extractor execution
# e.g., `recordlinkage.Compare(n_jobs=4)
# will have access to the dict, thus drastically decreasing
# the amount of needed SPARQL queries.
# This cache is also preserved across executions of the feature extractor.
_threading_manager = Manager()
_global_occupations_qid_cache = _threading_manager.dict()


# Adapted from https://github.com/J535D165/recordlinkage/blob/master/recordlinkage/compare.py
# See RECORDLINKAGE_LICENSE
class ExactMatch(BaseCompareFeature):
    """Compare pairs of lists through exact match on each pair of elements."""
    name = 'exact_match'
    description = (
        'Compare pairs of lists through exact match on each pair of elements.'
    )

    def __init__(
        self,
        left_on: str,
        right_on: str,
        match_value: float = 1.0,
        non_match_value: float = 0.0,
        missing_value: float = constants.FEATURE_MISSING_VALUE,
        label: str = None,
    ):
        """
        :param left_on: a Wikidata :class:`DataFrame <pandas.DataFrame>`
          column label
        :param right_on: a target catalog :class:`DataFrame <pandas.DataFrame>`
          column label
        :param match_value: (optional) a score when element pairs match
        :param non_match_value: (optional) a score when element pairs
          do not match
        :param missing_value: (optional) a score to fill null values
        :param label: (optional) a label for the output feature
          :class:`Series <pandas.Series>`
        """
        super(ExactMatch, self).__init__(left_on, right_on, label=label)
        self.match_value = match_value
        self.non_match_value = non_match_value
        self.missing_value = missing_value

    def _compute_vectorized(self, source_column, target_column):
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def exact_apply(pair):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare, the pair contains null values: %s", pair
                )
                return np.nan

            scores = []
            for source in pair[0]:
                for target in pair[1]:
                    if pd.isna(source) or pd.isna(target):
                        scores.append(self.missing_value)
                        continue
                    if source == target:
                        scores.append(self.match_value)
                    else:
                        scores.append(self.non_match_value)
            return max(scores)

        return fillna(concatenated.apply(exact_apply), self.missing_value)


class SimilarStrings(BaseCompareFeature):
    """Compare pairs of lists holding **strings**
    through similarity measures on each pair of elements.
    """
    name = 'similar_strings'
    description = (
        'Compare pairs of lists holding strings '
        'through similarity measures on each pair of elements'
    )

    def __init__(
        self,
        left_on: str,
        right_on: str,
        algorithm: str = 'levenshtein',
        threshold: float = None,
        missing_value: float = constants.FEATURE_MISSING_VALUE,
        analyzer: str = None,
        ngram_range: Tuple[int, int] = (2, 2),
        label: str = None,
    ):
        """
        :param left_on: a Wikidata :class:`DataFrame <pandas.DataFrame>`
          column label
        :param right_on: a target catalog :class:`DataFrame <pandas.DataFrame>`
          column label
        :param algorithm: (optional) ``{'cosine', 'levenshtein'}``.
          A string similarity algorithm, either the
          `cosine similarity <https://en.wikipedia.org/wiki/Cosine_similarity>`_
          or the
          `Levenshtein distance <https://en.wikipedia.org/wiki/Levenshtein_distance>`_
          respectively
        :param threshold: (optional) a threshold to filter features with
          a lower or equal score
        :param missing_value: (optional) a score to fill null values
        :param analyzer: (optional, only applies when *algorithm='cosine'*)
          ``{'soweego', 'word', 'char', 'char_wb'}``.
          A text analyzer to preprocess input. It is passed to the *analyzer*
          parameter of :class:`sklearn.feature_extraction.text.CountVectorizer`.

          - ``'soweego'`` is :func:`soweego.commons.text_utils.tokenize`
          - ``{'word', 'char', 'char_wb'}`` are *scikit* built-ins. See
            `here <https://scikit-learn.org/stable/modules/feature_extraction.html#limitations-of-the-bag-of-words-representation>`_
            for more details
          - ``None`` is :meth:`str.split`,
            and means input is already preprocessed

        :param ngram_range: (optional, only applies when *algorithm='cosine'*
          and *analyzer* is not *'soweego')*. Lower and upper boundary for
          n-gram extraction, passed to
          :class:`CountVectorizer <sklearn.feature_extraction.text.CountVectorizer>`
        :param label: (optional) a label for the output feature
          :class:`Series <pandas.Series>`
        """
        super(SimilarStrings, self).__init__(left_on, right_on, label=label)
        self.algorithm = algorithm
        self.threshold = threshold
        self.missing_value = missing_value
        self.analyzer = analyzer
        self.ngram_range = ngram_range

    def _compute_vectorized(self, source_column, target_column):
        if self.algorithm == 'levenshtein':
            algorithm = self.levenshtein_similarity

        elif self.algorithm == 'cosine':
            algorithm = self.cosine_similarity

        else:
            err_msg = (
                f'Bad string similarity algorithm: {self.algorithm}. '
                f"Please use one of ('levenshtein', 'cosine')"
            )
            LOGGER.critical(err_msg)
            raise ValueError(err_msg)

        compared = algorithm(source_column, target_column)
        compared_filled = fillna(compared, self.missing_value)

        if self.threshold is None:
            return compared_filled

        return (compared_filled >= self.threshold).astype(np.float64)

    # Adapted from
    # https://github.com/J535D165/recordlinkage/blob/master/recordlinkage/algorithms/string.py
    # Maximum edit distance among the list of values
    # TODO low scores if name is swapped with surname,
    #  see https://github.com/Wikidata/soweego/issues/175
    def levenshtein_similarity(self, source_column, target_column):
        paired = pd.Series(list(zip(source_column, target_column)))

        def levenshtein_apply(pair):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compute Levenshtein distance, "
                    "the pair contains null values: %s",
                    pair,
                )
                return np.nan

            scores = []
            source_list, target_list = pair

            for source in source_list:
                for target in target_list:
                    try:
                        score = 1 - jellyfish.levenshtein_distance(
                            source, target
                        ) / np.max([len(source), len(target)])
                        scores.append(score)
                    except TypeError:
                        if pd.isnull(source) or pd.isnull(target):
                            scores.append(self.missing_value)
                        else:
                            raise

            return max(scores)

        return paired.apply(levenshtein_apply)

    def cosine_similarity(self, source_column, target_column):
        if len(source_column) != len(target_column):
            raise ValueError('Columns must have the same length')

        if len(source_column) == len(target_column) == 0:
            LOGGER.warning("Can't compute cosine similarity, columns are empty")
            return pd.Series(np.nan)

        # This algorithm requires strings as input, but lists are expected
        source_column, target_column = (
            source_column.str.join(' '),
            target_column.str.join(' '),
        )

        # No analyzer means input underwent `commons.text_utils.tokenize`
        if self.analyzer is None:
            vectorizer = CountVectorizer(analyzer=str.split)

        elif self.analyzer == 'soweego':
            vectorizer = CountVectorizer(analyzer=text_utils.tokenize)

        # scikit-learn built-ins
        # `char` and `char_wb` make CHARACTER n-grams, instead of WORD ones:
        # they may be useful for short strings with misspellings.
        # `char_wb` makes n-grams INSIDE words,
        # thus eventually padding with whitespaces. See
        # https://scikit-learn.org/stable/modules/feature_extraction.html#limitations-of-the-bag-of-words-representation
        elif self.analyzer in ('word', 'char', 'char_wb'):
            vectorizer = CountVectorizer(
                analyzer=self.analyzer,
                strip_accents='unicode',
                ngram_range=self.ngram_range,
            )

        else:
            err_msg = (
                f'Bad text analyzer: {self.analyzer}. '
                f"Please use one of ('soweego', 'word', 'char', 'char_wb')"
            )
            LOGGER.critical(err_msg)
            raise ValueError(err_msg)

        data = source_column.append(target_column).fillna('')
        try:
            vectors = vectorizer.fit_transform(data)
        except ValueError as ve:
            LOGGER.warning(
                'Failed transforming text into vectors, reason: %s. Text: %s',
                ve,
                data,
            )
            return pd.Series(np.nan)

        def _metric_sparse_cosine(u, v):
            a = np.sqrt(u.multiply(u).sum(axis=1))
            b = np.sqrt(v.multiply(v).sum(axis=1))
            ab = v.multiply(u).sum(axis=1)
            cosine = np.divide(ab, np.multiply(a, b)).A1
            return cosine

        return _metric_sparse_cosine(
            vectors[: len(source_column)], vectors[len(source_column) :]
        )


class SimilarDates(BaseCompareFeature):
    """Compare pairs of lists holding **dates**
    through match by maximum shared precision.
    """
    name = 'similar_dates'
    description = (
        'Compare pairs of lists holding dates '
        'through match by maximum shared precision'
    )

    def __init__(
        self,
        left_on: str,
        right_on: str,
        missing_value: float = constants.FEATURE_MISSING_VALUE,
        label: str = None,
    ):
        """
        :param left_on: a Wikidata :class:`DataFrame <pandas.DataFrame>`
          column label
        :param right_on: a target catalog :class:`DataFrame <pandas.DataFrame>`
          column label
        :param missing_value: (optional) a score to fill null values
        :param label: (optional) a label for the output feature
          :class:`Series <pandas.Series>`
        """
        super(SimilarDates, self).__init__(left_on, right_on, label=label)

        self.missing_value = missing_value

    def _compute_vectorized(self, source_column, target_column):
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def check_date_equality(pair: Tuple[List[pd.Period], List[pd.Period]]):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare dates, the pair contains null values: %s",
                    pair,
                )
                return np.nan

            source_list, target_list = pair
            # Keep track of the best score
            best = 0

            for source, target in itertools.product(source_list, target_list):
                # Get precision number for both dates
                s_precision = constants.PD_PERIOD_PRECISIONS.index(
                    source.freq.name
                )
                t_precision = constants.PD_PERIOD_PRECISIONS.index(
                    target.freq.name
                )

                # Minimum pair precision = maximum shared precision
                lowest_prec = min(s_precision, t_precision)
                # Result for the current `source`
                current_result = 0

                # Loop through `pandas.Period` attributes that we can compare
                # and the precision that stands for said attribute
                for min_required_prec, d_attr in enumerate(
                    ['year', 'month', 'day', 'hour', 'minute', 'second']
                ):
                    # If both `source` and `target` have a precision which allows the
                    # current attribute to be compared then we do so. If the attribute
                    # matches then we add 1 to `current_result`, if not then we break the loop.
                    # We consider from lowest to highest precision. If a lowest
                    # precision attribute (e.g., year) doesn't match then we say that
                    # the dates don't match at all (we don't check if higher precision
                    # attributes match)
                    if lowest_prec >= min_required_prec and getattr(
                        source, d_attr
                    ) == getattr(target, d_attr):
                        current_result += 1
                    else:
                        break

                # We want a value between 0 and 1 for our score. 0 means no match at all and
                # 1 stands for perfect match. We just divide `current_result` by `lowest_prec`
                # so that we get the percentage of items that matches from the total number
                # of items we compared (since we have variable date precision)
                # we sum 1 to `lowest_prec` to account for the fact that the possible minimum
                # common precision is 0 (the year)
                best = max(best, (current_result / (lowest_prec + 1)))

            return best

        return fillna(
            concatenated.apply(check_date_equality), self.missing_value
        )


class SharedTokens(BaseCompareFeature):
    """Compare pairs of lists holding **string tokens**
    through weighted intersection.
    """
    name = 'shared_tokens'
    description = (
        'Compare pairs of lists holding string tokens '
        'through weighted intersection'
    )

    def __init__(self,
                 left_on: str,
                 right_on: str,
                 missing_value: float = constants.FEATURE_MISSING_VALUE,
                 label: str = None
    ):
        """
        :param left_on: a Wikidata :class:`DataFrame <pandas.DataFrame>`
          column label
        :param right_on: a target catalog :class:`DataFrame <pandas.DataFrame>`
          column label
        :param missing_value: (optional) a score to fill null values
        :param label: (optional) a label for the output feature
          :class:`Series <pandas.Series>`
        """
        super(SharedTokens, self).__init__(left_on, right_on, label=label)
        self.missing_value = missing_value

    def _compute_vectorized(self, source_column, target_column):
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def intersection_percentage_size(pair):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare tokens, the pair contains null values: %s",
                    pair,
                )
                return np.nan

            source_list, target_list = pair
            source_set, target_set = set(source_list), set()

            for value in target_list:
                if value:
                    target_set.update(filter(None, value.split()))

            intersection = source_set.intersection(target_set)
            count_intersect = len(intersection)
            count_total = len(source_set.union(target_set))

            # Penalize band stopwords
            count_low_score_words = len(
                text_utils.BAND_NAME_LOW_SCORE_WORDS.intersection(intersection)
            )

            return (
                (count_intersect - (count_low_score_words * 0.9)) / count_total
                if count_total > 0
                else np.nan
            )

        return fillna(
            concatenated.apply(intersection_percentage_size), self.missing_value
        )


class SharedOccupations(BaseCompareFeature):
    """Compare pairs of lists holding **occupation QIDs** *(ontology classes)*
    through expansion of the class hierarchy, plus intersection of values.
    """
    name = 'shared_occupations'
    description = (
        'Compare pairs of lists holding occupation QIDs '
        'through expansion of the class hierarchy, plus intersection of values'
    )

    def __init__(self, left_on: str, right_on: str, missing_value: float = 0.0, label: str = None):
        """
        :param left_on: a Wikidata :class:`DataFrame <pandas.DataFrame>`
          column label
        :param right_on: a target catalog :class:`DataFrame <pandas.DataFrame>`
          column label
        :param missing_value: (optional) a score to fill null values
        :param label: (optional) a label for the output feature
          :class:`Series <pandas.Series>`
        """
        super(SharedOccupations, self).__init__(left_on, right_on, label=label)

        global _global_occupations_qid_cache

        self.missing_value = missing_value
        # Set private attribute `_expand_occupations_cache`
        # to reference the global cache
        self._expand_occupations_cache = _global_occupations_qid_cache

    # This should be applied to a `pandas.Series`, where each element
    # is a set of QIDs.
    # This function will expand the set to include all superclasses and
    # subclasses of each QID in the original set.
    def _expand_occupations(self, occupation_qids: Set[str]) -> Set[str]:
        expanded_set = set()

        for qid in occupation_qids:
            expanded_set.add(qid)  # add qid to expanded set

            # Check if we have the subclasses and superclasses
            # of this specific qid in memory
            if qid in self._expand_occupations_cache:

                # if we do then add them to expanded set
                expanded_set |= self._expand_occupations_cache[qid]

            # If we don't have them then we get them from
            # wikidata and add them to the cache and
            # `expanded_set`
            else:

                # Get class hierarchy
                subclasses = sparql_queries.subclasses_of(qid)
                superclasses = sparql_queries.superclasses_of(qid)

                joined = subclasses | superclasses

                # Add joined to cache
                self._expand_occupations_cache[qid] = joined

                # Finally, add them to expanded set
                expanded_set |= joined

        return expanded_set

    def _compute_vectorized(
        self, source_column: pd.Series, target_column: pd.Series
    ):

        # we want to expand the target_column (add the
        # superclasses and subclasses of each occupation)
        target_column = target_column.apply(self._expand_occupations)

        # finally, we then zip together the source column and the target column so that
        # they're easier to process
        concatenated = pd.Series(list(zip(source_column, target_column)))

        # Given 2 sets, return the percentage of items that the
        # smaller set shares with the larger set
        def check_occupation_equality(pair: Tuple[Set[str], Set[str]]):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare occupations, "
                    "the pair contains null values: %s",
                    pair,
                )
                return np.nan

            s_item, t_item = pair

            min_length = min(len(s_item), len(t_item))
            n_shared_items = len(s_item & t_item)

            return n_shared_items / min_length

        return fillna(
            concatenated.apply(check_occupation_equality), self.missing_value
        )


class SharedTokensPlus(BaseCompareFeature):
    """Compare pairs of lists holding **string tokens**
    through weighted intersection.

    This feature is similar to :class:`SharedTokens`,
    but has extra functionality:

    - handles arbitrary stop words
    - accepts nested list of tokens
    - output score is computed FIXME metric
    """
    name = 'shared_tokens_plus'
    description = (
        'Compare pairs of lists holding string tokens '
        'through weighted intersection'
    )

    def __init__(
        self,
        left_on: str,
        right_on: str,
        missing_value: float = constants.FEATURE_MISSING_VALUE,
        label: str = None,
        stop_words: Set = None,
    ):
        """
        :param left_on: a Wikidata :class:`DataFrame <pandas.DataFrame>`
          column label
        :param right_on: a target catalog :class:`DataFrame <pandas.DataFrame>`
          column label
        :param missing_value: (optional) a score to fill null values
        :param label: (optional) a label for the output feature
          :class:`Series <pandas.Series>`
        :param stop_words: (optional) a set of
          `stop words <https://en.wikipedia.org/wiki/Stop_words>`_
          to be filtered from input pairs
        """
        super(SharedTokensPlus, self).__init__(left_on, right_on, label=label)

        self.missing_value = missing_value
        self.stop_words = stop_words

    @staticmethod
    def _flatten(list_to_flatten: List) -> List:
        to_process = [list_to_flatten]
        result = []

        while len(to_process) != 0:
            current = to_process.pop()

            for child in current:
                if isinstance(child, List):
                    to_process.append(child)
                else:
                    result.append(child)

        return result

    def _compute_vectorized(
        self, source_column: pd.Series, target_column: pd.Series
    ) -> pd.Series:

        # concatenate columns for easier processing. Here each element in
        # the columns is a set of tokens
        concatenated = pd.Series(list(zip(source_column, target_column)))

        # Compute shared tokens after filtering stop words
        def compare_apply(pair: Tuple[List[str], List[str]]) -> float:
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare, the pair contains null values: %s", pair
                )
                return np.nan

            # first we clean a bit the pair
            # make all lowercase and split on possible spaces
            # also reshape result into a list (flatten)
            pair = [
                self._flatten([el.lower().split() for el in p])
                for p in pair
            ]

            s_item, t_item = pair

            # finally convert to sets
            s_item = set(s_item)
            t_item = set(t_item)

            if self.stop_words:
                s_item -= self.stop_words
                t_item -= self.stop_words

            min_length = min(len(s_item), len(t_item))
            n_shared_items = len(s_item & t_item)

            # Prevent division by 0
            if min_length != 0:
                return n_shared_items / min_length
            else:
                return np.nan

        return fillna(concatenated.apply(compare_apply), self.missing_value)


def _pair_has_any_null(pair):
    if not all(pair):
        return True

    source_is_null, target_is_null = pd.isna(pair[0]), pd.isna(pair[1])
    if isinstance(source_is_null, np.ndarray):
        source_is_null = source_is_null.all()
    if isinstance(target_is_null, np.ndarray):
        target_is_null = target_is_null.all()

    if source_is_null or target_is_null:
        return True

    return False
