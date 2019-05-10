#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of techniques to compare record pairs (read extract features) for probabilistic linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import itertools
import logging
from multiprocessing import Manager, Process
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
_threading_manager = Manager()


# when expanding the QID in `OccupationQidSet._expand_occupations` it
# is useful to have a concurrent dict where we can cache each result, so that when this
# feature extractor is used executed in parallel, all instances will have
# access to the same cache, drastically decreasing the number of sparql
# requests that we need to make. This cache is also preserved across executions
# of the feature extractor.
_global_occupations_qid_cache = _threading_manager.dict()


# Adapted from https://github.com/J535D165/recordlinkage/blob/master/recordlinkage/compare.py
# See RECORDLINKAGE_LICENSE


class StringList(BaseCompareFeature):
    name = 'string_list'
    description = 'Compare pairs of lists with string values'

    def __init__(self,
                 left_on,
                 right_on,
                 algorithm='levenshtein',
                 threshold=None,
                 missing_value=constants.FEATURE_MISSING_VALUE,
                 analyzer=None,
                 ngram_range=(2, 2),
                 label=None):
        super(StringList, self).__init__(left_on, right_on, label=label)
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
            raise ValueError(
                'Bad string similarity algorithm: %s. Please use one of %s' % (
                    self.algorithm, ('levenshtein', 'cosine')))

        compared = algorithm(source_column, target_column)
        compared_filled = fillna(compared, self.missing_value)

        if self.threshold is None:
            return compared_filled
        return (compared_filled >= self.threshold).astype(np.float64)

    # Adapted from https://github.com/J535D165/recordlinkage/blob/master/recordlinkage/algorithms/string.py
    # Average the edit distance among the list of values
    # TODO low scores if name is swapped with surname, see https://github.com/Wikidata/soweego/issues/175
    def levenshtein_similarity(self, source_column, target_column):
        paired = pd.Series(list(zip(source_column, target_column)))

        def _levenshtein_apply(pair):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compute Levenshtein distance, the pair contains null values: %s", pair)
                return np.nan

            scores = []
            source_values, target_values = pair

            for source in source_values:
                for target in target_values:
                    try:
                        score = 1 - jellyfish.levenshtein_distance(source, target) \
                            / np.max([len(source), len(target)])
                        scores.append(score)
                    except TypeError:
                        if pd.isnull(source) or pd.isnull(target):
                            scores.append(self.missing_value)
                        else:
                            raise
            return max(scores)

        return paired.apply(_levenshtein_apply)

    def cosine_similarity(self, source_column, target_column):
        if len(source_column) != len(target_column):
            raise ValueError('Columns must have the same length')
        if len(source_column) == len(target_column) == 0:
            LOGGER.warning(
                "Can't compute cosine similarity, columns are empty")
            return pd.Series(np.nan)

        # This algorithm requires strings as input, but lists are expected
        source_column, target_column = source_column.str.join(
            ' '), target_column.str.join(' ')

        # No analyzer means input underwent commons.text_utils#tokenize
        if self.analyzer is None:
            vectorizer = CountVectorizer(analyzer=str.split)
        elif self.analyzer == 'soweego':
            vectorizer = CountVectorizer(analyzer=text_utils.tokenize)
        # scikit-learn built-ins
        # 'char' and char_wb' make CHARACTER n-grams, instead of WORD ones, may be useful for short strings with misspellings.
        # 'char_wb' makes n-grams INSIDE words, thus eventually padding with whitespaces.
        # See https://scikit-learn.org/stable/modules/feature_extraction.html#limitations-of-the-bag-of-words-representation
        elif self.analyzer in ('word', 'char', 'char_wb'):
            vectorizer = CountVectorizer(
                analyzer=self.analyzer, strip_accents='unicode', ngram_range=self.ngram_range)
        else:
            err_msg = f"Bad text analyzer: {self.analyzer}. Please use one of 'soweego', 'word', 'char', 'char_wb'"
            LOGGER.critical(err_msg)
            raise ValueError(err_msg)

        data = source_column.append(target_column).fillna('')
        try:
            vectors = vectorizer.fit_transform(data)
        except ValueError as ve:
            LOGGER.warning(
                'Failed transforming text into vectors, reason: %s. Text: %s', ve, data)
            return pd.Series(np.nan)

        def _metric_sparse_cosine(u, v):
            a = np.sqrt(u.multiply(u).sum(axis=1))
            b = np.sqrt(v.multiply(v).sum(axis=1))
            ab = v.multiply(u).sum(axis=1)
            cosine = np.divide(ab, np.multiply(a, b)).A1
            return cosine

        return _metric_sparse_cosine(vectors[:len(source_column)], vectors[len(source_column):])


class ExactList(BaseCompareFeature):
    name = 'exact_list'
    description = 'Compare pairs of lists through exact match on each pair of elements.'

    def __init__(self, left_on, right_on, agree_value=1.0, disagree_value=0.0,
                 missing_value=constants.FEATURE_MISSING_VALUE, label=None):
        super(ExactList, self).__init__(left_on, right_on, label=label)
        self.agree_value = agree_value
        self.disagree_value = disagree_value
        self.missing_value = missing_value

    def _compute_vectorized(self, source_column, target_column):
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def exact_apply(pair):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare, the pair contains null values: %s", pair)
                return np.nan

            scores = []
            for source in pair[0]:
                for target in pair[1]:
                    if pd.isna(source) or pd.isna(target):
                        scores.append(self.missing_value)
                        continue
                    if source == target:
                        scores.append(self.agree_value)
                    else:
                        scores.append(self.disagree_value)
            return max(scores)

        return fillna(concatenated.apply(exact_apply), self.missing_value)


class DateCompare(BaseCompareFeature):
    """
    Compares `pandas.Period` date objects, taking into
    account their maximum precisions.
    """

    name = "date_compare"
    description = "Compares the date attribute of record pairs."

    def __init__(self,
                 left_on,
                 right_on,
                 missing_value=constants.FEATURE_MISSING_VALUE,
                 label=None):
        super(DateCompare, self).__init__(left_on, right_on, label=label)

        self.missing_value = missing_value

    def _compute_vectorized(self, source_column, target_column):

        # we zip together the source column and the target column so that
        # they're easier to process
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def check_date_equality(pair: Tuple[List[pd.Period], List[pd.Period]]):
            """
            Compares the target pd.Periods with the source pd.Periods which represent either
            a birth or a death date.

            Returns the most optimistic match
            """

            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare dates, one of the values is NaN. Pair: %s", pair)
                return np.nan

            s_items, t_items = pair

            # will help us to keep track of the best score
            best = 0

            for s_date, t_date in itertools.product(s_items, t_items):

                # get precision number for both dates
                s_precision = constants.PD_PERIOD_PRECISIONS.index(
                    s_date.freq.name)
                t_precision = constants.PD_PERIOD_PRECISIONS.index(
                    t_date.freq.name)

                # we choose to compare on the lowest precision
                # since it's the maximum precision on which both
                # dates can be safely compared
                lowest_prec = min(s_precision, t_precision)

                # the result for the current `s_date`
                c_r = 0

                # now we loop through the possible `Period` attributes that we can compare
                # and the precision that stands for said attribute
                for min_required_prec, d_attr in enumerate(['year', 'month', 'day', 'hour', 'minute', 'second']):

                    # If both `s_date` and `t_date` have a precision which allows the
                    # current attribute to be compared then we do so. If the attribute
                    # matches then we add 1 to `c_r`, if not then we break the loop.
                    # We consider from lowest to highest precision. If a lowest
                    # precision attribute (ie, year) doesn't match then we say that
                    # the dates don't match at all (we don't check if higher precision
                    # attributes match)
                    if lowest_prec >= min_required_prec and getattr(s_date, d_attr) == getattr(t_date, d_attr):
                        c_r += 1
                    else:
                        break

                # we want a value between 0 and 1 for our score. 0 means no match at all and
                # 1 stands for perfect match. So we just divide `c_r` by `lowest_prec`
                # so that we get the percentage of items that matches from the total number
                # of items we compared (since we have variable date precision)
                # we sum 1 to `lowers_prec` to account for the fact that the possible minimum
                # common precision is 0 (the year)
                best = max(best, (c_r / (lowest_prec + 1)))

            return best

        return fillna(concatenated.apply(check_date_equality), self.missing_value)


class SimilarTokens(BaseCompareFeature):
    name = 'SimilarTokens'
    description = 'Compare pairs of lists with string values based on shared tokens.'

    def __init__(self, left_on, right_on, agree_value=1.0, disagree_value=0.0,
                 missing_value=constants.FEATURE_MISSING_VALUE, label=None):
        super(SimilarTokens, self).__init__(left_on, right_on, label=label)
        self.agree_value = agree_value
        self.disagree_value = disagree_value
        self.missing_value = missing_value

    def _compute_vectorized(self, source_column, target_column):
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def intersection_percentage_size(pair):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare Tokens, the pair contains null values: %s", pair)
                return np.nan

            source_labels, target_labels = pair

            first_set = set(source_labels)
            second_set = set()

            for label in target_labels:
                if label:
                    second_set.update(filter(None, label.split()))

            intersection = first_set.intersection(second_set)
            count_intersect = len(intersection)
            count_total = len(first_set.union(second_set))

            # Penalize band stopwords
            count_low_score_words = len(
                text_utils.BAND_NAME_LOW_SCORE_WORDS.intersection(intersection))

            return (count_intersect - (count_low_score_words * 0.9)) / count_total if count_total > 0 else np.nan

        return fillna(concatenated.apply(intersection_percentage_size), self.missing_value)


class OccupationQidSet(BaseCompareFeature):
    name = 'occupation_qid_set'
    description = 'Compare pairs of sets containing occupation QIDs.'

    def __init__(self,
                 left_on,
                 right_on,
                 missing_value=0.0,
                 label=None):
        super(OccupationQidSet, self).__init__(left_on, right_on, label=label)

        # declare that we want to use the global qid cache
        global _global_occupations_qid_cache

        self.missing_value = missing_value

        # set instance `_expand_occupations_cache` to reference the global
        # cache.
        self._expand_occupations_cache = _global_occupations_qid_cache

    def _expand_occupations(self, occupation_qids: Set[str]) -> Set[str]:
        """
        This should be applied to a pandas.Series, where each element
        is a set of QIDs.

        This function will expand the set to include all superclasses and
        subclasses of each QID in the original set.
        """

        expanded_set = set()

        for qid in occupation_qids:
            expanded_set.add(qid)  # add qid to expanded set

            # check if we have the subclasses and superclasses
            # of this specific qid in memory
            if qid in self._expand_occupations_cache:

                # if we do then add them to expanded set
                expanded_set |= self._expand_occupations_cache[qid]

            # if we don't have them then we get them from
            # wikidata and add them to the cache and
            # `expanded_set`
            else:

                # get subclasses and superclasses
                subclasses = sparql_queries.get_subclasses_of_qid(qid)
                superclasses = sparql_queries.get_superclasses_of_qid(qid)

                joined = subclasses | superclasses

                # add joined to cache
                self._expand_occupations_cache[qid] = joined

                # finally add them to expanded set
                expanded_set |= joined

        return expanded_set

    def _compute_vectorized(self, source_column: pd.Series, target_column: pd.Series):

        # we want to expand the target_column (add the
        # superclasses and subclasses of each occupation)
        target_column = target_column.apply(self._expand_occupations)

        # finally, we then zip together the source column and the target column so that
        # they're easier to process
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def check_occupation_equality(pair: Tuple[Set[str], Set[str]]):
            """Given 2 sets, returns the percentage of items that the
            smallest set shares with the larger set"""

            # explicitly check if set is empty
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare occupations, either the Wikidata or Target value is null. Pair: %s", pair)
                return np.nan

            s_item: Set
            t_item: Set
            s_item, t_item = pair

            min_length = min(len(s_item), len(t_item))
            n_shared_items = len(s_item & t_item)

            return n_shared_items / min_length

        return fillna(concatenated.apply(check_occupation_equality), self.missing_value)


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
