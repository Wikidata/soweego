#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of techniques to compare record pairs (read extract features) for probabilistic linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
from typing import List, Set, Tuple, Union

import jellyfish
import numpy as np
import pandas as pd
from recordlinkage.base import BaseCompareFeature
from recordlinkage.utils import fillna
from sklearn.feature_extraction.text import CountVectorizer

from soweego.commons import constants, text_utils
from soweego.wikidata import sparql_queries

LOGGER = logging.getLogger(__name__)


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
                 missing_value=0.0,
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
            # Paranoid checks to ensure we work on lists
            if isinstance(source_values, str):
                source_values = [source_values]
            if isinstance(target_values, str):
                target_values = [target_values]

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
            avg = np.average(scores)
            return avg

        return paired.apply(_levenshtein_apply)

    # TODO move this method to another class: the measure doesn't actually work on LISTS, it assumes joined descriptions as per workflow#_join_descriptions
    def cosine_similarity(self, source_column, target_column):
        if len(source_column) != len(target_column):
            raise ValueError('Columns must have the same length')
        if len(source_column) == len(target_column) == 0:
            LOGGER.warning(
                "Can't compute cosine similarity, columns are empty")
            return pd.Series(np.nan)

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
            raise ValueError(
                'Bad text analyzer: %s. Please use one of %s' % (self.analyzer, ('soweego', 'word', 'char', 'char_wb')))

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


class UrlList(BaseCompareFeature):
    name = 'url_list'
    description = 'Compare pairs of lists with URL values'

    def __init__(self, left_on, right_on, agree_value=1.0, disagree_value=0.0, missing_value=0.0, label=None):
        super(UrlList, self).__init__(left_on, right_on, label=label)
        self.agree_value = agree_value
        self.disagree_value = disagree_value
        self.missing_value = missing_value

    def _compute_vectorized(self, source_column, target_column):
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def exact_apply(pair):
            if _pair_has_any_null(pair):
                LOGGER.debug(
                    "Can't compare URLs, the pair contains null values: %s", pair)
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
            return np.average(scores)

        return fillna(concatenated.apply(exact_apply), self.missing_value)


class DateCompare(BaseCompareFeature):
    """
    Compares `pandas.Period` date objects, taking into
    account their maximum precisions.
    """

    name = "DateCompare"
    description = "Compares the date attribute of record pairs."

    def __init__(self,
                 left_on,
                 right_on,
                 missing_value=0.0,
                 label=None):
        super(DateCompare, self).__init__(left_on, right_on, label=label)

        self.missing_value = missing_value

    def _compute_vectorized(self, source_column, target_column):

        # we zip together the source column and the target column so that
        # they're easier to process
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def check_date_equality(pair: Tuple[Union[pd.Period, List[pd.Period]], pd.Period]):
            """
            Compares a target pd.Period with the source pd.Periods which represent either
            a birth or death date. The source date can be a list of possible dates.

            Returns the most optimistic match
            """

            s_item, t_item = pair

            # if t_item is NaT then we can't compare, and we skip this pair
            if pd.isna(t_item):
                LOGGER.debug(
                    "Can't compare dates, the target value is null. Pair: %s", pair)
                return np.nan

            # convert `s_item` to a list if it isn't already
            if not isinstance(s_item, list):
                s_item = [s_item]

            # will help us to keep track of the best score
            best = 0

            for s_date in s_item:

                # if the current s_date is NaT then we can't compare, so we skip it
                if pd.isna(s_date):
                    LOGGER.debug(
                        "Can't compare dates, the current Wikidata value is null. Current pair: %s", (s_date, t_item))
                    continue

                # get precision number for both dates
                s_precision = constants.PD_PERIOD_PRECISIONS.index(
                    s_date.freq.name)
                t_precision = constants.PD_PERIOD_PRECISIONS.index(
                    t_item.freq.name)

                # we choose to compare on the lowest precision
                # since it's the maximum precision on which both
                # dates can be safely compared
                lowest_prec = min(s_precision, t_precision)

                # the result for the current `s_date`
                c_r = 0

                # now we loop through the possible `Period` attributes that we can compare
                # and the precision that stands for said attribute
                for min_required_prec, d_attr in enumerate(['year', 'month', 'day', 'hour', 'minute', 'second']):

                    # If both `s_date` and `t_item` have a precision which allows the
                    # current attribute to be compared then we do so. If the attribute
                    # matches then we add 1 to `c_r`, if not then we break the loop.
                    # We consider from lowest to highest precision. If a lowest
                    # precision attribute (ie, year) doesn't match then we say that
                    # the dates don't match at all (we don't check if higher precision
                    # attributes match)
                    if lowest_prec >= min_required_prec and getattr(s_date, d_attr) == getattr(t_item, d_attr):
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


class SimilarTokens(BaseCompareFeature):
    name = 'SimilarTokens'
    description = 'Compare pairs of lists with string values based on shared tokens'

    def __init__(self, left_on, right_on, agree_value=1.0, disagree_value=0.0, missing_value=0.0, label=None):
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

            source_labels = [pair[0]] if isinstance(pair[0], str) else pair[0]
            target_labels = [pair[1]] if isinstance(pair[1], str) else pair[1]

            first_set = set(source_labels)
            second_set = set()

            for label in target_labels:
                if label:
                    second_set.update(filter(None, label.split()))

            count_intersect = len(first_set.intersection(second_set))
            count_total = len(first_set.union(second_set))

            return count_intersect / count_total if count_total > 0 else np.nan

        return fillna(concatenated.apply(intersection_percentage_size), self.missing_value)


class OccupationCompare(BaseCompareFeature):

    name = "OccupationCompare"
    description = "Compares occupations attribute of record pairs."

    # when expanding the occupations in `_expand_occupations` it
    # is useful to have a dict were we can cache each result, so that
    # we don't have to do a sparql query every time.
    _expand_occupations_cache = {}

    def __init__(self,
                 left_on,
                 right_on,
                 missing_value=0.0,
                 label=None):
        super(OccupationCompare, self).__init__(left_on, right_on, label=label)

        self.missing_value = missing_value

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
                expanded_set = expanded_set | self._expand_occupations_cache[qid]

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
                expanded_set = expanded_set | joined

        return expanded_set

    def _compute_vectorized(self, source_column: pd.Series, target_column: pd.Series):

        # each item in `source_column` is already an array composed
        # of QID strings representing the occupations of a person.
        # On the other hand, each item in `target_column` is a
        # string of space separated QIDs, so we need to first convert
        # those into lists
        # then we also convert each item both the source and target
        # from arrays to sets, so that they're easier to compare
        
        def to_set(itm):
            # if it is an empty array (from source), or an
            # empty string (from target)
            if len(itm) == 0:
                return set()

            # if it is a string with length > 0 split it into
            # its components
            elif isinstance(itm, str):
                itm = itm.split(" ")

            return set(itm)
        
        target_column = target_column.apply(to_set)
        source_column = source_column.apply(to_set)

        # finally, we want to expand the target_column (add the
        # superclasses and subclasses of each occupation)
        target_column = target_column.apply(self._expand_occupations)

        # we then zip together the source column and the target column so that
        # they're easier to process
        concatenated = pd.Series(list(zip(source_column, target_column)))

        def check_occupation_equality(pair: Tuple[Set[str], Set[str]]):
            """Given 2 sets, returns the percentage of items that the
            smallest set shares with the larger set"""
            s_item: Set
            t_item: Set
            s_item, t_item = pair

            # explicitly check if set is empty
            if s_item == set():
                LOGGER.debug(
                    "Can't compare occupations, the wikidata value is null. Pair: %s", pair)
                return np.nan

            if t_item == set():
                LOGGER.debug(
                    "Can't compare occupations, the target value is null. Pair: %s", pair)
                return np.nan

            smaller_length = min(len(s_item), len(t_item))
            n_shared_items = len(s_item.intersection(t_item))

            return n_shared_items / smaller_length

        return fillna(concatenated.apply(check_occupation_equality), self.missing_value)
