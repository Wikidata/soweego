#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of techniques to compare record pairs (read extract features) for probabilistic linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
from typing import Union, List, Tuple

import jellyfish
import numpy as np
import pandas as pd
from recordlinkage.base import BaseCompareFeature
from recordlinkage.utils import fillna
from sklearn.feature_extraction.text import CountVectorizer

from soweego.commons import text_utils

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
                'Bad string similarity algorithm: %s. Please use one of %s' % (self.algorithm, ('levenshtein', 'cosine')))

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
                return 0

            # convert `s_item` to a list if it isn't already
            if not isinstance(s_item, (list, tuple)):
                s_item = [s_item]

            # precisions listed from least to most precise, as defined here:
            # http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
            precisions = [
                'A-DEC',  # we know only the year
                'M',  # we know up to the month
                'D',  # up to the day
                'H',  # up to the hour
                'T',  # up to the minute
                'S',  # up to the second
                'U',  # up to the microsecond
                'N',  # up to the nanosecond
            ]

            # will help us to keep track of the best score
            best = 0

            for s_date in s_item:

                # if the current s_date is NaT then we can't compare, so we skip it
                if pd.isna(s_date):
                    continue

                # get precision number for both dates
                s_precision = precisions.index(s_date.freq.name)
                t_precision = precisions.index(t_item.freq.name)

                # we choose to compare on the lowest precision
                # since it's the maximum precision on which both
                # dates can be safely compared
                lowest_prec = min(s_precision, t_precision)

                # the result for the current `s_date`
                c_r = 0

                for min_required_prec, d_attr in enumerate(['year', 'month', 'day', 'hour', 'minute', 'second']):
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
                best = max(best, (c_r / (lowest_prec+1)))

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
