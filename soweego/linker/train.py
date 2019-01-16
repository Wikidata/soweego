#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Training set construction for supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os

import click
import recordlinkage as rl
from pandas import DataFrame, read_json

from soweego.commons import (constants, data_gathering, target_database,
                             text_utils, url_utils)
from soweego.linker.feature_extraction import StringList, UrlList
from soweego.validator.checks import get_vocabulary

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-c', '--cache', type=click.File(), default=None, help="Load dumped Wikidata training set. Default: no.")
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared')
def cli(target, target_type, cache, output_dir):
    """Build the training set."""
    wikidata_df, target_df = build(target, target_type, cache)
    wikidata_df.to_json(os.path.join(
        output_dir, 'wikidata_%s_training_set.json' % target))
    target_df.to_json(os.path.join(output_dir, '%s_training_set' % target))


def train(catalog, entity, classifier, wikidata_cache=None):
    wikidata, target = build(catalog, entity, wikidata_cache)
    preprocess(wikidata, target)
    candidate_pairs = block(wikidata, target)
    feature_vectors = extract_features(candidate_pairs, wikidata, target)
    classifier.fit(feature_vectors, candidate_pairs)


def build(catalog, entity, wikidata_cache):
    catalog_terms = get_vocabulary(catalog)

    # Wikidata
    if wikidata_cache is None:
        wikidata = {}

        data_gathering.gather_target_ids(
            entity, catalog, catalog_terms['pid'], wikidata)
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        data_gathering.gather_wikidata_dataset(
            wikidata, url_pids, ext_id_pids_to_urls)
        wikidata_df = DataFrame.from_dict(wikidata, orient='index')
    else:
        wikidata_df = read_json(wikidata_cache)

    # Target
    target_ids = get_target_ids(wikidata_cache, wikidata, wikidata_df)
    target = data_gathering.gather_target_dataset(
        entity, catalog, target_ids, False)
    target_df = DataFrame.from_dict(target, orient='index')

    return wikidata_df, target_df


def get_target_ids(wikidata_cache, wikidata, wikidata_df):
    identifiers = set()
    if wikidata_cache is None:
        for data in wikidata.values():
            for identifier in data['identifiers']:
                identifiers.add(identifier)
    else:
        ids_series = wikidata_df.identifiers.to_dict()
        for array in ids_series.values():
            for identifier in array:
                identifiers.add(identifier)
    return identifiers


def preprocess(wikidata_df, target_df):
    # Wikidata
    # Tokenize & join strings lists columns
    for column in (constants.DF_LABEL, constants.DF_ALIAS, constants.DF_PSEUDONYM):
        wikidata_df['%s_tokens' % column] = wikidata_df[column].map(
            _preprocess_strings_list, na_action='ignore')
    # Tokenize & join URLs lists
    wikidata_df['%s_tokens' % constants.DF_URL] = wikidata_df[constants.DF_URL].map(
        _preprocess_urls_list, na_action='ignore')
    # Join the list of descriptions
    # TODO It certainly doesn't make sense to compare descriptions in different languages
    wikidata_df[constants.DF_DESCRIPTION] = wikidata_df[constants.DF_DESCRIPTION].map(
        lambda row: ' '.join(row), na_action='ignore')

    # Target
    target_df[constants.DF_DESCRIPTION] = target_df[constants.DF_DESCRIPTION].map(
        lambda row: ' '.join(row), na_action='ignore')


def _preprocess_strings_list(strings_list):
    joined = []
    for value in strings_list:
        tokens = text_utils.tokenize(value)
        if tokens:
            joined.append(' '.join(tokens))
    return joined


def _preprocess_urls_list(urls_list):
    joined = []
    for value in urls_list:
        tokens = url_utils.tokenize(value)
        if tokens:
            joined.append(' '.join(tokens))
    return joined


def block(wikidata_df, target_df):
    """Block on target identifiers"""
    # Join the list of identifiers
    # TODO in this way, we can't block on QIDs that have multiple target IDs
    wikidata_df['identifiers'] = wikidata_df['identifiers'].map(
        lambda row: ' '.join(row))
    # Make a target ID column from the row labels
    target_df['identifier'] = target_df.index.to_series().astype(str)
    idx = rl.Index()
    idx.block('identifiers', 'identifier')
    return idx.index(wikidata_df, target_df)


def extract_features(candidate_pairs, wikidata_df, target_df):
    compare = rl.Compare()
    # TODO similar name match as a feature
    # TODO feature engineering on more fields
    # wikidata columns = Index(['identifiers', 'label', 'alias', 'description', 'url', 'given_name',
    #    'date_of_birth', 'date_of_death', 'place_of_death', 'birth_name',
    #    'place_of_birth', 'sex_or_gender', 'family_name', 'pseudonym'],
    # discogs columns = Index(['description_tokens', 'name_tokens', 'description', 'url', 'url_tokens',
    #    'name', 'born', 'born_precision', 'real_name', 'is_wiki',
    #    'data_quality', 'died', 'died_precision', 'identifier'],
    # Feature 1: exact match on URLs
    compare.add(UrlList('url', 'url', label='url_exact'))
    # Feature 2: dates
    # TODO parse dates
    # compare.date('date_of_birth', 'born', label='birth_date')
    # compare.date('date_of_death', 'died', label='death_date')
    # Feature 3: Levenshtein distance on names
    compare.add(StringList('label_tokens',
                           'name_tokens', label='name_levenshtein'))
    # Feture 4: cosine similarity on descriptions
    compare.add(StringList('description', 'description',
                           algorithm='cosine', analyzer='soweego', label='description_cosine'))
    return compare.compute(candidate_pairs, wikidata_df, target_df)


if __name__ == "__main__":
    wd = read_json(
        '/tmp/soweego_shared/wikidata_discogs_training_set.json')
    t = read_json('/tmp/soweego_shared/discogs_training_set')
    preprocess(wd, t)
    cp = block(wd, t)
    fv = extract_features(cp, wd, t)
    nb = rl.NaiveBayesClassifier(binarize=0.1)
    nb.fit(fv, cp)
    print()
