#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
from collections import defaultdict
from os import path

import click
import jellyfish
from soweego.commons import target_database, text_utils, url_utils
from soweego.commons.candidate_acquisition import (IDENTIFIER_COLUMN,
                                                   INDEXED_COLUMN, query_index)
from soweego.commons.db_manager import DBManager
from soweego.importer.models.musicbrainz_entity import MusicbrainzArtistEntity
from sqlalchemy_fulltext import FullTextMode, FullTextSearch

LOGGER = logging.getLogger(__name__)
EDIT_DISTANCES = {
    'jw': jellyfish.jaro_winkler,
    'l': jellyfish.levenshtein_distance,
    'dl': jellyfish.damerau_levenshtein_distance
}


@click.command()
@click.argument('source', type=click.File())
@click.argument('target', type=click.File())
@click.option('-s', '--strategy', type=click.Choice(['perfect', 'links', 'names', 'all']), default='all')
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='output',
              help="default: 'output'")
def baseline(source, target, strategy, output_dir):
    """Rule-based matching strategies.

    SOURCE and TARGET must be {string: identifier} JSON files.

    Available strategies are:
    'perfect' = perfect strings;
    'links' = similar links;
    'names' = similar names.

    Run all of them by default.
    """
    source_dataset = json.load(source)
    LOGGER.info("Loaded source dataset '%s'", source.name)
    target_dataset = json.load(target)
    LOGGER.info("Loaded target dataset '%s'", target.name)
    if strategy == 'perfect':
        _perfect_string_wrapper(source_dataset, target_dataset, output_dir)
    elif strategy == 'links':
        _similar_links_wrapper(source_dataset, target_dataset, output_dir)
    elif strategy == 'names':
        _similar_names_wrapper(source_dataset, target_dataset, output_dir)
    elif strategy == 'all':
        LOGGER.info('Will run all the baseline strategies')
        _perfect_string_wrapper(source_dataset, target_dataset, output_dir)
        _similar_links_wrapper(source_dataset, target_dataset, output_dir)
        _similar_names_wrapper(source_dataset, target_dataset, output_dir)


def _similar_names_wrapper(source_dataset, target_dataset, output_dir):
    LOGGER.info('Starting similar name match')
    matches = similar_name_match(source_dataset, target_dataset)
    with open(path.join(output_dir, 'similar_name_matches.json'), 'w') as output_file:
        json.dump(matches, output_file, indent=2, ensure_ascii=False)
        LOGGER.info("Matches dumped to '%s'", output_file.name)


def _similar_links_wrapper(source_dataset, target_dataset, output_dir):
    LOGGER.info('Starting similar link match')
    matches = similar_link_match(source_dataset, target_dataset)
    with open(path.join(output_dir, 'similar_link_matches.json'), 'w') as output_file:
        json.dump(matches, output_file, indent=2, ensure_ascii=False)
        LOGGER.info("Matches dumped to '%s'", output_file.name)
    return matches


def _perfect_string_wrapper(source_dataset, target_dataset, output_dir):
    LOGGER.info('Starting perfect string match')
    matches = perfect_string_match((source_dataset, target_dataset))
    with open(path.join(output_dir, 'perfect_string_matches.json'), 'w') as output_file:
        json.dump(matches, output_file, indent=2, ensure_ascii=False)
        LOGGER.info("Matches dumped to '%s'", output_file.name)


def perfect_string_match(datasets) -> dict:
    """Given an iterable of dictionaries ``{string: identifier}``,
    match perfect strings and return a dataset ``{id: id}``.

    This strategy applies to any object that can be
    treated as a string: names, links, etc.
    """
    matched = {}
    merged = defaultdict(list)
    for dataset in datasets:
        for string, identifier in dataset.items():
            merged[string].append(identifier)
    for string, identifiers in merged.items():
        if len(identifiers) > 1:
            LOGGER.debug("'%s': it's a match! %s -> %s",
                         string, identifiers[0], identifiers[1])
            matched[identifiers[0]] = identifiers[1]
    return matched


def similar_link_match(source, target) -> dict:
    """Given 2 dictionaries ``{link: identifier}``,
    match similar links and return a dataset ``{source_id: target_id}``.

    We treat links as natural language:
    similarity means that a pair of links share a set of keywords.

    This strategy only applies to URLs.
    """
    return perfect_string_match((_process_links(source), _process_links(target)))


def similar_name_match(source, target) -> dict:
    """Given 2 dictionaries ``{person_name: identifier}``,
    """Given a dictionaries ``{person_name: identifier} and a BaseEntity``,
    match similar names and return a dataset ``{source_id: target_id}``.

    This strategy only applies to people names.
    """
    matches = defaultdict(list)
    to_exclude = set()

    db_manager = DBManager()

    for label, qid in source.items():
        if not label:
            continue

        to_exclude.clear()

        tokenized = text_utils.tokenize(label)
        if len(tokenized) <= 1:
            continue

        boolean_search = ' '.join(map('+{0}'.format, tokenized))
        natural_search = ' '.join(tokenized)
        session = db_manager.new_session()

        # NOTICE: sets of size 1 are always exluded
        # Looks for sets equal or bigger containing our tokens
        ft_search = FullTextSearch(
            boolean_search, target, FullTextMode.BOOLEAN)
        for res in session.query(target).filter(ft_search).all():
            matches[qid].append(res.catalog_id)
            to_exclude.add(res.catalog_id)
        # Looks for sets contained in our set of tokens
        ft_search = FullTextSearch(natural_search, target)
        for res in session.query(target).filter(ft_search).filter(~target.catalog_id.in_(to_exclude)).all():
            res_tokenized = text_utils.tokenize(res.tokens)
            if len(res_tokenized) > 1 and res_tokenized.issubset(tokenized):
                matches[qid].append(res.catalog_id)

        if matches[qid]:
            matches[qid] = list(set(matches[qid]))
        else:
            del matches[qid]

    return matches


def edit_distance_match(source, target_table, target_database, target_search_type, metric, threshold) -> dict:
    """Given a source dataset ``{identifier: {string: [languages]}}``,
    match strings having the given edit distance ``metric``
    above the given ``threshold`` and return a dataset
    ``{source_id__target_id: distance_score}``.

    Compute the distance for each ``(source, target)`` entity pair.
    Target candidates are acquired as follows:
    - build a query upon the most frequent source entity strings;
    - exact strings are joined in an OR query, e.g., ``"string1" "string2"``;
    - run the query against a database table containing indexed of target entities.

    ``distance_type`` can be one of:

    - ``jw``, `Jaro-Winkler <https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance>`_;
    - ``l``, `Levenshtein<https://en.wikipedia.org/wiki/Levenshtein_distance>`_;
    - ``dl``, `Damerau-Levenshtein<https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance>`_.

    Return ``None`` if the given edit distance is not valid.
    """
    scores = {}
    distance_function = EDIT_DISTANCES.get(metric)
    if not distance_function:
        LOGGER.error(
            'Invalid distance_type parameter: "%s". ' +
            'Please pick one of "jw" (Jaro-Winkler), "l" (Levenshtein) ' +
            'or "dl" (Damerau-Levenshtein)', metric)
        return None
    LOGGER.info('Using %s edit distance', distance_function.__name__)
    for source_id, source_strings in source.items():
        query, most_frequent_source_strings = _build_index_query(
            source_strings)
        LOGGER.debug('Query: %s', query)
        target_candidates = query_index(
            query, target_search_type, target_table, target_database)
        if target_candidates is None:
            LOGGER.warning('Skipping query that went wrong: %s', query)
            continue
        if target_candidates == {}:
            LOGGER.info('Skipping query with no results: %s', query)
            continue
        # This should be a very small loop, just 1 iteration most of the time
        for source_string in most_frequent_source_strings:
            source_ascii, source_normalized = text_utils.normalize(
                source_string)
            for result in target_candidates:
                target_string = result[INDEXED_COLUMN]
                target_id = result[IDENTIFIER_COLUMN]
                target_ascii, target_normalized = text_utils.normalize(
                    target_string)
                try:
                    distance = distance_function(
                        source_normalized, target_normalized)
                # Damerau-Levenshtein does not support some Unicode code points
                except ValueError:
                    LOGGER.warning(
                        'Skipping unsupported string in pair: "%s", "%s"',
                        source_normalized, target_normalized)
                    continue
                LOGGER.debug('Source: %s > %s > %s - Target: %s > %s > %s - Distance: %f',
                             source_string, source_ascii, source_normalized,
                             target_string, target_ascii, target_normalized,
                             distance)
                if (metric in ('l', 'dl') and distance <= threshold) or (metric == 'jw' and distance >= threshold):
                    scores['%s__%s' % (source_id, target_id)] = distance
                    LOGGER.debug("It's a match! %s -> %s",
                                 source_id, target_id)
                else:
                    LOGGER.debug('Skipping potential match due to the threshold: %s -> %s - Threshold: %f - Distance: %f',
                                 source_id, target_id, threshold, distance)
    return scores


def _build_index_query(source_strings):
    query_builder = []
    frequencies = defaultdict(list)
    for label, languages in source_strings.items():
        frequencies[len(languages)].append(label)
    most_frequent = frequencies[max(frequencies.keys())]
    for label in most_frequent:
        # TODO experiment with different strategies
        query_builder.append('"%s"' % label)
    return ' '.join(query_builder), most_frequent


def _process_names(dataset) -> dict:
    """Convert a dataset `{person_name: identifier}`
    into a `{person_tokens: identifier}` one.

    Name tokens are grouped by identifier and joined to treat them as a string.
    """
    tokenized = defaultdict(set)
    processed = {}
    for name, identifier in dataset.items():
        LOGGER.debug('Identifier [%s]: processing name "%s"', identifier, name)
        tokens = text_utils.tokenize(name, text_utils.NAME_STOPWORDS)
        tokenized[identifier].update(tokens)
    for identifier, tokens in tokenized.items():
        LOGGER.debug('Identifier [%s]: tokens = %s', identifier, tokens)
        processed['|'.join(tokens)] = identifier
    return processed


def _process_links(dataset) -> dict:
    """Convert a dataset `{link: identifier}`
    into a `{link_tokens: identifier}` one.

    Link tokens are joined to treat them as a string.
    """
    processed = {}
    for link, identifier in dataset.items():
        tokens = url_utils.tokenize(link)
        if not tokens:
            LOGGER.info('Skipping invalid URL')
            continue
        processed['|'.join(tokens)] = identifier
    return processed
