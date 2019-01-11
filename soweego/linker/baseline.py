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
from typing import Callable, Iterable, Tuple

import click
import jellyfish
from soweego.commons import (data_gathering, target_database, text_utils,
                             url_utils)
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity

LOGGER = logging.getLogger(__name__)
EDIT_DISTANCES = {
    'jw': jellyfish.jaro_winkler,
    'l': jellyfish.levenshtein_distance,
    'dl': jellyfish.damerau_levenshtein_distance
}


@click.command()
@click.argument('source', type=click.File())
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-s', '--strategy', type=click.Choice(['perfect', 'links', 'names']), default='perfect')
@click.option('--upload/--no-upload', default=False, help='Upload check results to Wikidata. Default: no.')
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared',
              help="default: 'output'")
def baseline(source, target, target_type, strategy, upload, output_dir):
    """Rule-based matching strategies.

    SOURCE must be {string: identifier} JSON files.

    NOTICE: not all the entity types are available for all the targets

    Available strategies are:
    'perfect' = perfect strings;
    'links' = similar links;
    'names' = similar names.

    Run all of them by default.
    """

    # TODO source should be a stream from wikidata
    source_dataset = json.load(source)
    LOGGER.info("Loaded source dataset '%s'", source.name)

    target_entity = target_database.get_entity(target, target_type)
    target_link_entity = target_database.get_link_entity(target, target_type)
    target_pid = target_database.get_pid(target)

    result = None

    if strategy == 'perfect':
        result = perfect_name_match(source_dataset, target_entity, target_pid)
    elif strategy == 'links':
        result = similar_tokens_match(
            source_dataset, target_link_entity, target_pid, url_utils.tokenize)
    elif strategy == 'names':
        result = similar_tokens_match(
            source_dataset, target_entity, target_pid, text_utils.tokenize)

    if upload:
        print("SEND ME TO INGESTOR PLS")
    else:
        filepath = path.join(output_dir, 'baseline_output.csv')
        with open(filepath, 'w') as filehandle:
            for res in result:
                filehandle.write('%s\n' % ";".join(res))
                filehandle.flush()
        LOGGER.info("Dump baseline %s against %s in %s",
                    strategy, target, filepath)


def perfect_name_match(source_dataset, target_entity: BaseEntity, target_pid: str) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a Base Entity and a PID,
    match perfect strings and return a dataset ``[(source_id, PID, target_id), ...]``.

    This strategy applies to any object that can be
    treated as a string: names, links, etc.
    """
    for label, qid in source_dataset.items():
        for res in data_gathering.perfect_name_search(target_entity, label):
            yield (qid, target_pid, res.catalog_id)


def similar_tokens_match(source, target, target_pid: str, tokenize: Callable[[str], Iterable[str]]) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a BaseEntity and a tokenization function and a PID,
    match similar tokens and return a dataset ``[(source_id, PID, target_id), ...]``.

    Similar tokens match means that if a set of tokens is contained in another one, it's a match.
    """
    to_exclude = set()

    for label, qid in source.items():
        if not label:
            continue

        to_exclude.clear()

        tokenized = tokenize(label)
        if len(tokenized) <= 1:
            continue

        # NOTICE: sets of size 1 are always exluded
        # Looks for sets equal or bigger containing our tokens
        for res in data_gathering.tokens_fulltext_search(target, True, tokenized):
            yield (qid, target_pid, res.catalog_id)
            to_exclude.add(res.catalog_id)
        # Looks for sets contained in our set of tokens
        for res in data_gathering.tokens_fulltext_search(target, False, tokenized):
            res_tokenized = set(res.tokens.split(' '))
            if len(res_tokenized) > 1 and res_tokenized.issubset(tokenized):
                yield (qid, target_pid, res.catalog_id)


def edit_distance_match(source, target: BaseEntity, target_pid: str, metric: str, threshold: float) -> Iterable[Tuple[str, str, str]]:
    """Given a source dataset ``{identifier: {string: [languages]}}``,
    match strings having the given edit distance ``metric``
    above the given ``threshold`` and return a dataset
    ``[(source_id, PID, target_id, distance_score), ...]``.

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
        target_candidates = data_gathering.name_fulltext_search(
            target, query)
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
                target_string = result.name
                target_id = result.catalog_id
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
                    yield (source_id, target_pid, target_id, distance)
                    LOGGER.debug("It's a match! %s -> %s",
                                 source_id, target_id)
                else:
                    LOGGER.debug('Skipping potential match due to the threshold: %s -> %s - Threshold: %f - Distance: %f',
                                 source_id, target_id, threshold, distance)


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
