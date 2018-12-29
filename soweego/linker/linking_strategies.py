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
from soweego.commons.db_manager import DBManager
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
@click.option('-s', '--strategy', type=click.Choice(['perfect', 'links', 'names', 'edit_distance', 'all']), default='all')
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared',
              help="default: 'output'")
def baseline(source, target, target_type, strategy, output_dir):
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
    if strategy == 'perfect':
        _perfect_name_wrapper(source_dataset, target_entity, output_dir)
    elif strategy == 'links':
        _similar_links_wrapper(source_dataset, target_link_entity, output_dir)
    elif strategy == 'names':
        _similar_names_wrapper(source_dataset, target_entity, output_dir)
    elif strategy == 'edit_distance':
       # TODO create a command only for this matching technique to expose the edit distance function too
        edit_distance_match(source_dataset, target_entity, 'jw', 0)
    elif strategy == 'all':
        LOGGER.info('Will run all the baseline strategies')
        _perfect_name_wrapper(source_dataset, target_entity, output_dir)
        _similar_names_wrapper(source_dataset, target_entity, output_dir)
        _similar_links_wrapper(source_dataset, target_link_entity, output_dir)


def _similar_names_wrapper(source_dataset, target_dataset, output_dir):
    LOGGER.info('Starting similar name match')
    matches = similar_name_match(
        source_dataset, target_dataset, text_utils.tokenize)
    with open(path.join(output_dir, 'similar_name_matches.json'), 'w') as output_file:
        json.dump(matches, output_file, indent=2, ensure_ascii=False)
        LOGGER.info("Matches dumped to '%s'", output_file.name)


def _similar_links_wrapper(source_dataset, target_entity, output_dir):
    LOGGER.info('Starting similar link match')
    matches = similar_link_match(source_dataset, target_entity)
    with open(path.join(output_dir, 'similar_link_matches.json'), 'w') as output_file:
        json.dump(matches, output_file, indent=2, ensure_ascii=False)
        LOGGER.info("Matches dumped to '%s'", output_file.name)
    return matches


def _perfect_name_wrapper(source_dataset, target_entity, output_dir):
    LOGGER.info('Starting perfect string match')
    matches = perfect_name_match(source_dataset, target_entity)
    with open(path.join(output_dir, 'perfect_string_matches.json'), 'w') as output_file:
        json.dump(matches, output_file, indent=2, ensure_ascii=False)
        LOGGER.info("Matches dumped to '%s'", output_file.name)


def perfect_name_match(source_dataset, target_entity: BaseEntity) -> dict:
    """Given an iterable of dictionaries ``{string: identifier}``,
    match perfect strings and return a dataset ``{id: id}``.

    This strategy applies to any object that can be
    treated as a string: names, links, etc.
    """
    db_manager = DBManager()
    session = db_manager.new_session()
    matched = {}

    for label, qid in source_dataset.items():
        for res in session.query(target_entity).filter(target_entity.name == label).all():
            if matched.get(qid):
                LOGGER.warning(
                    '%s - %s has already a perfect name match' % (qid, label))
            matched[qid] = res.catalog_id

    return matched


def similar_link_match(source, target: BaseLinkEntity) -> dict:
    """Given a dictionaries ``{link: identifier} and a BaseLinkEntity``,
    match similar links and return a dataset ``{source_id: target_id}``.

    We treat links as natural language:
    similarity means that a pair of links share a set of keywords.

    This strategy only applies to URLs.
    """
    return similar_name_match(source, target, url_utils.tokenize)


def similar_name_match(source, target, tokenize) -> dict:
    """Given a dictionaries ``{person_name: identifier}, a BaseEntity and a tokenization function``,
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

        tokenized = tokenize(label)
        if len(tokenized) <= 1:
            continue

        boolean_search = ' '.join(map('+{0}'.format, tokenized))
        natural_search = ' '.join(tokenized)
        session = db_manager.new_session()

        # NOTICE: sets of size 1 are always exluded
        # Looks for sets equal or bigger containing our tokens
        ft_search = target.tokens.match(boolean_search)
        for res in session.query(target).filter(ft_search).all():
            matches[qid].append(res.catalog_id)
            to_exclude.add(res.catalog_id)
        # Looks for sets contained in our set of tokens
        ft_search = target.tokens.match(natural_search)
        for res in session.query(target).filter(ft_search).filter(~target.catalog_id.in_(to_exclude)).all():
            res_tokenized = text_utils.tokenize(res.tokens)
            if len(res_tokenized) > 1 and res_tokenized.issubset(tokenized):
                matches[qid].append(res.catalog_id)

        if matches[qid]:
            matches[qid] = list(set(matches[qid]))
        else:
            del matches[qid]

    return matches


def edit_distance_match(source, target: BaseEntity, metric, threshold) -> dict:
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
    db_manager = DBManager()
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
        session = db_manager.new_session()
        target_candidates = session.query(target).filter(
            target.name.match(query)).all()
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
