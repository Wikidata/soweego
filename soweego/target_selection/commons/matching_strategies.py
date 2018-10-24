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
import re
from collections import defaultdict
from os import path
from urllib.parse import urlsplit

import click
import jellyfish

from soweego.commons.candidate_acquisition import (IDENTIFIER_COLUMN,
                                                   INDEXED_COLUMN, query_index)

LOGGER = logging.getLogger(__name__)
# URLs stopwords
TOP_LEVEL_DOMAINS = set(['com', 'org', 'net', 'info', 'fm'])
DOMAIN_PREFIXES = set(['www', 'm', 'mobile'])
# Names processing
NAMES_STOPWORDS = set(
    ['sir', 'lord', 'mr', 'mrs', 'ms', 'miss', 'madam', 'jr', 'sr', 'phd',
     'dr', 'mme', 'mlle', 'baron', 'baronet', 'bt', 'graf', 'gräfin', 'de',
     'of', 'von', 'the']
)
EDIT_DISTANCES = {
    'jw': jellyfish.jaro_winkler,
    'l': jellyfish.levenshtein_distance,
    'dl': jellyfish.damerau_levenshtein_distance
}
# Latin alphabet diacritics and Russian
ASCII_TRANSLATION_TABLE = str.maketrans({
    'á': 'a', 'Á': 'A', 'à': 'a', 'À': 'A', 'ă': 'a', 'Ă': 'A', 'â': 'a',
    'Â': 'A', 'å': 'a', 'Å': 'A', 'ã': 'a', 'Ã': 'A', 'ą': 'a', 'Ą': 'A',
    'ā': 'a', 'Ā': 'A', 'ä': 'ae', 'Ä': 'AE', 'æ': 'ae', 'Æ': 'AE',
    'ḃ': 'b', 'Ḃ': 'B', 'ć': 'c', 'Ć': 'C', 'ĉ': 'c', 'Ĉ': 'C', 'č': 'c',
    'Č': 'C', 'ċ': 'c', 'Ċ': 'C', 'ç': 'c', 'Ç': 'C', 'ď': 'd', 'Ď': 'D',
    'ḋ': 'd', 'Ḋ': 'D', 'đ': 'd', 'Đ': 'D', 'ð': 'dh', 'Ð': 'Dh',
    'é': 'e', 'É': 'E', 'è': 'e', 'È': 'E', 'ĕ': 'e', 'Ĕ': 'E', 'ê': 'e',
    'Ê': 'E', 'ě': 'e', 'Ě': 'E', 'ë': 'e', 'Ë': 'E', 'ė': 'e', 'Ė': 'E',
    'ę': 'e', 'Ę': 'E', 'ē': 'e', 'Ē': 'E', 'ḟ': 'f', 'Ḟ': 'F', 'ƒ': 'f',
    'Ƒ': 'F', 'ğ': 'g', 'Ğ': 'G', 'ĝ': 'g', 'Ĝ': 'G', 'ġ': 'g', 'Ġ': 'G',
    'ģ': 'g', 'Ģ': 'G', 'ĥ': 'h', 'Ĥ': 'H', 'ħ': 'h', 'Ħ': 'H', 'í': 'i',
    'Í': 'I', 'ì': 'i', 'Ì': 'I', 'î': 'i', 'Î': 'I', 'ï': 'i', 'Ï': 'I',
    'ĩ': 'i', 'Ĩ': 'I', 'į': 'i', 'Į': 'I', 'ī': 'i', 'Ī': 'I', 'ĵ': 'j',
    'Ĵ': 'J', 'ķ': 'k', 'Ķ': 'K', 'ĺ': 'l', 'Ĺ': 'L', 'ľ': 'l', 'Ľ': 'L',
    'ļ': 'l', 'Ļ': 'L', 'ł': 'l', 'Ł': 'L', 'ṁ': 'm', 'Ṁ': 'M', 'ń': 'n',
    'Ń': 'N', 'ň': 'n', 'Ň': 'N', 'ñ': 'n', 'Ñ': 'N', 'ņ': 'n', 'Ņ': 'N',
    'ó': 'o', 'Ó': 'O', 'ò': 'o', 'Ò': 'O', 'ô': 'o', 'Ô': 'O', 'ő': 'o',
    'Ő': 'O', 'õ': 'o', 'Õ': 'O', 'ø': 'oe', 'Ø': 'OE', 'ō': 'o', 'Ō': 'O',
    'ơ': 'o', 'Ơ': 'O', 'ö': 'oe', 'Ö': 'OE', 'ṗ': 'p', 'Ṗ': 'P', 'ŕ': 'r',
    'Ŕ': 'R', 'ř': 'r', 'Ř': 'R', 'ŗ': 'r', 'Ŗ': 'R', 'ś': 's', 'Ś': 'S',
    'ŝ': 's', 'Ŝ': 'S', 'š': 's', 'Š': 'S', 'ṡ': 's', 'Ṡ': 'S', 'ş': 's',
    'Ş': 'S', 'ș': 's', 'Ș': 'S', 'ß': 'SS', 'ť': 't', 'Ť': 'T', 'ṫ': 't',
    'Ṫ': 'T', 'ţ': 't', 'Ţ': 'T', 'ț': 't', 'Ț': 'T', 'ŧ': 't', 'Ŧ': 'T',
    'ú': 'u', 'Ú': 'U', 'ù': 'u', 'Ù': 'U', 'ŭ': 'u', 'Ŭ': 'U', 'û': 'u',
    'Û': 'U', 'ů': 'u', 'Ů': 'U', 'ű': 'u', 'Ű': 'U', 'ũ': 'u', 'Ũ': 'U',
    'ų': 'u', 'Ų': 'U', 'ū': 'u', 'Ū': 'U', 'ư': 'u', 'Ư': 'U', 'ü': 'ue',
    'Ü': 'UE', 'ẃ': 'w', 'Ẃ': 'W', 'ẁ': 'w', 'Ẁ': 'W', 'ŵ': 'w', 'Ŵ': 'W',
    'ẅ': 'w', 'Ẅ': 'W', 'ý': 'y', 'Ý': 'Y', 'ỳ': 'y', 'Ỳ': 'Y', 'ŷ': 'y',
    'Ŷ': 'Y', 'ÿ': 'y', 'Ÿ': 'Y', 'ź': 'z', 'Ź': 'Z', 'ž': 'z', 'Ž': 'Z',
    'ż': 'z', 'Ż': 'Z', 'þ': 'th', 'Þ': 'Th', 'µ': 'u', 'а': 'a', 'А': 'a',
    'б': 'b', 'Б': 'b', 'в': 'v', 'В': 'v', 'г': 'g', 'Г': 'g', 'д': 'd',
    'Д': 'd', 'е': 'e', 'Е': 'E', 'ё': 'e', 'Ё': 'E', 'ж': 'zh', 'Ж': 'zh',
    'з': 'z', 'З': 'z', 'и': 'i', 'И': 'i', 'й': 'j', 'Й': 'j', 'к': 'k',
    'К': 'k', 'л': 'l', 'Л': 'l', 'м': 'm', 'М': 'm', 'н': 'n', 'Н': 'n',
    'о': 'o', 'О': 'o', 'п': 'p', 'П': 'p', 'р': 'r', 'Р': 'r', 'с': 's',
    'С': 's', 'т': 't', 'Т': 't', 'у': 'u', 'У': 'u', 'ф': 'f', 'Ф': 'f',
    'х': 'h', 'Х': 'h', 'ц': 'c', 'Ц': 'c', 'ч': 'ch', 'Ч': 'ch', 'ш': 'sh',
    'Ш': 'sh', 'щ': 'sch', 'Щ': 'sch', 'ъ': '', 'Ъ': '', 'ы': 'y', 'Ы': 'y',
    'ь': '', 'Ь': '', 'э': 'e', 'Э': 'e', 'ю': 'ju', 'Ю': 'ju', 'я': 'ja',
    'Я': 'ja'
})


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
    match similar names and return a dataset ``{source_id: target_id}``.

    This strategy only applies to people names.
    """
    return perfect_string_match((_process_names(source), _process_names(target)))


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
            source_normalized, source_ascii = _normalize(source_string)
            for result in target_candidates:
                target_string = result[INDEXED_COLUMN]
                target_id = result[IDENTIFIER_COLUMN]
                target_normalized, target_ascii = _normalize(target_string)
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
        ascii_lowercased, ascii_name = _normalize(name)
        split = re.split(r'\W+', ascii_lowercased)
        LOGGER.debug('%s > %s > %s > %s',
                     name, ascii_name, ascii_lowercased, split)
        filtered = filter(lambda token: len(token) > 1, split)
        for token in filtered:
            if token and token not in NAMES_STOPWORDS:
                tokenized[identifier].add(token)
    for identifier, tokens in tokenized.items():
        LOGGER.debug('Identifier [%s]: tokens = %s', identifier, tokens)
        processed['|'.join(tokens)] = identifier
    return processed


def _normalize(name):
    """Convert to ASCII and lowercase a name."""
    ascii_name = name.translate(ASCII_TRANSLATION_TABLE)
    lowercased = ascii_name.lower()
    return lowercased, ascii_name


def _process_links(dataset) -> dict:
    """Convert a dataset `{link: identifier}`
    into a `{link_tokens: identifier}` one.

    Link tokens are joined to treat them as a string.
    """
    processed = {}
    for link, identifier in dataset.items():
        tokens = tokenize_url(link)
        if not tokens:
            LOGGER.info('Skipping invalid URL')
            continue
        processed['|'.join(tokens)] = identifier
    return processed


def tokenize_url(url, domain_only=False) -> set:
    """Tokenize a URL, removing stopwords.
        Return `None` if the URL is invalid.
    """
    try:
        split = urlsplit(url)
    except ValueError as value_error:
        LOGGER.warning('Invalid URL: %s. Reason: %s',
                       url, value_error, exc_info=1)
        return None
    domain_tokens = set(split.netloc.split('.'))
    domain_tokens.difference_update(TOP_LEVEL_DOMAINS, DOMAIN_PREFIXES)
    if domain_only:
        LOGGER.debug('URL: %s - Domain-only tokens: %s', url, domain_tokens)
        return domain_tokens
    path_tokens = set(filter(None, split.path.split('/')))
    tokens = domain_tokens.union(path_tokens)
    LOGGER.debug('URL: %s - Tokens: %s', url, tokens)
    return tokens
