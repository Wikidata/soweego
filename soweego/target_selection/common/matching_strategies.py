#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
from collections import defaultdict
from urllib.parse import urlsplit

LOGGER = logging.getLogger(__name__)
TOP_LEVEL_DOMAINS = set(['com', 'org', 'net', 'info', 'fm'])
DOMAIN_PREFIXES = set(['www'])


def perfect_string_match(datasets) -> dict:
    """Given an iterable of dictionaries `{string: identifier}`,
    match perfect strings and return a dictionary `{id: id}`.

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
    """Given 2 dictionaries `{link: identifier}`,
    match similar links and return a dictionary `{id: id}`.

    This strategy only applies to URLs.
    """
    return perfect_string_match((_process_links(source), _process_links(target)))


def _process_links(dataset) -> dict:
    """Convert a dictionary `{link: identifier}`
    into a `{link_tokens: identifier}` one.

    Link tokens are joined to treat them as a string.
    """
    processed = {}
    for link, identifier in dataset.items():
        tokens = _tokenize_url(link)
        if not tokens:
            LOGGER.info('Skipping invalid URL')
            continue
        processed['|'.join(tokens)] = identifier
    return processed


def _tokenize_url(url) -> set:
    """Tokenize a URL, removing stopwords.
    Return `None` if the URL is invalid.
    """
    split = None
    try:
        split = urlsplit(url)
    except ValueError as value_error:
        LOGGER.warning('Invalid URL: %s. Reason: %s',
                       url, value_error, exc_info=1)
        return None
    domain_tokens = set(split.netloc.split('.'))
    path_tokens = set(filter(None, split.path.split('/')))
    domain_tokens.difference_update(TOP_LEVEL_DOMAINS, DOMAIN_PREFIXES)
    tokens = domain_tokens.union(path_tokens)
    LOGGER.debug('URL: %s - Tokens: %s', url, tokens)
    return tokens
