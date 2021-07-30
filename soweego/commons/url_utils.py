#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of utilities for URL cleaning, validation, resolution"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import re
from functools import lru_cache
from urllib.parse import unquote, urlsplit

import regex
import requests.exceptions
from requests import get
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

LOGGER = logging.getLogger(__name__)

# HTTP requests timeout in seconds
READ_TIMEOUT = 10

# URLs stopwords
TOP_LEVEL_DOMAINS = set(['com', 'org', 'net', 'info', 'fm'])
DOMAIN_PREFIXES = set(['www', 'm', 'mobile'])

# Used to check whether a URL is a wiki link
# From https://wikimediafoundation.org/our-work/wikimedia-projects/
WIKI_PROJECTS = [
    'wikipedia',
    'wikibooks',
    'wiktionary',
    'wikiquote',
    'commons.wikimedia',
    'wikisource',
    'wikiversity',
    'wikidata',
    'mediawiki',
    'wikivoyage',
    'meta.wikimedia',
]


def clean(url):
    stripped = url.strip()
    if ' ' in stripped:
        items = stripped.split()
        LOGGER.debug('URL <%s> split into %s', url, items)
        return items
    return [stripped]


# Adapted from https://github.com/django/django/blob/master/django/core/validators.py
# See DJANGO_LICENSE
def validate(url):
    ul = '\u00a1-\uffff'  # Unicode letters range (must not be a raw string)
    # IP patterns
    ipv4_re = r'(?:25[0-5]|2[0-4]\d|[0-1]?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}'
    ipv6_re = r'\[[0-9a-f:\.]+\]'
    # Host patterns
    hostname_re = (
        r'[a-z' + ul + r'0-9](?:[a-z' + ul + r'0-9-]{0,61}[a-z' + ul + r'0-9])?'
    )
    # Max length for domain name labels is 63 characters per RFC 1034 sec. 3.1
    domain_re = r'(?:\.(?!-)[a-z' + ul + r'0-9-]{1,63}(?<!-))*'
    # Top-level domain pattern
    tld_re = (
        r'\.'  # Dot
        r'(?!-)'  # Can't start with a dash
        r'(?:[a-z' + ul + '-]{2,63}'  # Domain label
        r'|xn--[a-z0-9]{1,59})'  # Or punycode label
        r'(?<!-)'  # Can't end with a dash
        r'\.?'  # May have a trailing dot
    )
    host_re = '(' + hostname_re + domain_re + tld_re + '|localhost)'
    final_regex = re.compile(
        r'^((?:[a-z0-9\.\-\+]*)://)?'  # Scheme is optional
        r'(?:[^\s:@/]+(?::[^\s:@/]*)?@)?'  # user:pass authentication
        r'(?:' + ipv4_re + '|' + ipv6_re + '|' + host_re + ')'
        r'(?::\d{2,5})?'  # Port
        r'(?:[/?#][^\s]*)?'  # Resource path
        r'\Z',
        re.IGNORECASE,
    )
    valid_url = re.search(final_regex, url)
    if not valid_url:
        LOGGER.debug('Dropping invalid URL: <%s>', url)
        return None
    if not valid_url.group(1):
        LOGGER.debug(
            "Adding 'https' to potential URL with missing scheme: <%s>", url
        )
        return 'https://' + valid_url.group()
    return valid_url.group()


@lru_cache()
def resolve(url):
    # Don't show warnings in case of unverified HTTPS requests
    disable_warnings(InsecureRequestWarning)
    # Some Web sites return 4xx just because of a non-browser user agent header
    browser_ua = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:62.0) Gecko/20100101 Firefox/62.0'
    }
    try:
        # Some Web sites do not accept the HEAD method: fire a GET, but don't download anything
        response = get(
            url, headers=browser_ua, stream=True, timeout=READ_TIMEOUT
        )
    except requests.exceptions.SSLError as ssl_error:
        LOGGER.debug(
            'SSL certificate verification failed, will retry without verification. Original URL: <%s> - Reason: %s',
            url,
            ssl_error,
        )
        try:
            response = get(url, headers=browser_ua, stream=True, verify=False)
        except Exception as unexpected_error:
            LOGGER.warning(
                'Dropping URL that led to an unexpected error: <%s> - Reason: %s',
                url,
                unexpected_error,
            )
            return None
    except requests.exceptions.Timeout as timeout:
        LOGGER.info(
            'Dropping URL that led to a request timeout: <%s> - Reason: %s',
            url,
            timeout,
        )
        return None
    except requests.exceptions.TooManyRedirects as too_many_redirects:
        LOGGER.info(
            'Dropping URL because of too many redirects: <%s> - %s',
            url,
            too_many_redirects,
        )
        return None
    except requests.exceptions.ConnectionError as connection_error:
        LOGGER.info(
            'Dropping URL that led to an aborted connection: <%s> - Reason: %s',
            url,
            connection_error,
        )
        return None
    except Exception as unexpected_error:
        LOGGER.warning(
            'Dropping URL that led to an unexpected error: <%s> - Reason: %s',
            url,
            unexpected_error,
        )
        return None
    if not response.ok:
        LOGGER.info(
            "Dropping dead URL that returned HTTP status '%s' (%d): <%s>",
            response.reason,
            response.status_code,
            url,
        )
        return None
    resolved = response.url
    history = response.history
    if len(history) > 1:
        LOGGER.debug(
            'Resolution chain from original URL to resolved URL: %s',
            [r.url for r in history],
        )
    else:
        LOGGER.debug('Original URL: <%s> - Resolved URL: <%s>', url, resolved)
    return resolved


def tokenize(url, domain_only=False) -> set:
    """Tokenize a URL, removing stopwords.
        Return `None` if the URL is invalid.
    """
    try:
        split = urlsplit(url)
    except ValueError as value_error:
        LOGGER.warning(
            'Invalid URL: %s. Reason: %s', url, value_error, exc_info=1
        )
        return None
    domain_tokens = set(re.split(r'\W+', split.netloc))
    domain_tokens.difference_update(TOP_LEVEL_DOMAINS, DOMAIN_PREFIXES)
    if domain_only:
        LOGGER.debug('URL: %s - Domain-only tokens: %s', url, domain_tokens)
        return domain_tokens
    path_tokens = set(filter(None, split.path.split('/')))

    tokens = domain_tokens
    for path_token in path_tokens:
        decoded = unquote(path_token)
        path_split = re.split(r'\W+', decoded)
        filtered = filter(lambda token: len(token) > 1, path_split)
        tokens = tokens.union(filtered)

    for query_token in re.split(r'\W+', unquote(split.query)):
        if query_token:
            tokens.add(query_token)

    LOGGER.debug('URL: %s - Tokens: %s', url, tokens)
    return tokens


def get_external_id_from_url(url, ext_id_pids_to_urls):
    LOGGER.debug('Trying to extract an identifier from <%s>', url)

    # Tidy up: remove trailing slash & use HTTPS
    tidy = url.rstrip('/')
    if not tidy.startswith('https'):
        tidy = tidy.replace('http', 'https', 1)

    # Start extraction
    for pid, formatters in ext_id_pids_to_urls.items():
        for formatter_url, (id_regex, url_regex,) in formatters.items():

            # Optimal case: match the original input URL against a full URL regex
            if url_regex is not None:
# TODO refactor this block, duplicate with the next
                match = (
                    re.match(url_regex, url)
                    if isinstance(url_regex, re.Pattern)
                    else regex.match(url_regex, url)
                )
                if match is not None:
                    groups = match.groups()
                    # This shouldn't happen, but who knows?
                    # For some reason, we have plenty of groups
                    # with `None` as the second element
                    if len(groups) > 1 and groups[1] is not None:
                        LOGGER.warning(
                            'Found multiple matching groups in <%s>: '
                            'Will use the first of %s',
                            url, groups,
                        )
                    ext_id = groups[0]
                    LOGGER.debug(
                        'Input URL matches the full URL regex. '
                        'URL: %s -> ID: %s - URL regex: %s',
                        url, ext_id, url_regex,
                    )
                    return (ext_id, pid,)

            # No URL regex: best matching effort using the tidy URL
            # Look for matching head & tail
            before, _, after = formatter_url.partition('$1')
            after = after.rstrip('/')
            if tidy.startswith(before) and tidy.endswith(after):
                LOGGER.debug(
                    'Clean URL matches external ID formatter URL: <%s> -> <%s>',
                    tidy, formatter_url,
                )
                url_fragment = (
                    tidy[len(before) : -len(after)]
                    if len(after)
                    else tidy[len(before) :]
                )

                # No ID regex: use the partitioned substring
                if id_regex is None:
                    LOGGER.debug(
                        'Missing ID regex, '
                        'will assume the URL substring as the ID. '
                        'URL: %s -> substring: %s',
                        tidy, url_fragment,
                    )
                    return url_fragment, pid

                # Use `re.match` instead of `re.search`
                # More precision, less recall:
                # valid IDs may be left in the URLs output
                match = (
                    re.match(id_regex, url_fragment)
                    if isinstance(id_regex, re.Pattern)
                    else regex.match(id_regex, url_fragment)
                )
                # Give up if the ID regex doesn't match
                if match is None:
                    LOGGER.debug(
                        "Skipping clean URL <%s> with substring '%s' "
                        "not matching the expected ID regex %s",
                        tidy, url_fragment, id_regex.pattern,
                    )
                    return (None, None,)

                ext_id = match.group()
                LOGGER.debug(
                    'Clean URL: %s -> ID: %s - substring: %s - ID regex: %s',
                    tidy, ext_id, url_fragment, id_regex,
                )
                return (ext_id, pid,)

    # Nothing worked: give up
    LOGGER.debug('Could not extract any identifier from <%s>', url)
    return (None, None,)


def is_wiki_link(url):
    domain = urlsplit(url).netloc
    return (
        True
        if any(wiki_project in domain for wiki_project in WIKI_PROJECTS)
        else False
    )
