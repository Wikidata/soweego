#!/usr/bin/env python3
# coding: utf-8

import gzip
import json
import logging
import os
import sys
import xml.etree.ElementTree as et
from collections import Counter, OrderedDict
from pkgutil import get_data
from urllib.parse import urlsplit

import click

from soweego.commons.candidate_acquisition import PROD_DB, TEST_DB
from soweego.target_selection.common import matching_strategies

LOGGER = logging.getLogger(__name__)
# Wikidata musicians samples
SAMPLES_LOCATION = 'soweego.wikidata.resources'
QID_NAMES_LANGUAGES_SAMPLE = 'musicians_sample_qid_labels_languages.json'
NAMES_SAMPLE = 'musicians_sample_labels.json'
LINKS_SAMPLE = 'musicians_sample_links.json'
SITELINKS_SAMPLE = 'musicians_sample_sitelinks.json'
TOOLFORGE_DB_TABLE = 'discogs'
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
    'meta.wikimedia'
]
MATCHING_STRATEGIES = [
    'perfect', 'links', 'names', 'jw', 'l', 'dl', 'all'
]


def extract_data_from_dump(dump_file_path):
    """Extract the set of identifiers and 3 dictionaries ``{name|link|wikilink: identifier}``
    from a Discogs dump file path.

    Dumps available at https://data.discogs.com/
    """
    ids = set()
    names = {}
    links = {}
    wikilinks = {}
    with gzip.open(dump_file_path, 'rt') as dump:
        for event, element in et.iterparse(dump):
            if element.tag == 'artist':
                identifier = element.findtext('id')
                ids.add(identifier)
                # Names
                name = element.findtext('name')
                if name:
                    names[name] = identifier
                else:
                    LOGGER.warning(
                        'Skipping extraction for identifier with no name: %s', identifier)
                    continue
                real_name = element.findtext('realname')
                if real_name:
                    names[real_name] = identifier
                else:
                    LOGGER.debug('Artist %s has an empty <realname> tag', identifier)
                variations = element.find('namevariations')
                if variations:
                    for variation_element in variations.iterfind('name'):
                        variation = variation_element.text
                        if variation:
                            names[variation] = identifier
                        else:
                            LOGGER.debug(
                                'Artist %s has an empty <namevariations> tag', identifier)
                # Links & Wiki links
                urls = element.find('urls')
                if urls:
                    for url_element in urls.iterfind('url'):
                        url = url_element.text
                        if url:
                            try:
                                domain = urlsplit(url).netloc
                                if any(wiki_project in domain for wiki_project in WIKI_PROJECTS):
                                    wikilinks[url] = identifier
                                else:
                                    links[url] = identifier
                            except ValueError as value_error:
                                LOGGER.warning(
                                    "Skipping %s: '%s'", value_error, url)
                                continue
                        else:
                            LOGGER.debug(
                                'Artist %s: skipping empty <url> tag', identifier)
                            continue
    return ids, names, links, wikilinks


def perfect_match(names, links, wikilinks, output_path):
    """Baseline matching strategy #1: treat everything as perfect string matches.
    Dump 3 JSON files with names, links, and wikilinks matches."""
    wikidata_names = json.loads(get_data(SAMPLES_LOCATION, NAMES_SAMPLE))
    name_matches = matching_strategies.perfect_string_match(
        (wikidata_names, names))
    json.dump(name_matches, open(os.path.join(
        output_path, 'musicians_labels_perfect_matches.json'), 'w'), indent=2, ensure_ascii=False)
    wikidata_links = json.loads(get_data(SAMPLES_LOCATION, LINKS_SAMPLE))
    link_matches = matching_strategies.perfect_string_match(
        (wikidata_links, links))
    json.dump(link_matches, open(os.path.join(
        output_path, 'musicians_links_perfect_matches.json'), 'w'), indent=2, ensure_ascii=False)
    wikidata_site_links = json.loads(
        get_data(SAMPLES_LOCATION, SITELINKS_SAMPLE))
    wikilink_matches = matching_strategies.perfect_string_match(
        (wikidata_site_links, wikilinks))
    json.dump(wikilink_matches, open(os.path.join(
        output_path, 'musicians_wikilinks_perfect_matches.json'), 'w'), indent=2, ensure_ascii=False)


def link_match(links, output_path):
    """Baseline matching strategy #2: match similar links.
    Dump a JSON file with link matches.
    """
    wikidata_links = json.loads(get_data(SAMPLES_LOCATION, LINKS_SAMPLE))
    matches = matching_strategies.similar_link_match(wikidata_links, links)
    json.dump(matches, open(os.path.join(
        output_path, 'musicians_links_similar_matches.json'), 'w'), indent=2, ensure_ascii=False)


def name_match(names, output_path):
    """Baseline matching strategy #3: match similar names.
    Dump a JSON file with names matches
    """
    wikidata_names = json.loads(get_data(SAMPLES_LOCATION, NAMES_SAMPLE))
    matches = matching_strategies.similar_name_match(wikidata_names, names)
    json.dump(matches, open(os.path.join(
        output_path, 'musicians_names_similar_matches.json'), 'w'), indent=2, ensure_ascii=False)


def edit_distance_name_match(target_table, target_database, target_search_type, metric, threshold, output_path):
    """Baseline matching strategy #4: match names based on Jaro-Winkler distance.
    Dump a JSON file with name matches.
    """
    # FIXME this breaks with Python 3.4
    # wikidata_names = json.loads(get_data(SAMPLES_LOCATION, QID_NAMES_LANGUAGES_SAMPLE))
    wikidata_names = json.load(open(
        '/data/project/soweego/soweego/soweego/wikidata/resources/musicians_sample_qid_labels_languages.json'))
    matches = matching_strategies.edit_distance_match(
        wikidata_names, target_table, target_database, target_search_type, metric, threshold)
    json.dump(matches, open(os.path.join(
        output_path, 'musicians_names_%s_matches.json' % metric), 'w'), indent=2, ensure_ascii=False)


def get_data_dictionaries(data_dir, dump_file_path):
    """Hit or set the cache of the dictionaries ``{name|link|wikilink: identifier}`` and return them."""
    ids_path = os.path.join(data_dir, 'all_ids')
    names_path = os.path.join(data_dir, 'names_id.json')
    links_path = os.path.join(data_dir, 'links_id.json')
    wiki_links_path = os.path.join(data_dir, 'wiki_links_id.json')
    if os.path.isdir(data_dir) and os.path.isfile(ids_path) and os.path.isfile(names_path) and os.path.isfile(links_path) and os.path.isfile(wiki_links_path):
        return set(identifier for identifier in open(ids_path)), json.load(open(names_path)), json.load(open(links_path)), json.load(open(wiki_links_path))
    os.makedirs(data_dir)
    ids, names, links, wikilinks = extract_data_from_dump(dump_file_path)
    with open(ids_path, 'w') as ids_file:
        ids_file.writelines([identifier + '\n' for identifier in ids])
    json.dump(names, open(names_path, 'w'), indent=2, ensure_ascii=False)
    json.dump(links, open(links_path, 'w'), indent=2, ensure_ascii=False)
    json.dump(wikilinks, open(wiki_links_path, 'w'),
              indent=2, ensure_ascii=False)
    return ids, names, links, wikilinks


def dump_url_domains(links, output_dir):
    """Dump a ``{URL_domain: count}`` JSON from the given list of links, in descending order of occurrences."""
    domains = []
    for link in links.keys():
        domain = urlsplit(link).netloc
        if domain:
            domains.append(domain)
        else:
            LOGGER.debug('No domain in %s', link)
    domains_by_frequency = OrderedDict(
        sorted(Counter(domains).items(), key=lambda x: x[1], reverse=True))
    json.dump(domains_by_frequency, open(os.path.join(
        output_dir, 'url_domains_count.json'), 'w'), indent=2, ensure_ascii=False)


@click.command()
@click.argument('dump_file', type=click.Path(exists=True, dir_okay=False))
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='output',
              help="default: 'output'")
@click.option('-s', '--strategy', type=click.Choice(MATCHING_STRATEGIES), default='all',
              help="'perfect': perfect string, " +
              "'links': similar links, 'names': similar names, " +
              "'jw': Jaro-Winkler on names, 'l': Levenshtein on names, " +
              "'dl': Damerau-Levenshtein on names, or 'all' (default).")
@click.option('-j', '--jaro-winkler-threshold', type=float, default=0.8,
              help='default: 0.8')
@click.option('-l', '--levenshtein-threshold', type=int, default=2,
              help='default: 2')
@click.option('-d', '--damerau-threshold', type=int, default=2,
              help='default: 2')
@click.option('--dump-domains', is_flag=True,
              help='Write a JSON with frequency counts of URL domains.')
@click.option('--target-database', type=click.Choice([TEST_DB, PROD_DB]), default=TEST_DB)
@click.option('--search-type', type=click.Choice(
    ['natural_language', 'boolean', 'expansion']), default='natural_language')
def main(dump_file, output_dir, strategy, jaro_winkler_threshold,
         levenshtein_threshold, damerau_threshold, dump_domains,
         target_database, search_type):
    """Run baseline matching strategies over names, links and wikilinks
    of a given Discogs database dump.

    Downloads available at https://data.discogs.com/
    """
    ids, names, links, wiki_links = get_data_dictionaries(
        output_dir, dump_file)
    if dump_domains:
        dump_url_domains(links, output_dir)
    if strategy == 'perfect':
        perfect_match(names, links, wiki_links, output_dir)
    elif strategy == 'links':
        link_match(links, output_dir)
    elif strategy == 'names':
        name_match(names, output_dir)
    elif strategy == 'jw':
        edit_distance_name_match(
            TOOLFORGE_DB_TABLE, target_database, search_type, strategy, jaro_winkler_threshold, output_dir)
    elif strategy == 'l':
        edit_distance_name_match(
            TOOLFORGE_DB_TABLE, target_database, search_type, strategy, levenshtein_threshold, output_dir)
    elif strategy == 'dl':
        edit_distance_name_match(
            TOOLFORGE_DB_TABLE, target_database, search_type, strategy, damerau_threshold, output_dir)
    elif strategy == 'all':
        perfect_match(names, links, wiki_links, output_dir)
        link_match(links, output_dir)
        name_match(names, output_dir)
        edit_distance_name_match(TOOLFORGE_DB_TABLE, target_database,
                                 search_type, 'jw', jaro_winkler_threshold, output_dir)
        edit_distance_name_match(TOOLFORGE_DB_TABLE, target_database,
                                 search_type, 'l', levenshtein_threshold, output_dir)
        edit_distance_name_match(TOOLFORGE_DB_TABLE, target_database,
                                 search_type, 'dl', damerau_threshold, output_dir)
