#!/usr/bin/env python3
# coding: utf-8

import gzip
import json
import os
import sys
import xml.etree.ElementTree as et
from collections import Counter, OrderedDict
from pkgutil import get_data
from urllib.parse import urlsplit

import click

from soweego.target_selection.common import matching_strategies

# Wikidata musicians samples
SAMPLES_LOCATION = 'soweego.wikidata.resources'
LABELS_SAMPLE = 'musicians_sample_labels.json'
LINKS_SAMPLE = 'musicians_sample_links.json'
SITELINKS_SAMPLE = 'musicians_sample_sitelinks.json'
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


def extract_data_from_dump(dump_file_path):
    """Extract 3 dictionaries ``{name|link|wikilink: identifier}`` from a Discogs dump file path.
    Dumps available at http://data.discogs.com/
    """
    names = {}
    links = {}
    wikilinks = {}
    with gzip.open(dump_file_path, 'rt') as dump:
        for event, element in et.iterparse(dump):
            if element.tag == 'artist':
                identifier = element.findtext('id')
                # Names
                name = element.findtext('name')
                if name:
                    names[name] = identifier
                else:
                    # TODO log a warning
                    print('No <name>')
                real_name = element.findtext('realname')
                if real_name:
                    names[real_name] = identifier
                # else:
                    # TODO log
                    # print('No <realname>')
                variations = element.find('namevariations')
                if variations:
                    for variation_element in variations.iterfind('name'):
                        variation = variation_element.text
                        if variation:
                            names[variation] = identifier
                        else:
                            print('empty variation <name>')
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
                                print(value_error, url)
                        else:
                            print('empty <url>')
    return names, links, wikilinks


def perfect_match(names, links, wikilinks, output_path):
    """Baseline matching strategy #1: treat everything as perfect string matches.
    Dump 3 JSON files with names, links, and wikilinks matches."""
    wikidata_names = json.loads(get_data(SAMPLES_LOCATION, LABELS_SAMPLE))
    name_matches = matching_strategies.equal_strings_match(
        (wikidata_names, names))
    json.dump(name_matches, open(os.path.join(
        output_path, 'musicians_labels_perfect_matches.json'), 'w'), indent=2, ensure_ascii=False)
    wikidata_links = json.loads(get_data(SAMPLES_LOCATION, LINKS_SAMPLE))
    link_matches = matching_strategies.equal_strings_match(
        (wikidata_links, links))
    json.dump(link_matches, open(os.path.join(
        output_path, 'musicians_links_perfect_matches.json'), 'w'), indent=2, ensure_ascii=False)
    wikidata_site_links = json.loads(
        get_data(SAMPLES_LOCATION, SITELINKS_SAMPLE))
    wikilink_matches = matching_strategies.equal_strings_match(
        (wikidata_site_links, wikilinks))
    json.dump(wikilink_matches, open(os.path.join(
        output_path, 'musicians_wikilinks_perfect_matches.json'), 'w'), indent=2, ensure_ascii=False)


def get_data_dictionaries(data_dir, dump_file_path):
    """Hit or set the cache of the dictionaries ``{name|link|wikilink: identifier}`` and return them."""
    names_path = os.path.join(data_dir, 'names_id.json')
    links_path = os.path.join(data_dir, 'links_id.json')
    wiki_links_path = os.path.join(data_dir, 'wiki_links_id.json')
    if os.path.isdir(data_dir) and os.path.isfile(names_path) and os.path.isfile(links_path) and os.path.isfile(wiki_links_path):
        return json.load(open(names_path)), json.load(open(links_path)), json.load(open(wiki_links_path))
    os.makedirs(data_dir)
    names, links, wikilinks = extract_data_from_dump(dump_file_path)
    json.dump(names, open(names_path, 'w'), indent=2, ensure_ascii=False)
    json.dump(links, open(links_path, 'w'), indent=2, ensure_ascii=False)
    json.dump(wikilinks, open(wiki_links_path, 'w'),
              indent=2, ensure_ascii=False)
    return names, links, wikilinks


def dump_url_domains(links, output_dir):
    """Dump a ``{URL_domain: count}`` JSON from the given list of links, in descending order of occurrences."""
    domains = []
    for link in links.keys():
        domain = urlsplit(link).netloc
        if domain:
            domains.append(domain)
        else:
            print('No domain in', link)
    domains_by_frequency = OrderedDict(
        sorted(Counter(domains).items(), key=lambda x: x[1], reverse=True))
    json.dump(domains_by_frequency, open(os.path.join(
        output_dir, 'url_domains_count.json'), 'w'), indent=2, ensure_ascii=False)


@click.command()
@click.argument('dump_file', type=click.Path(exists=True, dir_okay=False))
@click.option('-o', '--outdir', type=click.Path(file_okay=False), default='output')
def main(dump_file, outdir):
    """Run perfect string matching over names, links and wikilinks."""
    names, links, wiki_links = get_data_dictionaries(outdir, dump_file)
    dump_url_domains(links, outdir)
    perfect_match(names, links, wiki_links, outdir)
    sys.exit(0)
