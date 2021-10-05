#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Build a wiki table holding Web domains, frequency, and random examples
from a given URL dataset, as output by ``python -m soweego sync links``.
The input file name must start with ``CATALOG_ENTITY_urls``,
e.g., ``musicbrainz_band_urls_to_be_added.csv``.
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

import csv
import json
import os
import sys
from collections import OrderedDict, defaultdict
from random import sample
from urllib.parse import urlsplit

HEADER = """====TARGET====
{| class="sortable wikitable" style="font-size: 100%; text-align: center;"
! Domain
! Frequency
! Examples
|-
"""
FOOTER = '|}'
ROW = '| {domain} || {freq} || {examples}\n|-\n'
FREQ_THRESHOLD = 100
N_EXAMPLES = 3
CATALOG_URL_PREFIXES = {
    'discogs_band': 'https://www.discogs.com/artist/',
    'discogs_musician': 'https://www.discogs.com/artist/',
    'discogs_musical_work': 'https://www.discogs.com/master/',
    'musicbrainz_band': 'https://musicbrainz.org/artist/',
    'musicbrainz_musician': 'https://musicbrainz.org/artist/',
    'musicbrainz_musical_work': 'https://musicbrainz.org/release-group/',
}
WIKI_PROJECTS = (
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
)


def main(args):
    if len(args) != 2:
        print(
            f"Usage: python {__file__} URLS_CSV\n"
            "URLS_CSV file name must start with 'CATALOG_ENTITY_urls', "
            "e.g., 'discogs_band_urls'"
        )
        return 1

    file_in = args[1]
    catalog_and_entity = os.path.split(file_in)[1].partition('_urls')[0]
    file_out = f'{catalog_and_entity}_web_domains_table.mediawiki'
    json_out = f'{catalog_and_entity}.json'
    header = HEADER.replace('TARGET', catalog_and_entity.replace('_', ' ').title())
    prefix = CATALOG_URL_PREFIXES.get(catalog_and_entity)

    if prefix is None:
        raise ValueError(f'Invalid input file name: {file_in}')
        return 2

    freq = defaultdict(int)
    urls = defaultdict(list)
    wiki_urls = 0

    with open(file_in) as fin:
        r = csv.reader(fin)
        for (
            _,
            _,
            url,
            tid,
        ) in r:
            domain = urlsplit(url).netloc
            if any(wiki_project in domain for wiki_project in WIKI_PROJECTS):
                wiki_urls += 1
                continue
            freq[domain] += 1
            urls[domain].append(
                (
                    url,
                    tid,
                )
            )

    print(f'Total wiki URLs found: {wiki_urls}')

    rank = OrderedDict(sorted(freq.items(), key=lambda x: x[1], reverse=True))

    with open(json_out, 'w') as jout:
        json.dump(rank, jout)

    with open(file_out, 'w') as fout:
        fout.write(header)

        for domain, freq in rank.items():
            if freq < FREQ_THRESHOLD:
                continue

            examples = sample(urls[domain], N_EXAMPLES)
            buffer = []
            for i, (
                url,
                tid,
            ) in enumerate(examples, 1):
                buffer.append(f'{i}. [{url} URL], [{prefix}{tid} record]; ')

            fout.write(ROW.format(domain=domain, freq=freq, examples=''.join(buffer)))
        fout.write(FOOTER)

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
