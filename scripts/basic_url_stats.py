#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Simple statistics for rotten URL datasets, as output by
``python -m soweego importer check_urls``.
Dump two JSON files: Web domains ranked in descending order of frequency,
and URLs grouped by Web domains.
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

import csv
import json
import sys
from collections import defaultdict, OrderedDict
from urllib.parse import urlsplit


def main(args):
    if len(args) != 2:
        print(f'Usage: python {__file__} URLS_CSV')
        return 1

    file_in = args[1]
    rank_out = file_in.replace('.csv', '_domain_freq.json')
    urls_out = file_in.replace('.csv', '_by_domain.json')

    freq, urls = defaultdict(int), defaultdict(list)

    with open(args[1]) as fin:
        reader = csv.reader(fin)
        for url, _ in reader:
            domain = urlsplit(url).netloc
            freq[domain] += 1
            urls[domain].append(url)

    rank = OrderedDict(sorted(freq.items(), key=lambda x: x[1], reverse=True))

    with open(rank_out, 'w') as fout:
        json.dump(rank, fout)
    with open(urls_out, 'w') as fout:
        json.dump(urls, fout)

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))

