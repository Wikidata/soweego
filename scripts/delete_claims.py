#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Delete claims made by the soweego bot.
The required input comes from a SPARQL query like:

SELECT DISTINCT ?stmt WHERE {
  ?item p:P6262 ?stmt .
  ?stmt prov:wasDerivedFrom ?ref .
  ?ref pr:P887 wd:Q1266546 ;
       pr:P248 wd:Q14005 .
}

Just replace the PID in `p:P6262` (Fandom article ID) with the relevant one,
and the QID in `wd:Q14005` (MusicBrainz) with the target catalog.

N.B.: we look for (based on heuristic, record linkage), (stated in, catalog)
  references
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

import sys

import requests

WIKIDATA_API_URL = 'https://www.wikidata.org/w/api.php'
STMT_PREFIX = 'http://www.wikidata.org/entity/statement/'


def main(args):
    if len(args) != 3:
        print(f'Usage: python {__file__} GUIDS_CSV EDIT_SUMMARY')
        return 1

    file_in, summary = args[1], args[2]
    guids = set()

    with open(file_in) as fin:
        for line in fin:
            line = line.rstrip()
            line = line.lstrip(STMT_PREFIX)
            # Statement URIs don't have the dollar
            guid = line.replace('-', '$', 1)
            guids.add(guid)

    session = requests.Session()

    # Get edit token
    params = {'action': 'query', 'meta': 'tokens', 'format': 'json'}
    r = session.get(WIKIDATA_API_URL, params=params)
    token = r.json()['query']['tokens']['csrftoken']

    # Fire a POST for each GUID
    for guid in guids:
        data = {
            'action': 'wbremoveclaims',
            'format': 'json',
            'token': token,
            'bot': True,
            'claim': guid,
            'summary': summary,
        }
        r = session.post(WIKIDATA_API_URL, data=data)

        if r.ok:
            print(r.json())

        return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
