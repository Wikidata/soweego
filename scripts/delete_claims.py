#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Delete claims made by the soweego bot.
The required input comes from a SPARQL query like:

SELECT DISTINCT ?stmt WHERE {
  ?item p:P3192 ?stmt .
  ?stmt ps:P3192 ?val ;
        prov:wasDerivedFrom ?ref .
  ?ref pr:P887 wd:Q1266546 ;
       pr:P248 wd:Q14005 .
  FILTER (CONTAINS(?val, " ")) .
}

Just replace the ``P3192`` (Last FM ID) PID with the relevant one,
and the QID in ``wd:Q14005`` (MusicBrainz) with the target catalog.
The ``FILTER`` clause is optional: you can use it to look for specific values,
see https://www.wikidata.org/wiki/Wikidata:SPARQL_tutorial#FILTER

N.B.: we look for (based on heuristic, record linkage), (stated in, catalog)
  references
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

import json
import sys
from time import sleep

import requests

WIKIDATA_API_URL = 'https://www.wikidata.org/w/api.php'
STMT_PREFIX = 'http://www.wikidata.org/entity/statement/'


def main(args):
    if len(args) != 4:
        print(f'Usage: python {__file__} GUIDS_CSV LOGIN_CREDENTIALS_JSON EDIT_SUMMARY')
        return 1

    file_in, creds_in, summary = args[1:]

    with open(creds_in) as fin:
        creds = json.load(fin)
    user = creds['WIKIDATA_API_USER']
    pw = creds['WIKIDATA_API_PASSWORD']
    guids = set()

    with open(file_in) as fin:
        for line in fin:
            line = line.rstrip()
            line = line.lstrip(STMT_PREFIX)
            # Statement URIs don't have the dollar
            guid = line.replace('-', '$', 1)
            guids.add(guid)

    session = requests.Session()

    # Get login token
    params = {'action': 'query', 'meta': 'tokens', 'type': 'login', 'format': 'json'}
    resp = session.get(WIKIDATA_API_URL, params=params)
    login_token = resp.json()['query']['tokens']['logintoken']

    # Log in
    data = {'action': 'login', 'lgname': user, 'lgpassword': pw, 'lgtoken': login_token, 'format': 'json'}
    resp = session.post(WIKIDATA_API_URL, data=data)

    # Get edit token
    params = {'action': 'query', 'meta': 'tokens', 'format': 'json'}
    resp = session.get(WIKIDATA_API_URL, params=params)
    edit_token = resp.json()['query']['tokens']['csrftoken']

    # Fire a POST for each GUID
    for guid in guids:
        data = {
            'action': 'wbremoveclaims',
            'token': edit_token,
            'bot': True,
            'claim': guid,
            'summary': summary,
            'format': 'json'
        }
        resp = session.post(WIKIDATA_API_URL, data=data)
        if resp.ok:
            print(resp.json())

        sleep(9)

    # Log out
    data = {'action': 'logout', 'token': edit_token, 'format': 'json'}
    resp = session.post(WIKIDATA_API_URL, data=data)

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
