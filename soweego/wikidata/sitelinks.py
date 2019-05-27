#!/usr/bin/env python3
# coding: utf-8

import json
import re
from collections import defaultdict

import requests

WD = '/Users/focs/soweego/soweego/wikidata/resources/'
SAMPLE = 'imdb_unlinked_producers_sample'

site_qid = defaultdict(dict)
f = open(WD + SAMPLE)
lines = f.readlines()
all_ids = [re.search('Q\d+', l).group() for l in lines]
buckets = []
current = []
for qid in all_ids:
    current.append(qid)
    if len(current) > 49:
        buckets.append(current)
        current = []
buckets.append(current)
print(len(all_ids))
print(len(buckets))

dati = {'props': 'sitelinks', 'sitefilter': 'enwiki'}
dati['action'] = "wbgetentities"
dati['format'] = "json"
for b in buckets:
    dati['ids'] = '|'.join(b)
    r = requests.get("https://www.wikidata.org/w/api.php", params=dati).json()
    print('Call to API success: ', r.get('success'))
    for qid in r['entities']:
        entity = r['entities'][qid]
        if entity.get('sitelinks'):
            site_qid[entity['sitelinks']['enwiki']['title'].replace(' ', '_')] = qid

json.dump(site_qid, open(WD + SAMPLE + '_sitelinks.json', 'w'), indent=2, ensure_ascii=False)

