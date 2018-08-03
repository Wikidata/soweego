#!/usr/bin/env python3
# coding: utf-8

import requests
import json
import re

from collections import defaultdict

WD = '/Users/focs/wikidata/'
SAMPLE = 'musicians_1_percent_sample'

ids_labels = defaultdict(dict)
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

dati = {'props': 'labels'}
dati['action'] = "wbgetentities"
dati['format'] = "json"
for b in buckets:
    dati['ids'] = '|'.join(b)
    r = requests.get("https://www.wikidata.org/w/api.php", params=dati).json()
    print('Call to API success: ', r.get('success'))
    for qid in r['entities']:
        entity = r['entities'][qid]
        #print(entity)
        if entity.get('labels'):
            for language in entity['labels'].keys():
                ids_labels[qid][language] = entity['labels'][language]['value']
        else:
            ids_labels[qid] = None

qid_labels = {}
for qid in ids_labels:
    if not ids_labels.get(qid):
        qid_labels[qid] = None
        continue
    v = defaultdict(list)
    for lang in ids_labels[qid]:
        v[ids_labels[qid][lang]].append(lang)
    qid_labels[qid] = v

# Wikidata sample, labels
label_qid = {}
for qid in qid_labels.keys():
    if qid_labels.get(qid):
        labels = qid_labels[qid].keys()
        for l in labels:
            label_qid[l.lower()] = qid
# Better sample with labels as keys
json.dump(label_qid, open(WD + SAMPLE + '_labels.json', 'w'), indent=2, ensure_ascii=False)

